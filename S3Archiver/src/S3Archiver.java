import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class S3Archiver {
	public static List<String> processBuilderList;
	public static final int maxThreadCount = 15000;

	private static void initializeProcessBuilder() {
		processBuilderList = new ArrayList<String>();
		processBuilderList.add("/Users/slakshminarayana/Documents/codebase/amazon-s3-tar-tool/bin/s3tar-darwin-amd64");
		processBuilderList.add("--region");
		processBuilderList.add("us-east-1");
		processBuilderList.add("--storage-class");
		processBuilderList.add("DEEP_ARCHIVE");
		processBuilderList.add("-cvf");
	}

	private static void processDirectory(Path directoryPath) throws IOException {
		// Process only files under this directory because we have a dedicated thread to
		// process directories
		for (File file : directoryPath.toFile().listFiles()) {
			if (!file.isDirectory() && !file.getName().equals(".DS_Store")) {
				processFile(file);
			}
		}
	}

	private static void processFile(File file) {
		if (file.getName().endsWith(".csv")) {
			// Change to the correct source and destination
			// Create a guid for archiveName and ensure they are unique
			processBuilderList
					.add("s3://telegraph-test-aws-glue/tp=billing_eu_subscriptions/archivedData/archive_Dec.tar");
			processBuilderList.add("s3://telegraph-test-aws-glue/tp=billing_eu_subscriptions/");

			ProcessBuilder pb = new ProcessBuilder(processBuilderList);
			try {
				if (executeProcess(pb)) {
					changeStatusToComplete(file);
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static void changeStatusToComplete(File file) {
		StringBuilder sb = new StringBuilder(file.toString()).append("_processed");
		file.renameTo(new File(sb.toString()));
	}

	private static boolean executeProcess(ProcessBuilder pb) throws IOException, InterruptedException {
		Process process = pb.start();
		int exitCode = process.waitFor();
		// Do retry logic
		if (exitCode != 0) {
			InputStream errorStream = process.getErrorStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
			String line;
			System.out.print("Error message: ");
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}
		}

		return (exitCode == 0);
	}

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();

		Path inventoryReportPath = Paths.get(args[0]);
		ExecutorService executorService = Executors
				.newFixedThreadPool(Math.min(maxThreadCount, Integer.parseInt(args[1])));
		initializeProcessBuilder();

		// Parallelize only at directory level and not file level to ensure that we
		// don't hit S3 prefix throttling
		List<Future<Void>> futures = new ArrayList<>();
		Files.walk(inventoryReportPath).filter(Files::isDirectory)
				.forEach(path -> futures.add((Future<Void>) executorService.submit(() -> {
					try {
						// Ignore the root directory, else reports will be processed twice
						if (path.compareTo(inventoryReportPath) != 0) {
							processDirectory(path);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				})));

		// Process files at root level, if any; sequentially to ensure that we don't hit
		// S3 prefix throttling
		for (File file : inventoryReportPath.toFile().listFiles()) {
			if (!file.isDirectory() && !file.getName().equals(".DS_Store")) {
				processFile(file);
			}
		}

		for (Future<Void> future : futures) {
			try {
				future.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		executorService.shutdown();

		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;

		System.out.println("Archiving is complete, execution time: " + (executionTime / 60000) + " minutes");
	}
}
