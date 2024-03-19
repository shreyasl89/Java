import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class S3Cleanup {
	private static ExecutorService executorService;
	private static final int MAX_THREAD_COUNT = 15000;

	private static List<String> initializeProcessBuilder() {
		List<String> processBuilderList = new ArrayList<String>();

		processBuilderList.add("/usr/bin/aws");
		processBuilderList.add("s3");
		processBuilderList.add("rm");

		return processBuilderList;
	}

	private static void processDirectory(Path directoryPath) {
		// Iterate over the files and directories
		for (File file : directoryPath.toFile().listFiles()) {
			if (!file.isDirectory() && file.getName().endsWith(".csv_processed")) {
				processFile(file);
			}
		}
	}

	private static void processFile(File file) {
		if (file.getName().endsWith(".csv_processed")) {
			try {
				System.out.println("Cleaning up files from: " + file.getPath());
				BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
				String lineEntry;

				while ((lineEntry = bufferedReader.readLine()) != null) {
					lineEntry = lineEntry.replace("\"", "");
					String[] columns = lineEntry.split(",");
					columns[1] = columns[1].replace("%3D", "=");

					List<String> processBuilderList = initializeProcessBuilder();
					String fileName = "s3://" + columns[0] + "/" + columns[1];
					System.out.println("Trying to delete: " + fileName);

					processBuilderList.add(fileName);
					ProcessBuilder pb = new ProcessBuilder(processBuilderList);
					executeProcess(pb);
				}

				bufferedReader.close();

				System.out.println("Deleting manifest file: " + file.getPath());
				if (!file.delete()) {
					System.out.println("Failed to delete manifest file: " + file.getPath());
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		}
	}

	private static void executeProcess(ProcessBuilder pb) throws Exception {
		Process process = pb.start();
		int exitCode = process.waitFor();
		if (exitCode != 0) {
			InputStream errorStream = process.getErrorStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
			String line;
			StringBuilder sb = new StringBuilder();
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}

			throw new Exception("Error: " + sb.toString());
		}
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();

		if (args.length < 2) {
			System.out.println("Please specify inventoryReportPath and maximum size of thread pool");
			return;
		}

		Path inventoryReportPath = Paths.get(args[0]);
		executorService = Executors.newFixedThreadPool(Math.min(MAX_THREAD_COUNT, Integer.parseInt(args[2])));

		List<Future<Void>> futures = new ArrayList<>();
		Files.walk(inventoryReportPath).filter(Files::isDirectory)
				.forEach(path -> futures.add((Future<Void>) executorService.submit(() -> {
					try {
						processDirectory(path);
					} catch (Exception e) {
						e.printStackTrace();
					}
				})));

		for (File file : inventoryReportPath.toFile().listFiles()) {
			if (!file.isDirectory() && file.getName().equals(".csv_processed")) {
				executorService.submit(() -> {
					try {
						processFile(file);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
				processFile(file);
			}
		}

		for (Future<Void> future : futures) {
			try {
				future.get();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		executorService.shutdown();

		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;

		System.out.println("Cleanup is complete, execution time: " + (executionTime / 60000) + " minutes");
	}
}
