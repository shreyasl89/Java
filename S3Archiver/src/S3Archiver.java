import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class S3Archiver {
	private static final int MAX_THREAD_COUNT = 15000;
	private static final int MAX_RETRIES = 3;
	private static final long INITIAL_INTERVAL = 30000;
	private static final double MULTIPLIER = 2;
	private static String tarToolPath = "";

	private static List<String> initializeArchivalProcessBuilder() {
		List<String> processBuilderList = new ArrayList<String>();

		processBuilderList.add(tarToolPath);
		processBuilderList.add("--region");
		processBuilderList.add("us-east-1");
		processBuilderList.add("--storage-class");
		processBuilderList.add("DEEP_ARCHIVE");
		processBuilderList.add("--concat-in-memory");
		processBuilderList.add("--urldecode");
		processBuilderList.add("-cvf");

		return processBuilderList;
	}

	private static List<String> initializeValidateProcessBuilder() {
		List<String> processBuilderList = new ArrayList<String>();

		processBuilderList.add("/usr/bin/aws");
		processBuilderList.add("s3");
		processBuilderList.add("ls");

		return processBuilderList;
	}

	private static void processDirectory(Path directoryPath) throws Exception {
		// Process only files under this directory because we have a dedicated thread to
		// process sub-directories
		for (File file : directoryPath.toFile().listFiles()) {
			if (!file.isDirectory() && !file.getName().equals(".DS_Store")) {
				processFile(file);
			}
		}
	}

	private static void processFile(File file) throws Exception {
		if (file.getName().endsWith(".csv")) {
			System.out.println("Trying to archive file: " + file.getPath());
			FileReader fileReader = new FileReader(file.getParent() + "/archiveDestinationPath.txt");
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String tarDestnPath = bufferedReader.readLine() + UUID.randomUUID().toString() + ".tar";

			bufferedReader.close();
			fileReader.close();

			List<String> processBuilderList = initializeArchivalProcessBuilder();
			processBuilderList.add(tarDestnPath);
			processBuilderList.add("-m");
			processBuilderList.add(file.toString());

			ProcessBuilder pb = new ProcessBuilder(processBuilderList);
			int retryCount = 0;
			long interval = INITIAL_INTERVAL;
			while (retryCount < MAX_RETRIES) {
				try {
					executeProcess(pb);
					validateCompletion(tarDestnPath, file);
					System.out.println("Successfully archived " + file.getPath());
					break;
				} catch (Exception e) {
					if (e.getMessage().contains("ExpiredToken")) {
						System.exit(1);
					}
					if (e.getMessage().contains("less than 5MB")) {
						System.out.println("Skipping " + file.getPath() + " as it failed to archive due to size limit");
						break;
					}
					System.out.println(e.getMessage() + ". Retrying to archive " + file.toString() + " in " + interval
							+ " milliseconds...");
					TimeUnit.MILLISECONDS.sleep(interval);
					interval *= MULTIPLIER;
					retryCount++;
				}
			}
		}
	}

	private static void validateCompletion(String finalTarFile, File file) {
		List<String> processBuilderList = initializeValidateProcessBuilder();
		processBuilderList.add(finalTarFile);

		ProcessBuilder pb = new ProcessBuilder(processBuilderList);
		try {
			executeProcess(pb);

			StringBuilder sb = new StringBuilder(file.toString()).append("_processed");
			file.renameTo(new File(sb.toString()));
		} catch (Exception e) {
			System.out.println(e.getMessage());
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

			String errorMsg = (sb.length() != 0) ? sb.toString() : "File not found";
			throw new Exception("Error: " + errorMsg);
		}
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		if (args.length < 3) {
			System.out.println("Please specify tarToolPath, inventoryReportPath and maximum size of thread pool");
			return;
		}

		tarToolPath = args[0];
		Path inventoryReportPath = Paths.get(args[1]);
		ExecutorService executorService = Executors
				.newFixedThreadPool(Math.min(MAX_THREAD_COUNT, Integer.parseInt(args[2])));

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
					} catch (Exception e) {
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
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		executorService.shutdown();

		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;

		System.out.println("Archiving is complete, execution time: " + (executionTime / 60000) + " minutes");
	}
}
