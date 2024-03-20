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
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

public class S3Cleanup {
	private static ExecutorService executorService;
	private static final int MAX_THREAD_COUNT = 1000;
	private static final int MAX_RETRIES = 5;
	private static final long INITIAL_INTERVAL = 30000;
	private static final double MULTIPLIER = 2;

	private static List<String> initializeProcessBuilder() {
		List<String> processBuilderList = new ArrayList<String>();

		processBuilderList.add("/usr/bin/aws");
		processBuilderList.add("s3api");
		processBuilderList.add("delete-objects");
		processBuilderList.add("--bucket");

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
			int retryCount = 0;
			long interval = INITIAL_INTERVAL;
			while (retryCount < MAX_RETRIES) {
				try {
					BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
					String lineEntry;
					JSONObject finalJson = new JSONObject();
					JSONArray filesToBeDeleted = new JSONArray();
					List<String> processBuilderList = initializeProcessBuilder();
					int count = 0, iterationCount = 0;
					boolean newFile = true;

					while ((lineEntry = bufferedReader.readLine()) != null) {
						lineEntry = lineEntry.replace("\"", "");
						String[] columns = lineEntry.split(",");
						columns[1] = columns[1].replace("%3D", "=");

						if (newFile) {
							processBuilderList.add(columns[0]);
							processBuilderList.add("--delete");
							newFile = false;
						}

						JSONObject jsonObject = new JSONObject();
						jsonObject.put("Key", columns[1]);
						filesToBeDeleted.put(jsonObject);
						count++;

						if (count >= 250) {
							iterationCount++;
							System.out.println("Cleaning up files from: " + file.getPath() + ", iteration count: "
									+ iterationCount);
							finalJson.put("Objects", filesToBeDeleted);
							processBuilderList.add(finalJson.toString());

							ProcessBuilder pb = new ProcessBuilder(processBuilderList);
							executeProcess(pb);
							System.out.println(
									"Iteration count: " + iterationCount + " complete for file: " + file.getPath());
							processBuilderList.remove(processBuilderList.size() - 1);
							finalJson.clear();
							filesToBeDeleted.clear();
							count = 0;
						}
					}
					bufferedReader.close();

					if (!filesToBeDeleted.isEmpty()) {
						iterationCount++;
						System.out.println("Cleaning up files from: " + file.getPath() + ", final iteration count: "
								+ iterationCount);
						finalJson.put("Objects", filesToBeDeleted);
						processBuilderList.add(finalJson.toString());

						ProcessBuilder pb = new ProcessBuilder(processBuilderList);
						executeProcess(pb);
						System.out.println(
								"Iteration count: " + iterationCount + " complete for file: " + file.getPath());
					}

					System.out.println("Deleting manifest file: " + file.getPath());
					if (!file.delete()) {
						System.out.println("Failed to delete manifest file: " + file.getPath());
					}
					break;
				} catch (Exception e) {
					System.out.println(e.getMessage() + ". Retrying to archive " + file.toString() + " in " + interval
							+ " milliseconds...");
					try {
						TimeUnit.MILLISECONDS.sleep(interval);
					} catch (InterruptedException e1) {
						System.out.println(e.getMessage());
					}
					interval *= MULTIPLIER;
					retryCount++;
				}
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
		executorService = Executors.newFixedThreadPool(Math.min(MAX_THREAD_COUNT, Integer.parseInt(args[1])));

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

		System.out.println("Cleanup is complete, execution time: " + (executionTime / 60000.0) + " minutes");
	}
}
