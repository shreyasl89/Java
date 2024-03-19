import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

public class CSVParser {
	private static class processLineResult {
		String previousFilePath;
		int entryCount;

		public processLineResult() {
			previousFilePath = "";
			entryCount = 0;
		}
	}

	private static String sourcePath = "";
	private static int maxEntriesPerFile = 500000;
	private static ExecutorService executorService;

	private static void processDirectory(Path directoryPath) throws Exception {
		File directory = directoryPath.toFile();
		if (directory.isDirectory() && directory.getName().equals("processedReports"))
			return;

		// Get the list of files and sub-directories in the directory
		File[] files = directory.listFiles();

		// Iterate over the files and directories
		for (File file : files) {
			if (file.isDirectory()) {
				CSVParser.processDirectory(file.toPath());
			} else {
				executorService.submit(() -> {
					try {
						processFile(file);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		}
	}

	private static void processFile(File file) throws Exception {
		if (!file.getName().equals(".DS_Store")) {
			System.out.println("Processing file: " + file.getName());
			BufferedReader bufferedReader = null;
			GZIPInputStream gzipInputStream = null;
			if (file.getName().endsWith(".csv.gz")) {
				gzipInputStream = new GZIPInputStream(new FileInputStream(file));
				bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream));
			} else if (file.getName().endsWith(".csv")) {
				bufferedReader = new BufferedReader(new FileReader(file));
			} else {
				System.out.println("Unsupported file format: " + file.getName());
				return;
			}

			try {
				String lineEntry;
				processLineResult previousProcessLineResult = new processLineResult();
				while ((lineEntry = bufferedReader.readLine()) != null) {
					previousProcessLineResult = processLine(lineEntry, previousProcessLineResult);
					previousProcessLineResult.entryCount++;
				}
			} catch (Exception e) {
				System.out.println(e.getMessage() + " for " + file.getName() + " continuing with next file...");
			}

			bufferedReader.close();
			if (gzipInputStream != null)
				gzipInputStream.close();
		}
	}

	// To Do: Use a simple iterator for string manipulation as opposed to using
	// built-in methods to achieve linear time complexity
	private static processLineResult processLine(String lineEntry, processLineResult previousProcessLineResult)
			throws Exception {
		String[] columns = lineEntry.split(",");
		columns[1] = columns[1].replace("%3D", "=").replace("\"", "");
		String[] columnParts = columns[1].split("/");

		// destnPath is in format processedReports/<year>/<eventtype>/<month>/<date> to
		// aid archiving based on year
		if (columnParts.length < 3)
			throw new Exception("File hierarchy is incorrect");
		StringBuilder destnPath = new StringBuilder(sourcePath);
		destnPath.append("/processedReports/").append(columnParts[1]).append("/").append(columnParts[0]).append("/")
				.append(columnParts[2]).append("/").append(columnParts[3]);

		/*
		 * Create new csv if: 1. A csv has not been created for the given date 2.
		 * current line entry doesn't match the date as indicated on previous line entry
		 * 3. max number of entries in previous csv has reached it's max limit
		 */
		if (previousProcessLineResult.previousFilePath.equals("")
				|| !destnPath.toString()
						.equals(previousProcessLineResult.previousFilePath.substring(0,
								previousProcessLineResult.previousFilePath.lastIndexOf('/')))
				|| previousProcessLineResult.entryCount >= maxEntriesPerFile) {
			Files.createDirectories(Paths.get(destnPath.toString()));

			previousProcessLineResult.previousFilePath = destnPath.toString() + "/" + UUID.randomUUID().toString()
					+ ".csv";
			previousProcessLineResult.entryCount = 0;

			File tarDestn = new File(destnPath.toString() + "/archiveDestinationPath.txt");
			if (!tarDestn.exists()) {
				FileWriter fw = new FileWriter(tarDestn);
				PrintWriter pw = new PrintWriter(fw);

				columns[0] = columns[0].replace("\"", "");
				StringBuilder stringBuilder = new StringBuilder("s3://").append(columns[0]).append("/")
						.append(columns[1].substring(0, columns[1].indexOf("/hr"))).append("/archivedData/");
				pw.println(stringBuilder.toString());

				pw.close();
				fw.close();
			}
		}

		FileWriter fw = new FileWriter(previousProcessLineResult.previousFilePath, true);
		PrintWriter pw = new PrintWriter(fw);

		pw.println(lineEntry);

		pw.close();
		fw.close();

		return previousProcessLineResult;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		if (args.length < 2) {
			System.out.println("Please specify inventoryReportPath and maximum size of thread pool");
			return;
		}

		sourcePath = args[0];
		executorService = Executors.newFixedThreadPool(Integer.valueOf(args[1]));

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(sourcePath))) {
			for (Path path : stream) {
				if (Files.isDirectory(path)) {
					CSVParser.processDirectory(path);
				} else {
					executorService.submit(() -> {
						try {
							processFile(path.toFile());
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
				}
			}
		}

		executorService.shutdown();
		while (!executorService.isTerminated())
			;

		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;

		System.out.println("Parsing is complete, execution time: " + (executionTime / 60000.0) + " minutes");
	}
}