import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.io.File;
import java.io.IOException;

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

	private static void processDirectory(Path directoryPath) throws IOException {
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
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			}
		}
	}

	private static void processFile(File file) throws IOException {
		if (!file.getName().equals(".DS_Store")) {
			System.out.println("Processing file: " + file.getName());
			if (file.getName().endsWith(".csv.gz")) {
				processCompressedFile(file);
			} else if (file.getName().endsWith(".csv")) {
				processUncompressedFile(file);
			} else {
				System.out.println("Unsupported file format");
			}
		}
	}

	private static void processCompressedFile(File file) throws IOException {
		GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream));

		String lineEntry;
		processLineResult previousProcessLineResult = new processLineResult();
		while ((lineEntry = bufferedReader.readLine()) != null) {

			previousProcessLineResult = processLine(lineEntry, previousProcessLineResult);
			previousProcessLineResult.entryCount++;
		}

		bufferedReader.close();
		gzipInputStream.close();
	}

	private static void processUncompressedFile(File file) throws IOException {
		Scanner fileReader = null;
		try {
			fileReader = new Scanner(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		processLineResult previousProcessLineResult = new processLineResult();
		while (fileReader.hasNext()) {
			previousProcessLineResult = processLine(fileReader.next(), previousProcessLineResult);
			previousProcessLineResult.entryCount++;
		}
		fileReader.close();
	}

	// To Do: Use a simple iterator for string manipulation as opposed to using
	// built-in methods to achieve linear time complexity
	private static processLineResult processLine(String lineEntry, processLineResult previousProcessLineResult)
			throws IOException {
		String[] columns = lineEntry.split(",");
		columns[1] = columns[1].replace("%3D", "=").replace("\"", "");
		String[] columnParts = columns[1].split("/");

		// destnPath is in format processedReports/<year>/<eventtype>/<month>/<date>
		StringBuilder destnPath = new StringBuilder(sourcePath);
		destnPath.append("/processedReports/").append(columnParts[1]).append("/").append(columnParts[0]).append("/")
				.append(columnParts[2]).append("/").append(columnParts[3]);

		/*
		 * Create new csv if: 1. No csv has been created for the given date 2. current
		 * line entry doesn't match the date as indicated on previous line entry 3. max number of entries in
		 * previous csv has reached it's max limit
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
				pw.println(columns[0]);
				pw.println(columns[1].substring(0, columns[1].indexOf("/hr")) + "/archivedData");

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

	public static void main(String[] args) throws IOException {
		/*
		 * Scanner sc = new Scanner(System.in);
		 * 
		 * System.out.print("Enter the path of the inventory reports: "); sourcePath =
		 * sc.nextLine(); File directory = new File(sourcePath);
		 * 
		 * System.out.print("Enter the maximum number of concurrent threads: ");
		 * ExecutorService executorService = Executors.newFixedThreadPool(sc.nextInt());
		 * 
		 * sc.close();
		 */

		long startTime = System.currentTimeMillis();

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
						} catch (IOException e) {
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

		System.out.println("Parsing is complete, execution time: " + (executionTime / 60000) + " minutes");
	}
}