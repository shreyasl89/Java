import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

public class S3Archiver
{
	private static class processLineResult
	{
		String previousFilePath;
		int eventCount;
		
		public processLineResult()
		{
			previousFilePath = "";
			eventCount = 0;
		}
	}
	private static Path sourcePath;
	private static int maxEventCountPerFile = 500000;
	private static ExecutorService executorService;
	
	private static void processDirectory(Path directoryPath) throws IOException {
		
		File directory = directoryPath.toFile();
		if (directory.isDirectory() && directory.getName().equals("processedReports"))
			return;

        // Process only files under this directory because we have a dedicated thread to process directories 
        for (File file : directory.listFiles()) {
        	if (!file.isDirectory() && !file.getName().equals(".DS_Store"))
            {
        		processFile(file);
            }
        }
    }
	
	private static void processFile(File file) throws IOException
	{
		System.out.println("Processing file: "+ file.getName());
		GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));
	    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream));
	    
	    String lineEntry;
	    processLineResult previousProcessLineResult = new processLineResult();
	    while ((lineEntry = bufferedReader.readLine()) != null)
	    {
	    	
	    	previousProcessLineResult = processLine(lineEntry, previousProcessLineResult);
	    	previousProcessLineResult.eventCount++;
	    }
	    
	    bufferedReader.close();
	    gzipInputStream.close();
	}
	
	private static processLineResult processLine(String lineEntry, processLineResult previousProcessLineResult) throws IOException
	{
		String[] columns = lineEntry.split(",");
    	columns[1] = columns[1].replace("%3D", "=").replace("\"", "");
    	String[] columnParts = columns[1].split("/");
    	
    	StringBuilder destnPath = new StringBuilder(sourcePath.toString());
    	destnPath.append("/processedReports/")
    				.append(columnParts[1])
    				.append("/")
    				.append(columnParts[0])
    				.append("/")
    				.append(columnParts[2])
    				.append("/")
    				.append(columnParts[3]);
    		
    	if (previousProcessLineResult.previousFilePath.equals("") 
				|| !destnPath.toString().equals(previousProcessLineResult.previousFilePath.substring(0, previousProcessLineResult.previousFilePath.lastIndexOf('/')))
				|| previousProcessLineResult.eventCount >= maxEventCountPerFile)
		{
    		Files.createDirectories(Paths.get(destnPath.toString()));
			previousProcessLineResult.previousFilePath = destnPath.toString() + "/" + UUID.randomUUID().toString() + ".csv";
			previousProcessLineResult.eventCount = 0;
			
			File tarDestn = new File(destnPath.toString() + "/archiveDestinationPath.txt");
	    	if (!tarDestn.exists())
	    	{
	    		FileWriter fw = new FileWriter(tarDestn);
	    		fw.write(columns[1].substring(0, columns[1].indexOf("/hr")) + "/archivedData");
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
	
	public static void main(String[] args) throws IOException
	{
		long startTime = System.currentTimeMillis();
		
		sourcePath = Paths.get(args[0]);
		executorService = Executors.newFixedThreadPool(Integer.valueOf(args[1]));
		
		// Parallelize only at directory level and not file level to ensure that we don't hit S3 prefix throttling
		List<Future<Void>> futures = new ArrayList<>();
        Files.walk(sourcePath)
                .filter(Files::isDirectory)
                .forEach(path -> futures.add((Future<Void>) executorService.submit(() -> {
					try {
						// Ignore the root directory, else reports will be processed twice
						if (path.compareTo(sourcePath) != 0 && !path.toString().contains("processedReports")) 
						{
							processDirectory(path);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				})));
        
        // Process files at root level, if any; sequentially to ensure that we don't hit S3 prefix throttling
        for (File file : sourcePath.toFile().listFiles()) {
        	if (!file.isDirectory() && !file.getName().equals(".DS_Store"))
        	{
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

		System.out.println("Processing is complete, execution time: " + (executionTime/60000) + " minutes");
        
		//List<String> list = new ArrayList<String>();
		
		/*ProcessBuilder pb = new ProcessBuilder("/Users/slakshminarayana/Documents/codebase/amazon-s3-tar-tool/bin/s3tar-darwin-amd64", "--region", "us-east-1", "--storage-class", "DEEP_ARCHIVE", "-cvf", "s3://telegraph-test-aws-glue/tp=billing_eu_subscriptions/archive_Nov27.tar", "s3://telegraph-test-aws-glue/tp=billing_eu_subscriptions/");

        // Start the process
        Process process = pb.start();
        try
        {
        	process.waitFor();
		}
        catch (InterruptedException e)
        {
			e.printStackTrace();
		}*/
    }
}
