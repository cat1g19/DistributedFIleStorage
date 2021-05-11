import java.io.IOException;

public class Dstore {
	
	public static void main(String[] args) {

		try {
			// parse arguments
			int port = Integer.parseInt(args[0]);
			int cport = Integer.parseInt(args[1]);
			int timeout = Integer.parseInt(args[2]);
			String fileFolder = args[3];
			System.out.println("Dstore started, listening on port " + port + "; Controller port: " + cport + "; timeout: " + timeout + "ms; file folder: " + fileFolder);
			
			// init logger
			DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
			System.out.println("DstoreLogger initialised");
			
			// log some dummy entries to generate a log file
			DstoreLogger.getInstance().log("Dummy log entry");
			
			// loop to keep the process running
			while (true)
				try {
					Thread.sleep(1000);
				} catch (Exception e) {}
			
		} catch (NumberFormatException e) {
			System.out.println("Error parsing arguments: " + e.getMessage());
			System.out.println("Expected: java Dstore port cport timeout file_folder"); 
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error initialising the DstoreLogger");
			e.printStackTrace();
		}
	}
}
