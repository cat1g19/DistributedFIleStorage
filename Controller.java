import java.io.IOException;

public class Controller {
	
	public static void main(String[] args) {
		
		try {
			// parse arguments
			int cport = Integer.parseInt(args[0]);
			int replicationFactor = Integer.parseInt(args[1]);
			int timeout = Integer.parseInt(args[2]);
			int rebalancePeriod = Integer.parseInt(args[3]);			
			System.out.println("Controller started, listening on port " + cport + "; replication factor: " + replicationFactor + "; timeout: " + timeout + "ms; rebalance period: " + rebalancePeriod + "ms");
			
			// init logger
			ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
			System.out.println("ControllerLogger initialised");
			
			// log some dummy entries to generate a log file
			ControllerLogger.getInstance().log("Dummy log entry");
			
			// loop to keep the process running
			while (true)
				try {
					Thread.sleep(1000);
				} catch (Exception e) {}
			
		} catch (NumberFormatException e) {
			System.out.println("Error parsing arguments: " + e.getMessage());
			System.out.println("Expected: java Controller cport R timeout rebelancePeriod"); 
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error initialising the ControllerLogger");
			e.printStackTrace();
		}
	}
}
