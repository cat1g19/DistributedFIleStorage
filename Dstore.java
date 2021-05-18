import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Dstore{
    private static int timeout;
    private static int port;
    private static int cPort;
    private static String directoryName;
    private static ServerSocket serverSocket;
    private static Socket controllerSocket;
    private static ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

    public static void main(String[] args) {
        try{
            System.out.println(args[0]+" "+args[1]+" "+args[2]+" "+args[3]);
            port = Integer.parseInt(args[0]);
            cPort = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            directoryName = args[3];
            File storingDirectory = new File(directoryName);

            if (storingDirectory.exists() == false) {
                if (storingDirectory.mkdir() == false) throw new RuntimeException(
                        "Cannot create storing directory at location " + storingDirectory.getAbsolutePath()
                );
            }

            DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);

            try{
                // Communication with the Controller
                try{
                    controllerSocket = new Socket("localhost", cPort);

                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                // communication tools for talking to the Controller
                                PrintWriter printWriter = new PrintWriter(controllerSocket.getOutputStream());
                                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
                                //OutputStream outputStream = controllerSocket.getOutputStream();
                                //InputStream inputStream = controllerSocket.getInputStream();

                                // send initial JOIN message to the Controller
                                printWriter.println(Protocol.JOIN_TOKEN + " " + port);
                                DstoreLogger.getInstance().messageSent(controllerSocket, Protocol.JOIN_TOKEN + " " + port);
                                printWriter.flush();

                                // receive command messages from the Controller
                                String command;
                                while ((command = bufferedReader.readLine()) != null){
                                    String commandParts[] = command.split(" ");
                                    switch (commandParts[0]){
                                        case Protocol.REMOVE_TOKEN:
                                            readWriteLock.writeLock().lock();
                                            System.out.println("WriteLock has been locked");
                                            try {
                                                removeFile(controllerSocket, commandParts);
                                            } finally {
                                                readWriteLock.writeLock().unlock();
                                                System.out.println("WriteLock has been unlocked");
                                            }
                                            break;
                                    }
                                }

                                //printWriter.close();
                                //bufferedReader.close();
                                //inputStream.close();
                                //outputStream.close();
                            } catch (Exception e){
                                System.err.println("Error " + e);
                            }
                        }
                    }).start();
                } catch (Exception e){
                    System.err.println("Error " + e);
                }

                try{
                    serverSocket = new ServerSocket(port);

                    while (true){
                        try{
                            if (controllerSocket.isConnected()){
                                Socket clientSocket = serverSocket.accept();
                                System.out.println("Dstore made a connection with client from port " + clientSocket.getPort());
                                //clientSocket.setSoTimeout(timeout);
                                new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        try{
                                            // Communication tools with the Controller
                                            //PrintWriter printWriter = new PrintWriter(controllerSocket.getOutputStream());
                                            //BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
                                            //OutputStream outputStream = controllerSocket.getOutputStream();
                                            //InputStream inputStream = controllerSocket.getInputStream();
                                            //System.out.println("Dstore made a connection with a Client");

                                            // Communication tools with the Client
                                            //PrintWriter clientPrintWriter = new PrintWriter(clientSocket.getOutputStream());
                                            BufferedReader clientBufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                                            //OutputStream clientOutputStream = clientSocket.getOutputStream();
                                            //InputStream clientInputStream = clientSocket.getInputStream();

                                            String message;
                                            while ((message = clientBufferedReader.readLine()) != null){
                                                System.out.println(message + " message received");
                                                String[] msgParts = message.split(" ");
                                                switch (msgParts[0]){
                                                    case Protocol.STORE_TOKEN:
                                                        readWriteLock.writeLock().lock();
                                                        System.out.println("WriteLock has been locked");
                                                        try {
                                                            System.out.println("storeFile");
                                                            storeFile(controllerSocket, clientSocket, msgParts);
                                                            System.out.println("storeFile");
                                                        } finally {
                                                            readWriteLock.writeLock().unlock();
                                                            System.out.println("WriteLock has been unlocked");
                                                        }
                                                        break;
                                                    case Protocol.LOAD_DATA_TOKEN:
                                                        readWriteLock.readLock().lock();
                                                        System.out.println("ReadLock has been locked");
                                                        try {
                                                            System.out.println("loadFile");
                                                            loadFile(clientSocket, msgParts);
                                                            System.out.println("loadFile");
                                                        } finally {
                                                            readWriteLock.readLock().unlock();
                                                            System.out.println("ReadLock has been unlocked");
                                                        }
                                                        break;
                                                }
                                            }

                                            //System.out.println("BufferdReader closed");
                                            //clientBufferedReader.close();
                                            //System.out.println("Client Socket closed");
                                            //clientSocket.close();
                                        } catch (Exception e){
                                            System.err.println("Error " + e);
                                        }
                                    }
                                }).start();
                                //System.out.println("BufferdReader closed");
                                //clientBufferedReader.close();
                            }
                        } catch (Exception e){
                            System.err.println("Error " + e);
                        }
                    }

                } catch (Exception e){
                    System.err.println("Error " + e);
                }
            } catch (Exception e){
                System.err.println("Error " + e);
            }
        } catch (Exception e){
            System.err.println("Error " + e);
        }
    }

    public static void storeFile(Socket controllerSocket, Socket clientSocket,String[] msgParts) throws IOException{
        String message = msgParts[0] + " " + msgParts[1] + " " + msgParts[2];
        if (msgParts.length == 3){
            PrintWriter clientPrintWriter = new PrintWriter(clientSocket.getOutputStream());
            InputStream clientInputStream = clientSocket.getInputStream();
            PrintWriter printWriter = new PrintWriter(controllerSocket.getOutputStream());

            DstoreLogger.getInstance().messageReceived(clientSocket,message);
            String filename = msgParts[1];
            int filesize = Integer.parseInt(msgParts[2]);

            if (!clientSocket.isClosed() && clientSocket.isConnected()) {
                clientPrintWriter.println(Protocol.ACK_TOKEN);
                clientPrintWriter.flush();
                DstoreLogger.getInstance().messageSent(clientSocket, Protocol.ACK_TOKEN);
            }

            try{
                File outputFile = new File(directoryName + File.separator + filename);
                if (outputFile.createNewFile()){
                    try {
                        ExecutorService timeoutService = Executors.newSingleThreadExecutor();
                        timeoutService.execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Files.write(outputFile.toPath(), clientInputStream.readNBytes(filesize));
                                    if (!controllerSocket.isClosed() && controllerSocket.isConnected()) {
                                        printWriter.println(Protocol.STORE_ACK_TOKEN + " " + filename);
                                        printWriter.flush();
                                        DstoreLogger.getInstance().messageSent(controllerSocket, Protocol.STORE_ACK_TOKEN + " " + filename);
                                    }
                                    System.out.println("File written and Ack sent to controller");
                                } catch (IOException e){
                                    System.err.println("Error " + e);
                                }
                            }
                        });
                        timeoutService.shutdown();
                        boolean storedWithinTimeout =  timeoutService.awaitTermination(timeout, TimeUnit.MILLISECONDS);

                        /**If not stored within the timeout successfully, logs the error.*/
                        if(!storedWithinTimeout) {
                            System.err.println("Could not store \"" + filename + "\" within timeout!");
                            DstoreLogger.getInstance().log("Could not store \"" + filename + "\" within timeout!");
                        }

                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("File already exists.");
                }
            } catch (Exception e){
                System.err.println("Error " + e);
            }
            //printWriter.close();
            //clientPrintWriter.close();
            //clientInputStream.close();
        } else{
            DstoreLogger.getInstance().log("Malformed STORE message from client port " + clientSocket.getPort());
            System.out.println("Malformed STORE message from client port " + clientSocket.getPort());
        }
    }

    public static void loadFile(Socket clientSocket, String[] messageParts) throws IOException {
        /**Checks if the message is formatted correctly.*/
        if (messageParts.length == 2) {
            DstoreLogger.getInstance().messageReceived(clientSocket,messageParts[0] + " " + messageParts[1]);
            String filename = messageParts[1];
            File file = new File(directoryName, filename);

            /**Checks if the file exists, closes the connection if not.*/
            if (file.exists()) {
                clientSocket.getOutputStream().write(Files.readAllBytes(file.toPath()));

                clientSocket.shutdownOutput();
            } else {
                clientSocket.close();
            }
        } else {
            DstoreLogger.getInstance().log("Malformed LOAD_DATA message from client port " + clientSocket.getPort());
            System.out.println("Malformed LOAD_DATA message from client port " + clientSocket.getPort());
        }
    }

    public static void removeFile(Socket controllerSocket, String[] messageParts) throws IOException {
        PrintWriter controllerWriter = new PrintWriter(controllerSocket.getOutputStream());
        /**Checks if the message is formatted correctly.*/
        if (messageParts.length == 2) {
            DstoreLogger.getInstance().messageReceived(controllerSocket, messageParts[0] + " " + messageParts[1]);
            String filename = messageParts[1];
            File file = new File(directoryName + File.separator + filename);

            /**Checks if the file exists.*/
            if (file.exists()) {
                Files.delete(file.toPath());
                controllerWriter.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                controllerWriter.flush();
                DstoreLogger.getInstance().messageSent(controllerSocket,Protocol.REMOVE_ACK_TOKEN + " " + filename);
            } else {
                controllerWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
                controllerWriter.flush();
                DstoreLogger.getInstance().messageSent(controllerSocket,Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
            }
            //controllerWriter.close();
        } else {
            DstoreLogger.getInstance().log("Malformed REMOVE message from controller port");
            System.out.println("Malformed REMOVE message from controller");
        }
    }
}