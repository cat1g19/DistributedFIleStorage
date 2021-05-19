import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Controller {
    private static int replicationFactor;
    private static int cPort;
    private static int timeout;
    private static int rebalancePeriod;
    private static ServerSocket serverSocket;
    private static FileIndex fileIndex;
    //private static Socket client;
    // printWriter used to send control messages using println()
    private static PrintWriter printWriter;
    // bufferedReader used to receive control messages using readLine()
    //private static BufferedReader bufferedReader;
    private static ConcurrentHashMap<Socket, Integer> dstoreSocketToPortNo = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException{
        int cPort = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        new Controller(cPort, replicationFactor, timeout, rebalancePeriod);

        try {
            serverSocket = new ServerSocket(cPort);

            while (true) {
                try{
                    System.out.println("Server running, awaits connections");
                    Socket client = serverSocket.accept();
                    //client.setSoTimeout(timeout);
                    //System.out.println("Port " + client.getPort());
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try{

                                System.out.println("Server connected with port " + client.getPort());
                                //printWriter = new PrintWriter(client.getOutputStream());
                                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));

                                String message;
                                while ((message = bufferedReader.readLine()) != null) {
                                    //System.out.println(message + " message received");
                                    String[] messageParts = message.split(" ");

                                    Lock readLock = fileIndex.getUpdateLock().readLock();
                                    Lock writeLock = fileIndex.getUpdateLock().writeLock();

                                    switch (messageParts[0]) {
                                        // dataStore joining the system
                                        case (Protocol.JOIN_TOKEN):
                                            writeLock.lock();
                                            //System.out.println("WriterLock has been locked");
                                            try {
                                                dstoreJoins(client, messageParts);
                                                System.out.println("dstoreJoins");
                                            } finally {
                                                writeLock.unlock();
                                                //System.out.println("WriterLock has been unlocked");
                                            }
                                            break;
                                        case (Protocol.STORE_TOKEN):
                                            writeLock.lock();
                                            //System.out.println("WriteLock has been locked");
                                            try {
                                                storeFile(client, messageParts);
                                                System.out.println("storeFile");
                                            } finally {
                                                writeLock.unlock();
                                                //System.out.println("WriteLock has been unlocked");
                                            }
                                            break;
                                        case (Protocol.STORE_ACK_TOKEN):
                                            writeLock.lock();
                                            //System.out.println("WriteLock has been locked");
                                            try {
                                                storeAck(client, messageParts);
                                                System.out.println("storeAck");
                                            } finally {
                                                writeLock.unlock();
                                                //System.out.println("WriteLock has been unlocked");
                                            }
                                            break;
                                        case (Protocol.LOAD_TOKEN):
                                            readLock.lock();
                                            //System.out.println("ReadLock has been locked");
                                            try {
                                                loadFile(client, messageParts);
                                                System.out.println("loadFile");
                                            } finally {
                                                readLock.unlock();
                                                //System.out.println("ReadLock has been unlocked");
                                            }
                                            break;
                                        case (Protocol.RELOAD_TOKEN):
                                            readLock.lock();
                                            //System.out.println("ReadLock has been locked");
                                            try {
                                                reloadFile(client, messageParts);
                                                System.out.println("reloadFile");
                                            } finally {
                                                readLock.unlock();
                                                //System.out.println("ReadLock has been unlocked");
                                            }
                                            break;
                                        case (Protocol.LIST_TOKEN):
                                            readLock.lock();
                                            //System.out.println("ReadLock has been locked");
                                            try {
                                                listFiles(client, messageParts);
                                                System.out.println("listFiles");
                                            } finally {
                                                readLock.unlock();
                                                //System.out.println("ReadLock has been unlocked");
                                            }
                                            break;
                                        case (Protocol.REMOVE_TOKEN):
                                            writeLock.lock();
                                            //System.out.println("WriteLock has been locked");
                                            try {
                                                removeFile(client, messageParts);
                                                System.out.println("removeFile");
                                            } finally {
                                                writeLock.unlock();
                                                //System.out.println("WriteLock has been unlocked");
                                            }
                                            break;
                                        case (Protocol.REMOVE_ACK_TOKEN):
                                            writeLock.lock();
                                            //System.out.println("WriteLock has been locked");
                                            try {
                                                removeAck(client, messageParts);
                                                System.out.println("removeAck");
                                            } finally {
                                                writeLock.unlock();
                                                //System.out.println("WriteLock has been unlocked");
                                            }
                                            break;
                                        default:
                                            System.err.println("Malformed message");
                                    }
                                }
                                //System.out.println("Buffer closed");
                                //bufferedReader.close();
                            } catch (Exception e){
                                System.err.println("Error " + e);
                            }
                        }
                    }).start();
                } catch (Exception e){
                    System.err.println("Error " + e);
                }
            }
        } catch (SocketException e){
            System.err.println("Error " + e);
        }
    }

    public static void dstoreJoins (Socket dstoreSocket, String[] messageParts) throws IOException{
        if (messageParts.length == 2){
            ControllerLogger.getInstance().messageReceived(dstoreSocket,messageParts[0] + " " + messageParts[1]);
            int dstorePort = Integer.parseInt(messageParts[1]);
            ControllerLogger.getInstance().dstoreJoined(dstoreSocket,dstorePort);
            fileIndex.dstoreJoin(dstoreSocket);
            dstoreSocketToPortNo.put(dstoreSocket, dstorePort);
        } else {
            ControllerLogger.getInstance().log("Malformed JOIN message from dstore port " + dstoreSocket.getLocalPort());
            System.err.println("Malformed JOIN message from dstore port " + dstoreSocket.getLocalPort());
        }
    }

    public static void storeFile (Socket clientSocket, String[] messageParts) throws IOException{
        PrintWriter printWriter = new PrintWriter(clientSocket.getOutputStream());

        if (enoughDstores()) {
            if (messageParts.length == 3) {
                ControllerLogger.getInstance().messageReceived(clientSocket, messageParts[0] + " " + messageParts[1] + " " + messageParts[2]);
                String filename = messageParts[1];
                int filesize = Integer.parseInt(messageParts[2]);

                if (!(fileIndex.getFileToState().get(filename) == FileIndex.IndexState.STORE_IN_PROGRESS) && !(fileIndex.getFileToState().get(filename) == FileIndex.IndexState.STORE_COMPLETE) ) {
                    StringBuilder clientResponse = new StringBuilder(Protocol.STORE_TO_TOKEN);
                    Vector<Socket> allocatedDstores = new Vector<>();

                    fileIndex.getDstoresToNoOfFiles().entrySet().stream()
                            .sorted(Comparator.comparingInt(entry -> entry.getValue()))
                            .limit(replicationFactor)
                            .forEach(entry -> {
                                allocatedDstores.add(entry.getKey());
                                clientResponse.append(' ').append(dstoreSocketToPortNo.get(entry.getKey()));
                            });

                    fileIndex.storeFile(filename, filesize, clientSocket, allocatedDstores);
                    if (!clientSocket.isClosed() && clientSocket.isConnected()) {
                        printWriter.println(clientResponse.toString());
                        printWriter.flush();
                        ControllerLogger.getInstance().messageSent(clientSocket,clientResponse.toString());
                    }
                } else {
                    if (!clientSocket.isClosed() && clientSocket.isConnected()) {
                        printWriter.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                        printWriter.flush();
                        ControllerLogger.getInstance().messageSent(clientSocket,Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    }
                }
            } else {
                ControllerLogger.getInstance().log("Malformed STORE message from client port " + clientSocket.getLocalPort());
                System.err.println("Malformed STORE message from client port " + clientSocket.getLocalPort());
            }
        } else{
            sendNotEnoughDstores(printWriter,clientSocket);
        }
        //printWriter.close();
        //clientSocket.close();
    }

    public static void storeAck (Socket dstoreSocket, String[] messageParts) throws IOException{
        if(messageParts.length == 2) {
            ControllerLogger.getInstance().messageReceived(dstoreSocket, messageParts[0] + " " + messageParts[1]);
            String fileName = messageParts[1];

            //System.out.println(fileIndex.getFileToState().get(fileName));
            /**Checks if the file is being stored.*/
            if(fileIndex.getFileToState().get(fileName) == FileIndex.IndexState.STORE_IN_PROGRESS) {
                int requiredAcks = fileIndex.getFilesToAcks().get(fileName).decrementAndGet();
                /**Updates the index and decrements the required acknowledgements.*/
                fileIndex.getFilesToDstores().get(fileName).add(dstoreSocket);
                fileIndex.getDstoresToFiles().get(dstoreSocket).add(fileName);
                fileIndex.getDstoresToNoOfFiles().put(dstoreSocket,fileIndex.getDstoresToFiles().get(dstoreSocket).size());

                /**Checks if an appropriate number of STORE_ACKs have been processed.
                 * If so, updates the state of the file to the default state in the file index.*/
                if(requiredAcks == 0) {
                    fileIndex.getFileToState().put(fileName, FileIndex.IndexState.STORE_COMPLETE);
                    //fileIndex.getStateTimers().remove(fileName);
                    fileIndex.getFilesToAcks().remove(fileName);

                    // Send STORE_COMPLETE to client
                    Socket client  = fileIndex.getFilesToStateClients().get(fileName);
                    PrintWriter writerToClient = new PrintWriter(client.getOutputStream());
                    /*
                    if (!client.isClosed() && client.isConnected()) {
                        writerToClient.println(Protocol.STORE_COMPLETE_TOKEN);
                        writerToClient.flush();
                        ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);
                    }

                     */
                    writerToClient.println(Protocol.STORE_COMPLETE_TOKEN);
                    writerToClient.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);
                    //writerToClient.close();
                    //client.close();
                }

            } else {
                if (!dstoreSocket.isClosed() && dstoreSocket.isConnected()) {
                    PrintWriter dstoreWriter = new PrintWriter(dstoreSocket.getOutputStream());
                    dstoreWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    dstoreWriter.flush();
                    ControllerLogger.getInstance().messageSent(dstoreSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    //dstoreWriter.close();
                    //dstoreSocket.close();
                } else{
                    ControllerLogger.getInstance().log("Got non-existent file ack from crushed dstore");
                }
            }
        } else{
            ControllerLogger.getInstance().log("Malformed STORE_ACK message from dstore port " + dstoreSocket.getLocalPort());
            System.err.println("Malformed STORE_ACK message from dstore port " + dstoreSocket.getLocalPort());
        }
    }

    public static void loadFile(Socket clientSocket, String[] messageParts) throws IOException{
        PrintWriter clientWriter = new PrintWriter(clientSocket.getOutputStream());

        if (enoughDstores()) {
            if (messageParts.length == 2) {
                ControllerLogger.getInstance().messageReceived(clientSocket,messageParts[0] + " " + messageParts[1]);
                String fileName = messageParts[1];
                clientSocket.setSoTimeout(timeout);

                /**Checks if the file can be loaded according to the file index.*/
                if (fileIndex.getFileToState().get(fileName) == FileIndex.IndexState.STORE_COMPLETE) {
                    Socket dstoreSocket = fileIndex.getFilesToDstores().get(fileName).get(0);
                    fileIndex.loadFile(fileName, clientSocket, dstoreSocket);
                    if (!clientSocket.isClosed() && clientSocket.isConnected()) {
                        clientWriter.println(Protocol.LOAD_FROM_TOKEN + " " + dstoreSocketToPortNo.get(dstoreSocket) + " " + fileIndex.getFileToSize().get(fileName));
                        clientWriter.flush();
                        ControllerLogger.getInstance().messageSent(clientSocket, Protocol.LOAD_FROM_TOKEN + " " + dstoreSocketToPortNo.get(dstoreSocket) + " " + fileIndex.getFileToSize().get(fileName));
                    }
                } else {
                    if (clientSocket.isConnected() && !clientSocket.isClosed()) {
                        clientWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        clientWriter.flush();
                        ControllerLogger.getInstance().messageSent(clientSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    }
                }
            } else {
                ControllerLogger.getInstance().log("Malformed LOAD message from client port " + clientSocket.getLocalPort());
                System.err.println("Malformed LOAD message from client port " + clientSocket.getLocalPort());
            }
        }
        else {
            sendNotEnoughDstores(clientWriter,clientSocket);
        }

        //clientWriter.close();
        //clientSocket.close();
    }

    public static void reloadFile(Socket clientSocket, String[] messageParts) throws IOException{
        PrintWriter clientWriter = new PrintWriter(clientSocket.getOutputStream());

        if (enoughDstores()){
            /**Checks if the message is formatted correctly.*/
            if(messageParts.length == 2) {
                ControllerLogger.getInstance().messageReceived(clientSocket, messageParts[0] + " " + messageParts[1]);
                String fileName = messageParts[1];
                clientSocket.setSoTimeout(timeout);

                /**Checks if the file can be reloaded according to the file index.*/
                if(fileIndex.containsFile(fileName)) {
                    /**Gets the Dstores the client has already tried to load from.*/
                    List<Socket> loadAttempts = fileIndex.getLoadAttempts().get(clientSocket).get(fileName);
                    /**Gets one of the Dstores the client hasn't loaded from that contains the file.*/
                    Socket nextDstore = fileIndex.getFilesToDstores().get(fileName).stream()
                            .filter(Predicate.not(loadAttempts::contains))
                            .findFirst()
                            .orElse(null);

                    /**Checks if there is a remaining Dstore, if so, sends its port and adds the Dstore to the consumed list.
                     * If not, removes the load attempts for the file from the map for the client.*/
                    if(nextDstore != null) {
                        loadAttempts.add(nextDstore);
                        clientWriter.println(Protocol.LOAD_FROM_TOKEN + " " + dstoreSocketToPortNo.get(nextDstore) + " " + fileIndex.getFileToSize().get(fileName));
                        clientWriter.flush();
                        ControllerLogger.getInstance().messageSent(clientSocket,Protocol.LOAD_FROM_TOKEN + " " + dstoreSocketToPortNo.get(nextDstore) + " " + fileIndex.getFileToSize().get(fileName));
                    } else {
                        fileIndex.getLoadAttempts().get(clientSocket).remove(fileName);
                        printWriter.println(Protocol.ERROR_LOAD_TOKEN);
                        printWriter.flush();
                        ControllerLogger.getInstance().messageSent(clientSocket,Protocol.ERROR_LOAD_TOKEN);
                    }
                } else if(! fileIndex.containsFile(fileName)) {
                    clientWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    clientWriter.flush();
                    ControllerLogger.getInstance().messageSent(clientSocket,Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            } else {
                ControllerLogger.getInstance().log("Malformed RELOAD message from client port " + clientSocket.getLocalPort());
                System.err.println("Malformed RELOAD message from client port " + clientSocket.getLocalPort());
            }
        }
        else {
            sendNotEnoughDstores(clientWriter,clientSocket);
        }

        //clientWriter.close();
        //clientSocket.close();
    }

    public static void listFiles(Socket clientSocket, String[] messageParts) throws IOException{
        PrintWriter clientWriter = new PrintWriter(clientSocket.getOutputStream());
        if (enoughDstores()){
            if(messageParts.length == 1) {
                ControllerLogger.getInstance().messageReceived(clientSocket, messageParts[0]);
                StringBuilder clientResponse = new StringBuilder(Protocol.LIST_TOKEN);
                fileIndex.getFileToState().keySet().stream()
                        .filter(fileName -> fileIndex.getFileToState().get(fileName) == FileIndex.IndexState.STORE_COMPLETE)
                        .forEach(fileName -> clientResponse.append(' ').append(fileName));
                clientWriter.println(clientResponse.toString());
                clientWriter.flush();
                ControllerLogger.getInstance().messageSent(clientSocket,clientResponse.toString());
            } else {
                ControllerLogger.getInstance().log("Malformed LIST message from client port " + clientSocket.getLocalPort());
                System.err.println("Malformed LIST message from client port " + clientSocket.getLocalPort());
            }
        }
        else {
            sendNotEnoughDstores(clientWriter,clientSocket);
        }

        //clientWriter.close();
        //clientSocket.close();
    }

    public static void removeFile(Socket clientSocket, String[] messageParts) throws IOException{
        PrintWriter clientWriter = new PrintWriter(clientSocket.getOutputStream());
        if (enoughDstores()){
            /**Checks if the message is formatted correctly.*/
            if(messageParts.length == 2) {
                ControllerLogger.getInstance().messageReceived(clientSocket, messageParts[0] + " " + messageParts[1]);
                String fileName = messageParts[1];

                /**Checks if the file can be removed according to the file index.*/
                if(fileIndex.getFileToState().get(fileName) == FileIndex.IndexState.STORE_COMPLETE) {
                    List<Socket> storingDstores = fileIndex.getFilesToDstores().get(fileName);
                    fileIndex.removeFile(fileName, clientSocket, storingDstores);
                    for(Socket dstore : storingDstores){
                        PrintWriter dstoreWriter = new PrintWriter(dstore.getOutputStream());
                        dstoreWriter.println(Protocol.REMOVE_TOKEN + " " + fileName);
                        dstoreWriter.flush();
                        ControllerLogger.getInstance().messageSent(dstore,Protocol.REMOVE_TOKEN + " " + fileName);
                        //dstoreWriter.close();
                        //dstore.close();
                    }
                } else {
                    clientWriter.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    clientWriter.flush();
                    ControllerLogger.getInstance().messageSent(clientSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
            } else{
                ControllerLogger.getInstance().log("Malformed REMOVE message from client port " + clientSocket.getLocalPort());
                System.err.println("Malformed REMOVE message from client port " + clientSocket.getLocalPort());
            }
        }
        else {
            sendNotEnoughDstores(clientWriter,clientSocket);
        }

        //clientWriter.close();
        //clientSocket.close();
    }

    public static void removeAck(Socket dstoreSocket, String[] messageParts) throws IOException{
        /**Checks if the message is formatted correctly.*/
        if(messageParts.length == 2) {
            ControllerLogger.getInstance().messageReceived(dstoreSocket, messageParts[0] + " " + messageParts[1]);
            String fileName = messageParts[1];

            /**Checks if the file is being removed.*/
            if(fileIndex.getFileToState().get(fileName) == FileIndex.IndexState.REMOVE_IN_PROGRESS) {
                /**Decrements the required acknowledgements.*/
                int requiredAcks = fileIndex.getFilesToAcks().get(fileName).decrementAndGet();

                /**Checks if an appropriate number of REMOVE_ACKs have been processed.
                 * If so, removes the file from the file index.*/
                if(requiredAcks == 0) {
                    fileIndex.getFileToState().remove(fileName);
                    //fileIndex.getStateTimers().remove(fileName);
                    fileIndex.getFilesToAcks().remove(fileName);
                    Socket client = fileIndex.getFilesToStateClients().get(fileName);
                    PrintWriter clientWriter = new PrintWriter(client.getOutputStream());
                    clientWriter.println(Protocol.REMOVE_COMPLETE_TOKEN);
                    clientWriter.flush();
                    ControllerLogger.getInstance().messageSent(client,Protocol.REMOVE_COMPLETE_TOKEN);
                    //clientWriter.close();
                    //client.close();
                }
            }
        } else {
            ControllerLogger.getInstance().log("Malformed REMOVE_ACK message from dstore port " + dstoreSocket.getLocalPort());
            System.err.println("Malformed REMOVE_ACK message from client port " + dstoreSocket.getLocalPort());
        }
    }

    public static boolean enoughDstores(){
        int n = 0;
        for (Socket dstore : dstoreSocketToPortNo.keySet()){
            if (!dstore.isClosed() && dstore.isConnected()){
                n++;
            } else{
                dstoreSocketToPortNo.remove(dstore);
                fileIndex.updateDstores(dstore);
            }
        }
        return n >= replicationFactor;
    }

    public static void sendNotEnoughDstores(PrintWriter clientWriter, Socket clientSocket){
        if (!clientSocket.isClosed() && clientSocket.isConnected()) {
            clientWriter.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            clientWriter.flush();
            ControllerLogger.getInstance().messageSent(clientSocket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        }
    }

    public Controller(int cPort, int replicationFactor, int timeout, int rebalancePeriod){
        this.cPort = cPort;
        this.replicationFactor = replicationFactor;
        this.fileIndex = new FileIndex(timeout, rebalancePeriod);
        this.rebalancePeriod = rebalancePeriod;

        /**Starts the update thread.*/
        new Thread(() -> {
            while(!Thread.interrupted()) {
                fileIndex.update();
            }
        }).start();
    }
}
