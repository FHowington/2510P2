import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Scanner;


public class ServerInstance extends Server {

    private static final int SERVER_PORT = 8765;

    private ServerInstance() {
        super(SERVER_PORT, "ALPHA");
    }

    private ServerInstance(int _port) {
        super(_port, "ALPHA");
    }

    private static ArrayList<ServerThread> servers;
    private static LinkedList<Node> localQueue;
    private static LinkedList<Node> incomingQueue;
    public int replies = 0;
    public int writes = 0;

    String fileLocation = "db.txt";
    File file;
    // Basically we should not send out more reqs when we are still waiting to hear back on current batch
    boolean amWaiting = false;

    public void start() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("Shutting down server");
                    for (ServerThread s : servers) {
                        s.disconnect();
                    }
                }
            });


            Scanner reader = new Scanner(System.in);

            file = new File(fileLocation);
            final ServerSocket serverSock = new ServerSocket(port);

            Socket sock = null;
            ServerThread thread = null;

            servers = new ArrayList<>();

            localQueue = new LinkedList<>();
            incomingQueue = new LinkedList<>();

            while (true) {
                System.out.println("Enter server address and port to connect to, or q to exit");
                String server = reader.next();
                if (server.equals("q")) {
                    System.out.println("All outgoing connections finalized");
                    break;
                }
                int port = reader.nextInt();
                Socket sockOut = new Socket(server, port);
                thread = new ServerThread(sockOut, this, 1);
                thread.start();
                servers.add(thread);
            }


            while (true) {
                sock = serverSock.accept();
                thread = new ServerThread(sock, this, 0);
                thread.start();
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    public synchronized void addClientToQueue(Node n) {
        if(servers.size() > 0) {
            localQueue.add(n);
            if (!amWaiting && !instantRead()) {
                if (n.operation == 0) {
                    if(!instantRead()) {
                        broadcastReq("READ", n.timestamp);
                    }
                } else {
                    broadcastReq("UPDATE", n.timestamp);
                }
                amWaiting = true;
            }
        }
        else{
            if (n.operation == 0) {
                n.thread.readResponse(tail(file));
            } else {
                int current = Integer.parseInt(tail(file));
                try {
                    FileWriter fr = new FileWriter(file, true);
                    int incoming = n.message;
                    fr.write(Integer.toString(incoming + current) + "\n");
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public synchronized void addRequestToQueue(Node n) {
        incomingQueue.add(n);
        requestHandler();
    }

    public void addServer(ServerThread gt) {
        servers.add(gt);
    }

    public void removeServer(ServerThread gt) {
        servers.remove(gt);
    }




    public synchronized void replyHandler() {
        replies++;
        if (replies == servers.size()) {
            Node fulfill = localQueue.removeFirst();
            replies -= servers.size();
            if (fulfill.operation == 0) {
                fulfill.thread.readResponse(tail(file));
            } else {
                int current = Integer.parseInt(tail(file));
                try {
                    FileWriter fr = new FileWriter(file, true);
                    int incoming = fulfill.message;
                    fr.write(Integer.toString(incoming + current) + "\n");
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                broadcastWComplete();
            }
            amWaiting = false;

            // Basically want to fulfill all reads if possible
            instantRead();

            if (!localQueue.isEmpty()) {
                Node next = localQueue.getFirst();
                if (next.operation == 0) {
                    broadcastReq("READ", next.timestamp);
                } else {
                    broadcastReq("UPDATE", next.timestamp);
                }
                amWaiting = true;
            }
            // Now we need to run this to reply to anyone who may have been queued
            requestHandler();
        }
    }

    private synchronized void broadcastReq(String type, long time) {
        for (ServerThread s : servers) {
            s.sendRequest(type, time);
        }
    }

    private synchronized void broadcastWComplete() {
        this.wDec();
        for (ServerThread s : servers) {
            s.writeAck();
        }
    }

    private synchronized void requestHandler() {
        // Basically we want to respond to every other persons request in the queue whose
        // TS is smaller than the one at the head of our queue
        System.out.println("Handling requests");
        long minTime;

        if (!localQueue.isEmpty()) {
            minTime = localQueue.getFirst().timestamp;
        } else {
            minTime = Long.MAX_VALUE;
        }

        System.out.println(minTime);
        while (!incomingQueue.isEmpty() && incomingQueue.getFirst().timestamp < minTime) {
            System.out.println(incomingQueue.getFirst().timestamp);
            incomingQueue.removeFirst().thread.replyRequest();
        }
    }


    public synchronized boolean instantRead() {
        boolean retval = false;

        Node current;
        if(!localQueue.isEmpty()) {
            current = localQueue.getFirst();
        }
        else{
            return false;
        }
        if(writes != 0){
            return false;
        }

        while (current != null && current.operation == 0) {
            int i = 0;
            Node remoteCurrent;
            if(!incomingQueue.isEmpty()) {
                remoteCurrent = incomingQueue.getFirst();
            }
            else{
                remoteCurrent = null;
            }
            // If every waiting operation from another server with lower timestamp is a read,
            // issue immediately
            boolean canIssue = true;
            while (remoteCurrent != null && remoteCurrent.timestamp < current.timestamp) {
                if (remoteCurrent.operation == 0) {
                    remoteCurrent = incomingQueue.get(++i);
                } else {
                    canIssue = false;
                    break;
                }
            }
            if (canIssue) {
                current.thread.readResponse(tail(file));
                localQueue.removeFirst();
                retval = true;
            } else {
                break;
            }
            if(!localQueue.isEmpty()){
                current = localQueue.getFirst();
            }
            else{
                break;
            }
        }
        return retval;
    }

    public synchronized void wInc(){
        this.writes++;
    }

    public synchronized void wDec(){
        this.writes--;
    }


    public static void main(String[] args) {
        if (args.length > 0) {
            try {
                ServerInstance server = new ServerInstance(Integer.parseInt(args[0]));
                server.start();
            } catch (NumberFormatException e) {
                System.out.printf("Enter a valid port number or pass no arguments to use the default port (%d)\n",
                        ServerInstance.SERVER_PORT);
            }
        } else {
            ServerInstance server = new ServerInstance();
            server.start();
        }
    }


    public static class Node {
        // 0 for read, 1 for write

        int operation;
        ServerThread thread;
        int message;
        long timestamp;


        // If we need to write, no need for thread reference
        public Node(int op, int message, long time) {
            this.operation = op;
            this.message = message;
            this.timestamp = time;
        }

        public Node(int op, ServerThread thread, long time) {
            this.operation = op;
            this.thread = thread;
            this.timestamp = time;
        }
    }


    // Taken from
    // https://stackoverflow.com/questions/686231/quickly-read-the-last-line-of-a-text-file/22105812
    public String tail(File file) {
        RandomAccessFile fileHandler = null;
        try {
            fileHandler = new RandomAccessFile(file, "r");
            long fileLength = fileHandler.length() - 1;
            StringBuilder sb = new StringBuilder();

            for (long filePointer = fileLength; filePointer != -1; filePointer--) {
                fileHandler.seek(filePointer);
                int readByte = fileHandler.readByte();

                if (readByte == 0xA) {
                    if (filePointer == fileLength) {
                        continue;
                    }
                    break;

                } else if (readByte == 0xD) {
                    if (filePointer == fileLength - 1) {
                        continue;
                    }
                    break;
                }

                sb.append((char) readByte);
            }

            String lastLine = sb.reverse().toString();
            return lastLine;
        } catch (java.io.FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (java.io.IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (fileHandler != null)
                try {
                    fileHandler.close();
                } catch (IOException e) {
                    /* ignore */
                }
        }
    }
}

