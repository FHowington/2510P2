import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


public class ServerInstance extends Server {

    private static final int SERVER_PORT = 8765;

    private ServerInstance() {
        super(SERVER_PORT, "ALPHA");
    }

    private ServerInstance(int _port) {
        super(_port, "ALPHA");
    }

    private static ArrayList<String> servers;
    private static ArrayList<Integer> ports;

    HashMap<String, HashMap<String,Integer>> masterTable = new HashMap<>();



    // Basically we should not send out more reqs when we are still waiting to hear back on current batch
    public void start() {
        try {

            final ServerSocket serverSock = new ServerSocket(port);

            Socket sock;
            ServerThread thread;

            servers = new ArrayList<>();
            ports = new ArrayList<>();


            Scanner reader = new Scanner(System.in);

            while(true) {
                System.out.println("Enter server and port of all indexing helpers, q when done");
                String server = reader.next();
                if (server.equals("q")) {
                    System.out.println("All indexing helpers finalized");
                    break;
                }
                Integer port = reader.nextInt();
                servers.add(server);
                ports.add(port);
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

    public void startIndexing(String path, int numToUse){
        System.out.println("Indexing");
        try {
            MasterInstance master = new MasterInstance(servers, ports, this, path);
            master.run();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        if (args.length > 0) {
            try {
                ServerInstance server = new ServerInstance(Integer.parseInt(args[0]));
                server.start();
            } catch (NumberFormatException e) {
                System.out.print("Enter a valid port number\n");
            }
        } else {
            ServerInstance server = new ServerInstance();
            server.start();
        }
    }

    public synchronized void updateMap(HashMap<String, Integer> si, String file){
        for(Map.Entry<String, Integer> entry: si.entrySet()){
            if(masterTable.containsKey(entry.getKey())){
                masterTable.get(entry.getKey()).put(file,entry.getValue());
            }
            else {
                HashMap<String, Integer> newhm = new HashMap<>();
                newhm.put(file, entry.getValue());
                masterTable.put(entry.getKey(), newhm);
            }
        }

    }
}

