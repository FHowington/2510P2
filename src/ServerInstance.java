import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


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

    private static ArrayList<String> searchServers;
    private static ArrayList<Integer> searchPorts;

    private LinkedHashMap<String, HashMap<String,Integer>> masterTable = new LinkedHashMap<>();



    // Basically we should not send out more reqs when we are still waiting to hear back on current batch
    public void start() {
        try {

            final ServerSocket serverSock = new ServerSocket(port);

            Socket sock;
            ServerThread thread;

            servers = new ArrayList<>();
            ports = new ArrayList<>();
            searchServers = new ArrayList<>();
            searchPorts = new ArrayList<>();


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

            while(true) {
                System.out.println("Enter server and port of all searching helpers, q when done");
                String server = reader.next();
                if (server.equals("q")) {
                    System.out.println("All indexing helpers finalized");
                    break;
                }
                Integer port = reader.nextInt();
                searchServers.add(server);
                searchPorts.add(port);
            }

            while (true) {
                sock = serverSock.accept();
                thread = new ServerThread(sock, this);
                thread.start();
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    public void startIndexing(String path){
        System.out.println("Indexing");
        try {
            MasterInstance master = new MasterInstance(servers, ports, this, path);
            master.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startSearching(HashSet<String> terms, ServerThread client){
        System.out.println("Indexing");

        try {
            SearchMasterInstance master = new SearchMasterInstance(searchServers, searchPorts, this, terms,
                    masterTable, client);
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

    public synchronized void searchResult(HashMap<String, Integer> si, ServerThread client){
        client.sendResults(si);
    }
}

