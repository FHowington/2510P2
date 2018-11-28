import java.io.IOException;
import java.lang.Thread;
import java.net.Socket;
import java.util.*;

public class SearchMasterInstance extends Thread {
    private ServerInstance gs;
    private ArrayList<SearchMasterThread> runningServers;
    private HashMap<String, Integer> fileCounts = new HashMap<>();

    private int stillRunning = 0;
    ServerThread client;

    public SearchMasterInstance(ArrayList<String> servers, ArrayList<Integer> ports, ServerInstance _gs,
                                HashSet<String> terms, LinkedHashMap<String,
            HashMap<String, Integer>> masterTable, ServerThread client) throws IOException {

        runningServers = new ArrayList<>();
        this.gs = _gs;
        this.client = client;

        int numPerServer = (int) Math.ceil((double) terms.size() / servers.size());


        Iterator<String> it = terms.iterator();

        for (int i = 0; i < servers.size(); i++) {
            LinkedHashMap<String, HashMap<String, Integer>> split = new LinkedHashMap<>();
            int count = 0;
            while (it.hasNext() && count < numPerServer) {
                String entry = it.next();
                split.put(entry, masterTable.get(entry));
                count++;
            }

            if (count > 0) {
                Socket sock = new Socket(servers.get(i), ports.get(i));
                SearchMasterThread t = new SearchMasterThread(sock, this, split);
                runningServers.add(t);
            }
        }
    }

    public void run() {
        for (Thread t : runningServers) {
            t.start();
            stillRunning++;
        }
    }

    public synchronized void searchResult(HashMap<String, Integer> si){
        si.forEach((key, value) -> fileCounts.merge(key, value, (v1, v2) -> v1+v2));
        stillRunning--;
        if(stillRunning == 0){
            gs.searchResult(fileCounts, client);
        }
    }
}
