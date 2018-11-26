import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread;
import java.net.Socket;
import java.util.*;

public class SearchMasterInstance extends Thread {
    private ServerInstance gs;
    private ArrayList<SearchMasterThread> runningServers;
    private HashMap<String, Integer> fileCounts = new HashMap<>();

    private int stillRunning = 0;
    private HashSet<String> terms;

    public SearchMasterInstance(ArrayList<String> servers, ArrayList<Integer> ports, ServerInstance _gs,
                                HashSet<String> terms, LinkedHashMap<String,
            HashMap<String, Integer>> masterTable) throws IOException {

        runningServers = new ArrayList<>();
        this.gs = _gs;
        this.terms = terms;

        int numPerServer = (int) Math.ceil((double) masterTable.size() / servers.size());


        Iterator<Map.Entry<String, HashMap<String, Integer>>> it = masterTable.entrySet().iterator();

        for (int i = 0; i < servers.size(); i++) {
            LinkedHashMap<String, HashMap<String, Integer>> split = new LinkedHashMap<>();
            int count = 0;
            while (it.hasNext() && count < numPerServer) {
                Map.Entry<String, HashMap<String, Integer>> entry = it.next();
                split.put(entry.getKey(), entry.getValue());
                count++;
            }
            if (count > 0) {
                Socket sock = new Socket(servers.get(i), ports.get(i));
                SearchMasterThread t = new SearchMasterThread(sock, this, terms, split);
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
}
