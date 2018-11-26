import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class SearchMasterInstance extends Thread {
    private ServerInstance gs;
    private ArrayList<SearchMasterThread> runningServers;
    private HashMap<String, Integer> fileCounts = new HashMap<>();

    private int stillRunning = 0;
    private HashSet<String> terms;

    public SearchMasterInstance(ArrayList<String> servers, ArrayList<Integer> ports, ServerInstance _gs, HashSet<String> terms) throws IOException {
        runningServers = new ArrayList<>();
        this.gs = _gs;
        this.terms = terms;

        for(int i=0; i<servers.size(); i++){


            Socket sock = new Socket(servers.get(i), ports.get(i));
            SearchMasterThread t = new SearchMasterThread(sock, this, terms);
            runningServers.add(t);
        }
    }

    public void run() {
        for(Thread t: runningServers){
            t.start();
            stillRunning++;
        }
    }
}
