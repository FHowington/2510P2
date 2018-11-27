import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class MasterInstance extends Thread {
    private ServerInstance gs;
    private ArrayList<MasterThread> runningServers;
    private HashMap<String, Integer> fileCounts = new HashMap<>();

    private int stillRunning = 0;
    private String path;

    public MasterInstance(ArrayList<String> servers, ArrayList<Integer> ports, ServerInstance _gs, String path) throws IOException {
        runningServers = new ArrayList<>();
        this.gs = _gs;

        int lc = countLines(path);
        int labor = lc/servers.size()+1;
        int pos = 0;

        for(int i=0; i<servers.size(); i++){
            int end = pos + labor;
            if(end > lc){
                end = lc;
            }

            Socket sock = new Socket(servers.get(i), ports.get(i));
            MasterThread t = new MasterThread(sock, this, path, pos, end);
            pos += labor;
            runningServers.add(t);
            this.path = path;
        }
    }

    public void run() {
        for(Thread t: runningServers){
            t.start();
            stillRunning++;
        }
    }


    private static int countLines(String filename){
        try {
            try (InputStream is = new BufferedInputStream(new FileInputStream(filename))) {
                byte[] c = new byte[1024];
                int count = 0;
                int readChars = 0;
                boolean empty = true;
                while ((readChars = is.read(c)) != -1) {
                    empty = false;
                    for (int i = 0; i < readChars; ++i) {
                        if (c[i] == '\n') {
                            ++count;
                        }
                    }
                }
                return (count == 0 && !empty) ? 1 : count;
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    public synchronized void updateMap(HashMap<String, Integer> si){
        si.forEach((key, value) -> fileCounts.merge(key, value, (v1, v2) -> v1+v2));
        stillRunning--;
        if(stillRunning == 0){
            gs.updateMap(fileCounts, path);
        }
    }
}
