import java.io.IOException;
import java.lang.Thread;
import java.net.Socket;
import java.util.ArrayList;

public class MasterInstance extends Thread {
    private ServerInstance gs;
    private ArrayList<Thread> runningServers;

    public MasterInstance(ArrayList<String> servers, ArrayList<Integer> ports, ServerInstance _gs) throws IOException {
        runningServers = new ArrayList<>();
        this.gs = _gs;

        for(int i=0; i<servers.size(); i++){
            Socket sock = new Socket(servers.get(i), ports.get(i));
            Thread t = new MasterThread(sock, this);
            runningServers.add(t);
        }
    }

    public void run() {
        for(Thread t: runningServers){
            t.start();
        }
    }

}
