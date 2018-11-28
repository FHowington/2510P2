import java.lang.Thread;
import java.net.Socket;
import java.io.*;
import java.util.*;

public class ServerThread extends Thread {
    private final Socket socket;
    public int type = 0;
    private int host;
    ObjectOutputStream output = null;
    ObjectInputStream input = null;
    private ServerInstance gs;

    public ServerThread(Socket _socket, ServerInstance _gs, int host) {
        socket = _socket;
        this.host = host;
        this.gs = _gs;
    }

    public void run() {
        boolean proceed = true;

        try {
            output = new ObjectOutputStream(socket.getOutputStream());
            input = new ObjectInputStream(socket.getInputStream());

            while (proceed) {
                Envelope message = (Envelope) input.readObject();
                System.out.println("Received message: " + message.getMessage());
                if (message.getMessage().equals("INDEX")) {
                    System.out.println("Indexing!");
                    gs.startIndexing((String)message.getObjContents().get(0),(Integer)message.getObjContents().get(1));
                }
                if (message.getMessage().equals("SEARCH")) {
                    System.out.println("Searching!");
                    gs.startSearching((HashSet<String>) message.getObjContents().get(0), this);
                }

                if(message.getMessage().equals("DISCONNECT")){
                    System.out.println("Removing client");
                    return;
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    public void sendResults(HashMap<String, Integer> results){
        Envelope response = new Envelope("RESULTS");
        response.addObject(sortHashMapByValues(results));
        try {
            output.writeObject(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static LinkedHashMap<String, Integer> sortHashMapByValues(
            HashMap<String, Integer> passedMap) {

        List<String> mapKeys = new ArrayList<>(passedMap.keySet());
        List<Integer> mapValues = new ArrayList<>(passedMap.values());
        mapValues.sort(Collections.reverseOrder());
        mapKeys.sort(Collections.reverseOrder());

        LinkedHashMap<String, Integer> sortedMap =
                new LinkedHashMap<>();

        for (Integer val : mapValues) {
            Iterator<String> keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                String key = keyIt.next();
                Integer comp1 = passedMap.get(key);

                if (comp1.equals(val)) {
                    keyIt.remove();
                    sortedMap.put(key, val);
                    break;
                }
            }
        }
        return sortedMap;
    }
}
