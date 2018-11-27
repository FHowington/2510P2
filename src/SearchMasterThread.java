import java.lang.Thread;
import java.net.Socket;
import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

public class SearchMasterThread extends Thread {
    private final Socket socket;
    private SearchMasterInstance gs;
    LinkedHashMap<String, HashMap<String, Integer>> data;
    HashSet<String> terms;


    SearchMasterThread(Socket _socket, SearchMasterInstance _gs,
                       LinkedHashMap<String, HashMap<String, Integer>> data) {
        socket = _socket;
        this.gs = _gs;
        this.data = data;
    }

    public void run() {

        try {
            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            Envelope response = new Envelope("SEARCH");
            response.addObject(data);
            output.writeObject(response);

            Envelope message = (Envelope) input.readObject();
            System.out.println("Received message: " + message.getMessage());

            if (message.getMessage().equals("RESULTS")) {
                HashMap<String, Integer> result = (HashMap<String, Integer>) message.getObjContents().get(0);
                gs.searchResult(result);
                System.out.println("Sent results to master");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
