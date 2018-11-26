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


    SearchMasterThread(Socket _socket, SearchMasterInstance _gs, HashSet<String> terms,
                       LinkedHashMap<String, HashMap<String, Integer>> data) {
        socket = _socket;
        this.gs = _gs;
        this.data = data;
        this.terms = terms;
    }

    public void run() {

        try {
            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            Envelope response = new Envelope("SEARCH");
            response.addObject(terms);
            response.addObject(data);
            output.writeObject(response);

            Envelope message = (Envelope) input.readObject();
            System.out.println("Received message: " + message.getMessage());

            if (message.getMessage().equals("RESULTS")) {
                System.out.println("Got results!");
                HashMap<String, Integer> result = (HashMap<String, Integer>) message.getObjContents().get(0);
                System.out.println(result);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
