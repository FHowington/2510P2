import java.lang.Thread;
import java.net.Socket;
import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

public class SearchMasterThread extends Thread {
    private final Socket socket;
    private SearchMasterInstance gs;
    private String path;
    private int beginning;
    private int end;

    SearchMasterThread(Socket _socket, SearchMasterInstance _gs, HashSet<String> terms,
                       LinkedHashMap<String, HashMap<String, Integer>> data) {
        socket = _socket;
        this.gs = _gs;
        this.path = path;
        this.beginning = beginning;
        this.end = end;
    }

    public void run() {

        try {
            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            Envelope response = new Envelope("INDEX");
            response.addObject(path);
            response.addObject(beginning);
            response.addObject(end);
            output.writeObject(response);

            Envelope message = (Envelope) input.readObject();
            System.out.println("Received message: " + message.getMessage());

            if (message.getMessage().equals("TOKENS")) {
                System.out.println("Got tokens!");
                HashMap<String, Integer> result = (HashMap<String, Integer>) message.getObjContents().get(0);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
