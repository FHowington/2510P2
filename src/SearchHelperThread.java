import java.lang.Thread;
import java.net.Socket;
import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SearchHelperThread extends Thread {
    private final Socket socket;
    private SearchHelperInstance gs;

    SearchHelperThread(Socket _socket, SearchHelperInstance _gs) {
        socket = _socket;
        this.gs = _gs;
    }

    public void run() {
        boolean proceed = true;

        try {
            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());

            while (proceed) {
                Envelope message = (Envelope) input.readObject();
                System.out.println("Received message: " + message.getMessage());
                if (message.getMessage().equals("SEARCH")) {
                    System.out.println("Got search request");
                    LinkedHashMap<String, HashMap<String, Integer>> stringFileNumber =
                            (LinkedHashMap<String, HashMap<String, Integer>>) message.getObjContents().get(0);

                    HashMap<String, Integer> result = new HashMap<>();

                    // For every search term, update every file that contains the term
                    for(Map.Entry<String, HashMap<String, Integer>> entry: stringFileNumber.entrySet()){
                        if(entry.getValue() != null) {
                            for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
                                // We now have a hashmap of files and how many times an entry known to be searched for appears
                                result.put(innerEntry.getKey(), result.getOrDefault(innerEntry.getKey(), 0) + innerEntry.getValue());
                            }
                        }
                    }

                    Envelope response = new Envelope("RESULTS");
                    response.addObject(result);
                    output.writeObject(response);
                    return;
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }


}
