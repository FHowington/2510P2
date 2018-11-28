import java.net.Socket;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class Client {
    private Socket sock;
    private ObjectOutputStream output;
    private ObjectInputStream input;

    public boolean connect(final String server, final int port) {
        System.out.println("Attempting to connect");

        try {
            sock = new Socket(server, port);
            output = new ObjectOutputStream(sock.getOutputStream());
            input = new ObjectInputStream(sock.getInputStream());
            System.out.println("Connected to " + server + " on port " + port);

            Envelope response = new Envelope("CLIENT");
            output.writeObject(response);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            return false;
        }
        return true;
    }

    public boolean isConnected() {
        return sock != null && sock.isConnected();
    }

    public void disconnect() {
        if (isConnected()) {
            try {
                Envelope message = new Envelope("DISCONNECT");
                output.writeObject(message);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }

    public void index(String path, int helpers) {
        if (isConnected()) {
            try {
                Envelope message = new Envelope("INDEX");
                message.addObject(path);
                message.addObject(helpers);
                output.writeObject(message);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }

    public void search(HashSet<String> terms) {
        if (isConnected()) {
            try {
                Envelope message = new Envelope("SEARCH");
                message.addObject(terms);
                output.writeObject(message);

                message = (Envelope) input.readObject();

                if(message.getObjContents().size() > 0 && message.getObjContents().get(0) != null){
                    Iterator<Map.Entry<String, Integer>> result =
                            ((LinkedHashMap<String, Integer>) message.getObjContents().get(0)).entrySet().iterator();

                    if (result.hasNext()) {
                        System.out.println("Search results in descending relevance:");

                    }
                    else{
                        System.out.println("No results found.");
                    }

                    while (result.hasNext()){
                        System.out.println(result.next());
                    }
                }
                else{
                    System.out.println("No results found.");
                }

            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }
}
