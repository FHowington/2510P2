import java.lang.Thread;
import java.net.Socket;
import java.io.*;
import java.util.HashMap;

public class SearchHelperThread extends Thread {
    private final Socket socket;
    private SearchHelperInstance gs;

    private HashMap<String, Integer> result;

    SearchHelperThread(Socket _socket, SearchHelperInstance _gs) {
        socket = _socket;
        this.gs = _gs;
        result = new HashMap<>();
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

                    Envelope response = new Envelope("RESULTS");
                    response.addObject(null);
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
