import java.lang.Thread;
import java.net.Socket;
import java.io.*;

public class MasterThread extends Thread {
    private final Socket socket;
    ObjectOutputStream output = null;
    ObjectInputStream input = null;
    private MasterInstance gs;
    String path;
    int beginning;
    int end;

    public MasterThread(Socket _socket, MasterInstance _gs, String path, int beginning, int end) {
        socket = _socket;
        this.gs = _gs;
        this.path = path;
        this.beginning = beginning;
        this.end = end;
    }

    public void run() {
        boolean proceed = true;

        try {
            output = new ObjectOutputStream(socket.getOutputStream());
            input = new ObjectInputStream(socket.getInputStream());
            Envelope response = new Envelope("INDEX");
            response.addObject(path);
            response.addObject(beginning);
            response.addObject(end);
            output.writeObject(response);

            while (proceed) {
                Envelope message = (Envelope) input.readObject();
                System.out.println("Received message: " + message.getMessage());

                if (message.getMessage().equals("INDEX")) {
                    System.out.println("Indexing!");
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
