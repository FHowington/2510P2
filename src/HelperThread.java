import java.lang.Thread;
import java.net.Socket;
import java.io.*;

public class HelperThread extends Thread {
    private final Socket socket;
    public int type = 0;
    private int host;
    ObjectOutputStream output = null;
    ObjectInputStream input = null;
    private HelperInstance gs;

    public HelperThread(Socket _socket, HelperInstance _gs, int host) {
        socket = _socket;
        this.host = host;
        this.gs = _gs;
    }

    public void run() {
        boolean proceed = true;

        try {
            output = new ObjectOutputStream(socket.getOutputStream());
            input = new ObjectInputStream(socket.getInputStream());
            Envelope response = new Envelope("SERVER");
            output.writeObject(response);

            while (proceed) {
                Envelope message = (Envelope) input.readObject();
                System.out.println("Received message: " + message.getMessage());
                if (message.getMessage().equals("INDEX")) {
                    System.out.println("Indexing!");
                    gs.startIndexing();
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    public synchronized void disconnect() {
        System.out.println("Sending message: DISCONNECT");
        Envelope reply = new Envelope("DISCONNECT");
        try {
            output.writeObject(reply);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            this.output.close();
            this.input.close();
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
