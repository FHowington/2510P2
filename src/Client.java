import java.net.Socket;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Client {
    protected Socket sock;
    protected ObjectOutputStream output;
    protected ObjectInputStream input;

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
        if (sock == null || !sock.isConnected()) {
            return false;
        } else {
            return true;
        }
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

    public String read() {
        if (isConnected()) {
            try {
                Envelope message = new Envelope("INDEX");
                output.writeObject(message);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
        return null;
    }
}
