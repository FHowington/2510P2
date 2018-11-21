import java.lang.Thread;
import java.net.Socket;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.stream.Stream;

public class HelperThread extends Thread {
    private final Socket socket;
    ObjectOutputStream output = null;
    ObjectInputStream input = null;
    private HelperInstance gs;

    public HelperThread(Socket _socket, HelperInstance _gs, int host) {
        socket = _socket;
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
                    System.out.println("Got index request");
                    Tokenize((String)message.getObjContents().get(0), (int)message.getObjContents().get(1), (int)message.getObjContents().get(2));
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }


    public void Tokenize(String path, int start, int end) {

        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            lines.skip(start).limit((end-start)).forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
