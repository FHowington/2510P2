import java.lang.Thread;
import java.net.Socket;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.stream.Stream;

public class HelperThread extends Thread {
    private final Socket socket;
    private HelperInstance gs;

    private HashMap<String, Integer> result;

    HelperThread(Socket _socket, HelperInstance _gs) {
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
                if (message.getMessage().equals("INDEX")) {
                    System.out.println("Got index request");
                    stringPull((String)message.getObjContents().get(0), (int)message.getObjContents().get(1), (int)message.getObjContents().get(2));

                    Envelope response = new Envelope("TOKENS");
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


    private void stringPull(String path, int start, int end) {

        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            lines.skip(start).limit((end-start)).forEach(this::tokenize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void tokenize(String s){
        StringTokenizer st = new StringTokenizer(s);

        while(st.hasMoreTokens()){
            String t = st.nextToken();
            result.put(t,result.getOrDefault(t,0) + 1);
        }
    }
}
