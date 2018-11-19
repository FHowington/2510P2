import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;


public class HelperInstance extends Server {

    private static final int SERVER_PORT = 8765;

    private HelperInstance() {
        super(SERVER_PORT, "ALPHA");
    }

    private HelperInstance(int _port) {
        super(_port, "ALPHA");
    }

    private static ArrayList<String> servers;
    private static ArrayList<Integer> ports;



    // Basically we should not send out more reqs when we are still waiting to hear back on current batch
    public void start() {
        try {

            final ServerSocket serverSock = new ServerSocket(port);

            Socket sock = null;
            HelperThread thread = null;

            servers = new ArrayList<>();


            while (true) {
                sock = serverSock.accept();
                thread = new HelperThread(sock, this, 0);
                thread.start();
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    public void startIndexing(){
        System.out.println("Indexing");
    }



    public static void main(String[] args) {
        if (args.length > 0) {
            try {
                HelperInstance server = new HelperInstance(Integer.parseInt(args[0]));
                server.start();
            } catch (NumberFormatException e) {
                System.out.printf("Enter a valid port number\n",
                        HelperInstance.SERVER_PORT);
            }
        } else {
            HelperInstance server = new HelperInstance();
            server.start();
        }
    }
}

