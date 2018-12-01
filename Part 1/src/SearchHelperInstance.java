import java.net.ServerSocket;
import java.net.Socket;

public class SearchHelperInstance extends Server {

    private static final int SERVER_PORT = 8765;

    private SearchHelperInstance() {
        super(SERVER_PORT, "ALPHA");
    }

    private SearchHelperInstance(int _port) {
        super(_port, "ALPHA");
    }



    // Basically we should not send out more reqs when we are still waiting to hear back on current batch
    public void start() {
        try {

            final ServerSocket serverSock = new ServerSocket(port);

            Socket sock;
            SearchHelperThread thread ;


            while (true) {
                sock = serverSock.accept();
                thread = new SearchHelperThread(sock, this);
                thread.start();
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            try {
                SearchHelperInstance server = new SearchHelperInstance(Integer.parseInt(args[0]));
                server.start();
            } catch (NumberFormatException e) {
                System.out.println("Enter a valid port number");
            }
        } else {
            SearchHelperInstance server = new SearchHelperInstance();
            server.start();
        }
    }
}

