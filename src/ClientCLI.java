import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;

public class ClientCLI {


    public static void main(String[] args) {
        Scanner reader = new Scanner(System.in);  // Reading from System.
        final Client cl = new Client();

        if (args.length < 2) {
            System.out.println("Insufficient arguments.\nUsage: java ClientCLI <ServerAddress> <ServerPort>");
            return;
        }

        cl.connect(args[0], Integer.parseInt(args[1]));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down client");
            cl.disconnect();
        }));

        while (true) {
            System.out.println("\nEnter a command to execute. HELP for commands.");
            String s = reader.next();

            switch (s.toUpperCase()) {
                case "INDEX":
                    System.out.println("Specify the path to the document to be indexed");
                    String path = reader.next();
                    cl.index(path);
                    break;


                case "SEARCH":
                    System.out.println("Specify all search terms on a single line");
                    reader.nextLine();
                    StringTokenizer st = new StringTokenizer(reader.nextLine());
                    HashSet<String> terms = new HashSet<>();

                    while(st.hasMoreTokens()) {
                        terms.add(st.nextToken());
                    }

                    cl.search(terms);
                    break;

                case "EXIT":
                    return;

                case "HELP":
                    System.out.println("Commands:\nINDEX: Completely indexes file at given location\n" +
                            "SEARCH: Searches for files containing given terms\n" +
                            "EXIT: Exits client");
                    break;
            }
        }
    }
}

