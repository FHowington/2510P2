import java.util.Scanner;

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
                case "READ":
                    System.out.println("Specify the path to the document to be indexed and the number of helpers to use");
                    String path = reader.next();
                    int helpers = reader.nextInt();

                    cl.index(path, helpers);
                    System.out.println("Indexing request sent to server");
                    break;

                case "EXIT":
                    return;

                case "HELP":
                    System.out.println("Commands:\nUPDATE: Updates file by adding a 1 to last element in central file\n" +
                            "LOOP: Loops through commands in given file" +
                            "READ: Performs read on last line in file" +
                            "EXIT: Exits client");
                    break;
            }
        }
    }
}

