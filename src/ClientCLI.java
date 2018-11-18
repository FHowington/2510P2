import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.ArrayList;

public class ClientCLI {


    public static void main(String[] args) {
        Scanner reader = new Scanner(System.in);  // Reading from System.
        final Client cl = new Client();

        if (args.length < 2) {
            System.out.println("Insufficient arguments.\nUsage: java ClientCLI <ServerAddress> <ServerPort>");
            return;
        }

        cl.connect(args[0], Integer.parseInt(args[1]));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down client");
                cl.disconnect();
            }
        });

        while (true) {
            System.out.println("\nEnter a command to execute. HELP for commands.");
            String s = reader.next();

            switch (s.toUpperCase()) {
                case "READ":
                    System.out.println(cl.read());
                    break;

                case "LOOP":
                    System.out.println("Enter file name");
                    File file = new File(reader.next());
                    System.out.println("Enter line to begin reading");
                    int begin = reader.nextInt();
                    System.out.println("Enter line to end reading");
                    int end = reader.nextInt();

                    Scanner sc = null;
                    try {
                        sc = new Scanner(file);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }

                    int i = 0;
                    while (i < begin) {
                        sc.nextLine();
                        i++;
                    }
                    while (sc.hasNextLine() && i < end) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        String next = sc.nextLine();
                        int nextVal = Integer.parseInt(next);
                        System.out.println(nextVal);
                        cl.update(nextVal);
                        i++;
                    }
                    System.out.println("Complete.");
                    break;

                case "UPDATE":
                    int val = reader.nextInt();
                    cl.update(val);
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

