import java.lang.Thread;
import java.net.Socket;
import java.io.*;

public class ServerThread extends Thread {
    private final Socket socket;
    public int type = 0;
    private int host;
    ObjectOutputStream output = null;
    ObjectInputStream input = null;
    private ServerInstance gs;

    public boolean connected = false;

    public ServerThread(Socket _socket, ServerInstance _gs, int host) {
        socket = _socket;
        this.host = host;
        this.gs = _gs;
    }


    public void run() {
        boolean proceed = true;

        try {
            if (!connected) {
                if (host == 1) {
                    output = new ObjectOutputStream(socket.getOutputStream());
                    input = new ObjectInputStream(socket.getInputStream());
                    Envelope response = new Envelope("SERVER");
                    output.writeObject(response);
                    type = 2;
                    System.out.println("Connected to new server");
                } else {
                    System.out.println("Connection from " + socket.getInetAddress() + ":" + socket.getPort());
                    input = new ObjectInputStream(socket.getInputStream());
                    output = new ObjectOutputStream(socket.getOutputStream());
                }
                connected = true;
            }

            while (proceed) {
                Envelope message = (Envelope) input.readObject();
                System.out.println("Received message: " + message.getMessage());
                if (type == 0) {
                    if (message.getMessage().equals("CLIENT")) {
                        System.out.println("Connected to new Client");
                        type = 1;
                    } else if (message.getMessage().equals("SERVER")) {
                        System.out.println("Connected to new Server");
                        this.gs.addServer(this);
                        type = 2;
                    } else {
                        System.err.println("Error: Invalid handshake");
                        return;
                    }
                } else if (type == 1) {
                    // This is message handling if we are connected to a client

                    if (message.getMessage().equals("READ")) {
                        long t = System.nanoTime();
                        this.gs.addClientToQueue(new ServerInstance.Node(0, this, t));

                        //this.sendRequest(message.getMessage(), t);
                        // Response is handled by GroupServer
                    } else if (message.getMessage().equals("UPDATE")) {
                        this.gs.wInc();
                        long t = System.nanoTime();
                        this.gs.addClientToQueue(new ServerInstance.Node(1, (int) message.getObjContents().get(0), t));
                    } else if (message.getMessage().equals("DISCONNECT")) {
                        this.output.close();
                        this.input.close();
                        return;
                    }
                } else if (type == 2) {
                    if (message.getMessage().equals("REQUEST")) {
                        if (message.getObjContents().get(1).equals("READ")) {
                            this.gs.addRequestToQueue(new ServerInstance.Node(0, this, (long) message.getObjContents().get(0)));
                        } else { //Request to write
                            this.gs.addRequestToQueue(new ServerInstance.Node(1, this, (long) message.getObjContents().get(0)));
                            this.gs.wInc();
                        }
                    } else if (message.getMessage().equals("REPLY")) {
                        this.gs.replyHandler();
                    }
                    else if (message.getMessage().equals("WDONE")) {
                        this.gs.wDec();
                    }else if (message.getMessage().equals("DISCONNECT")) {
                        this.gs.removeServer(this);
                        this.output.close();
                        this.input.close();
                        return;
                    }
                }


            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    public synchronized void readResponse(String s) {
        System.out.println("Sending Message: COMPLETE");
        Envelope response = new Envelope("COMPLETE");
        response.addObject(s);
        try {
            output.writeObject(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //
    public synchronized void sendRequest(String op, long time) {
        Envelope response = new Envelope("REQUEST");
        response.addObject(time);
        response.addObject(op);

        System.out.println("Sending Message: REQUEST");
        try {
            output.writeObject(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void replyRequest() {
        System.out.println("Sending message: REPLY");
        Envelope reply = new Envelope("REPLY");

        try {
            output.writeObject(reply);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void writeAck() {
        System.out.println("Sending message: WDONE");
        Envelope reply = new Envelope("WDONE");
        try {
            output.writeObject(reply);
        } catch (Exception e) {
            e.printStackTrace();
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
