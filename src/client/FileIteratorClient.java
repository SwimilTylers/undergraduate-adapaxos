package client;

import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericClientMessage;

import java.io.*;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/2/2 13:23
 */
public class FileIteratorClient implements Runnable{
    private String clientId;
    private BufferedReader reader;

    private Socket net;

    public FileIteratorClient(String id, File file){
        clientId = id;
        try {
            reader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            reader = null;
        }
    }

    public FileIteratorClient(String id){
        clientId = id;
        try {
            reader = new BufferedReader(new FileReader("client.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            reader = null;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (net != null)
            net.close();
        if (reader != null)
            reader.close();

        super.finalize();
    }

    public void connect(String serverAddr, int serverPort) throws IOException {
        net = new Socket(serverAddr, serverPort);
    }


    @Override
    public void run() {
        String str = "";
        try{
            while ((str = reader.readLine()) != null){
                synchronized (this) {
                    System.out.println("client: " + str);
                    sendServerMessage(net, new GenericClientMessage.Propose(str));
                    wait(3000);
                }
            }
        } catch (Exception e){e.printStackTrace();}
    }

    synchronized public void sendServerMessage(@NotNull Socket server, @NotNull GenericClientMessage msg){
        try {
            OutputStream socketStream = server.getOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(socketStream);
            outputStream.writeObject(msg);

            outputStream.flush();
            socketStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
