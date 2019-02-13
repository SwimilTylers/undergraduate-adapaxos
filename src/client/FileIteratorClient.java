package client;

import network.message.protocols.GenericClientMessage;

import java.io.*;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/2/2 13:23
 */
public class FileIteratorClient implements Runnable{
    private String clientId;
    private String exec;

    private Socket net;

    public FileIteratorClient(String id){
        clientId = id;
        exec = "hello world !";
    }

    @Override
    protected void finalize() throws Throwable {
        if (net != null)
            net.close();

        super.finalize();
    }

    public void connect(String serverAddr, int serverPort) throws IOException {
        net = new Socket(serverAddr, serverPort);
    }


    @Override
    public void run() {
        try {
            OutputStream socketStream = net.getOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(socketStream);
            outputStream.writeObject(new GenericClientMessage.Propose("FROM ["+clientId+"] "+exec));
            outputStream.flush();
            socketStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
