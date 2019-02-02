package client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/2/2 13:23
 */
public class FileIteratorClient implements Runnable{
    private String clientId;
    private String exec;

    private Socket net;
    private BufferedInputStream inputStream;
    private BufferedOutputStream outputStream;

    public FileIteratorClient(String id){
        clientId = id;
        exec = "hello world !";
    }

    @Override
    protected void finalize() throws Throwable {
        if (inputStream != null)
            inputStream.close();
        if (outputStream != null)
            outputStream.close();
        if (net != null)
            net.close();

        super.finalize();
    }

    public void connect(String serverAddr, int serverPort) throws IOException {
        net = new Socket(serverAddr, serverPort);
        inputStream = new BufferedInputStream(net.getInputStream());
        outputStream = new BufferedOutputStream(net.getOutputStream());
    }


    @Override
    public void run() {
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject("FROM ["+clientId+"] "+exec);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
