package myhdfs.server;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Random;

/**
 * Created by erzhang on 1/23/18.
 */
public class NameNodeHandler implements Runnable {

    private Socket sc;
    private String[] datanodes = {"localhost","node-1","node-2"};

    public NameNodeHandler(Socket sc) {
        this.sc = sc;
    }

    public void run() {
        try {
            InputStream fromClientIn = sc.getInputStream();
            OutputStream toClientOut = sc.getOutputStream();

            byte[] b = new byte[1024];
            int read = fromClientIn.read(b);
            String request = new String(b,0,read);
            System.out.println("NameNode get request from client:" + request);
            String[] split = request.split(":");
            String command = split[0];
            String path = split[1];
            String blockNum = split[2];



            long timestamp = System.currentTimeMillis();
            int nextInt = new Random().nextInt(3);
            String dn = datanodes[nextInt];

            //Send data to client
            toClientOut.write((timestamp+":"+dn).getBytes());
            toClientOut.flush();
            toClientOut.close();
            fromClientIn.read();
            sc.close();
        }catch (Exception e){

        }

    }
}
