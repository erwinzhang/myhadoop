package myhdfs.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by erzhang on 1/23/18.
 */
public class Client {


    public static void main(String[] args) throws IOException {
        Socket toNN = null;
        try {
            //request for a upload file block
            toNN = new Socket("localhost", 9900);
            OutputStream toNNout = toNN.getOutputStream();
            InputStream toNNin = toNN.getInputStream();

            toNNout.write("WB:/aa/a.log:1".getBytes());
            toNNout.flush();

            byte[] b = new byte[1024];
            int read = toNNin.read(b);

            String response = new String(b,0,read);
            String[] split = response.split(":");
            String blkid = split[0];
            String dnHost = split[1];

            System.out.println(blkid);
            System.out.println(dnHost);


            toNNin.close();
            toNNout.close();

//        toNNout.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            toNN.close();
        }
    }

}
