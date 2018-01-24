package myhdfs.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by erzhang on 1/23/18.
 */
public class NameNode {

    public static void main(String[] args) throws IOException {
        ServerSocket nnSocket = null;
        try {
            nnSocket = new ServerSocket(9900);
            System.out.println("Start namenode....");
            int i = 1;
            while(i < 1000){
                Socket sc = nnSocket.accept();
                new Thread(new NameNodeHandler(sc)).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            nnSocket.close();
        }
    }



}
