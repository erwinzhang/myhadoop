import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * Created by erzhang on 1/14/18.
 */
public class HDFSAPI {

    FileSystem fsClient = null;
    Configuration config = null;

    @Before
    public void init() throws Exception {
        config = new Configuration();
        config.set("dfs.replication","2");
        config.set("dfs.blocksize","32m");
        fsClient = FileSystem.get(new URI("hdfs://localhost:9000/"), config, "erzhang");
    }

    @Test
    public void testPutFile() throws Exception {
        fsClient.copyFromLocalFile(new Path("/Developer/DATA/test.txt"),new Path("/hd1.txt"));

    }

    @Test
    public void testGetFile() throws IOException {
        fsClient.copyToLocalFile(new Path("/hd1.txt"),new Path("/Developer/DATA/2.txt"));
    }

    @Test
    public void testMkDir() throws IOException {
        //fs.permission.umask-mode  default is 022, means it will remove "w" for added dirs for user and group
        //If want to add 777 permission, need to modify the configuration to 000
        fsClient.mkdirs(new Path("/tt/aa"), new FsPermission((short)777));
    }

    @Test
    public void delete() throws IOException {
        fsClient.delete(new Path("/tt"),true);
    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> ri = fsClient.listFiles(new Path("/"), true);
        while(ri.hasNext()){
            LocatedFileStatus file = ri.next();
            System.out.println("Block location:" + Arrays.toString(file.getBlockLocations()));
            System.out.println("Latest access time:" + file.getAccessTime());
            System.out.println("Latest replication number:" + file.getReplication());
            System.out.println("Latest full path:" + file.getPath());
            System.out.println("------------");
        }
    }

    @Test
    public void listDir() throws IOException {
        FileStatus[] fs = fsClient.listStatus(new Path("/"));
        for(FileStatus f : fs){
            System.out.println(f.getAccessTime());
        }

    }


    @Test
    public void testReadFilePart() throws Exception {
        FSDataInputStream hdfsIn = fsClient.open(new Path("/jmeter.log"));
        hdfsIn.seek(20); //Set start point when reading file
        FileOutputStream localOut = new FileOutputStream("/Developer/DATA/t.txt");
        byte[] b = new byte[10]; // Read 10 bytes every time

        int len = 0;
        long count = 0;
        while((len = hdfsIn.read(b)) != -1){
            localOut.write(b);
            count += len;
            if(count == 20) break;

        }

        localOut.flush();
        localOut.close();
        hdfsIn.close();
    }

    @Test
    public void testWriteHDFS() throws IOException {
        FSDataOutputStream hdfsOut = fsClient.create(new Path("/a.log"));
        hdfsOut.write("122".getBytes());
        hdfsOut.write("222".getBytes());
        hdfsOut.flush();
        hdfsOut.close();
    }




    @After
    public void tearDown() throws IOException {
        fsClient.close();
    }





    public static void main(String args[]) throws Exception {
        System.out.println("In...");
        FileSystem fsClient = null;
        Configuration config = null;
        try{
            //1. Create a HDFS client object
            config = new Configuration();
//          config.addResource("");
            config.set("dfs.replication","2");
            config.set("dfs.blocksize","32m");
            fsClient = FileSystem.get(new URI("hdfs://localhost:9000/"), config, "erzhang");

            //2. Upload a file
            fsClient.copyFromLocalFile(new Path("/Developer/DATA/test.txt"),new Path("/hd.txt"));


        }catch (Exception e){
            e.printStackTrace();
        }finally {
            fsClient.close();
        }



    }
}
