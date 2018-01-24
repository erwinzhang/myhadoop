import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
        fsClient.close();
    }

    @Test
    public void testGetFile() throws IOException {
        fsClient.copyToLocalFile(new Path("/hd1.txt"),new Path("/Developer/DATA/2.txt"));
        fsClient.close();
    }

    @Test
    public void testMkDir() throws IOException {
        //fs.permission.umask-mode  default is 022, means it will remove "w" for added dirs for user and group
        //If want to add 777 permission, need to modify the configuration to 000
        fsClient.mkdirs(new Path("/tt/aa"), new FsPermission((short)777));
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
