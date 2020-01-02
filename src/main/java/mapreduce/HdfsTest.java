package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsTest {
    public void mkdir() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://119.3.167.84:9000");
        FileSystem fs = FileSystem.newInstance(conf);
        fs.mkdirs(new Path("/test1"));
        System.out.println("成功");
    }
    public static  void read() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://119.3.167.84:9000");
        FileSystem fs= FileSystem.get(conf);
        Path path=new Path("/input_2017081099/training-1000.txt");
        FSDataInputStream fis=fs.open(path);
        byte b[]=new byte[5000];
        int i=fis.read(b);
        System.out.println(new String(b,0,i));
    }
    HdfsTest(){

    }
    /**
     *src:hdfs路径  例：hdfs://192.168.0.168:9000/data/a.json
     *dst：本地路径
     */

    public static void down() throws IOException {

        String dest = "hdfs://119.3.167.84:9000/input_2017081099/testoutput/part-r-00000";
        String local = "C:\\Users\\ASUS\\Desktop\\test.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dest),conf);
        FSDataInputStream fsdi = fs.open(new Path(dest));
        OutputStream output = new FileOutputStream(local);
        IOUtils.copyBytes(fsdi,output,4096,true);
    }
    public static void del() throws IOException {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://119.3.167.84:9000");
        FileSystem fileSystem= FileSystem.get(conf);
        fileSystem.delete(new Path("/input_2017081099/test-1000.txt"),true);
    }
    public static  void up(String url){
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://119.3.167.84:9000"), conf, "root");
            FSDataOutputStream outputStream = null;
            FileInputStream fileInputStream = null;
            try {
                Path path = new Path("/input_2017081099/test.txt");
                outputStream = fs.create(path);
                fileInputStream = new FileInputStream(new File(url));
                //输入流、输出流、缓冲区大小、是否关闭数据流，如果为false就在 finally里关闭
                IOUtils.copyBytes(fileInputStream, outputStream,4096, false);

            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if(fileInputStream != null){
                    IOUtils.closeStream(fileInputStream);
                }
                if(outputStream != null){
                    IOUtils.closeStream(outputStream);
                }
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (URISyntaxException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("1");
    }
    public static void main(String args[]) throws IOException {
        down();
    }
}
