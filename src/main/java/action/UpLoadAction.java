package action;

import mapreduce.data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;

public class UpLoadAction {
    private data data=null;

    private File file;

    public void setData(mapreduce.data data) {
        this.data = data;
    }

    public mapreduce.data getData() {
        return data;
    }

    private String fileFileName;
    private String fileContentType;

    public String getFileFileName() {
        return fileFileName;
    }

    public void setFileFileName(String fileFileName) {
        this.fileFileName = fileFileName;
    }

    public String getFileContentType() {
        return fileContentType;
    }

    public void setFileContentType(String fileContentType) {
        this.fileContentType = fileContentType;
    }
    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String upload() throws IOException {
        System.out.print(this.fileFileName);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://119.3.167.84:9000/");
        System.setProperty("HADOOP_USER_NAME","root");
        FileSystem fs=FileSystem.get(conf);
        FSDataOutputStream out=fs.create(new Path("/input_2017081098/"+this.fileFileName));
        FileInputStream in =new FileInputStream(file);
        long filelengtgh=this.file.length();
        byte[] filecontent=new byte[(int) filelengtgh];
        //FileOutputStream out =new FileOutputStream(new File("C:/Users/14708/Desktop/javaEE/"+this.fileFileName));
        /*BufferedReader read=new BufferedReader(new InputStreamReader(in,"UTF-8"));
        String line;*/
        in.read(filecontent);
        out.write(filecontent);
     /*  while ((line=read.readLine())!=null){
            System.out.print(line);
            out.write(line.getBytes("UTF-8"));
            out.writeBytes("\n");
        }*/
        out.close();
        //read.close();
        in.close();
        fs.close();

        data=new data("1","上传成功!");
        return "success";
    }
}
