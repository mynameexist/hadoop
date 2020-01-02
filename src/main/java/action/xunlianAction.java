package action;

import mapreduce.OMapper;
import mapreduce.OReducer;
import mapreduce.OSubmit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class xunlianAction {
    public String xunlian() throws InterruptedException, IOException, ClassNotFoundException {
        //jisuan();
        List<String> list= OSubmit.readfile("/5.txt");
        for(String i : list){
            System.out.println(i);
        }
        list=readfile("/input_2017081099/2017081099_模型.txt");
        for(String i : list){
            System.out.println(i);
        }
        return "success";
    }
    public static  void jisuan() throws IOException, ClassNotFoundException, InterruptedException {
        ArrayList<String> list=OSubmit.readfile("/input_2017081099/2017081099_模型.txt");
        for(String i:list){
            String arr[]=i.split("\t");
            OSubmit.map.put(arr[0],Integer.parseInt(arr[1]));
            System.out.println(i);
        }
        solve();
    }
    private static void solve() throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(xunlianAction.class);
        job.setMapperClass(OMapper.class);
        job.setReducerClass(OReducer.class);
        FileInputFormat.setInputPaths(job, new Path("hdfs://119.3.167.84:9000/input_2017081099/test-1000.txt"));
        Path outPath = new Path("hdfs://119.3.167.84:9000/input_2017081099/testoutput");
        FileSystem fileSystem = outPath.getFileSystem(new Configuration());//上下不同点
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job, new Path("hdfs://119.3.167.84:9000/input_2017081099/testoutput"));
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
    public static ArrayList<String> readfile(String remoteFilePath) throws IOException {
        ArrayList<String> strArray = new ArrayList<String>();
        Configuration conf=new Configuration();
        conf.set("fs.default.name", "hdfs://119.3.167.84:9000");
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        FSDataInputStream in = fs.open(remotePath);

        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = d.readLine()) != null) {
            strArray.add(line);
            //System.out.println(line);
        }
        d.close();
        in.close();
        fs.close();
        return strArray;
    }
}
