package action;

import mapreduce.OMapper;
import mapreduce.OReducer;
import mapreduce.OSubmit;
import mapreduce.data;
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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class xunlianAction {
    private String msg;
    private data data;
    public void setMsg(String msg) {
        this.msg = msg;
    }

    public data getData() {
        return data;
    }

    public void setData(data data) {
        this.data = data;
    }

    public String getMsg() {
        return msg;
    }

    public String test() throws InterruptedException, IOException, ClassNotFoundException, JSONException {
        //System.out.println("msg="+msg);
        Connection connect = Jsoup.connect("https://jsonin.com/fenci.php").ignoreContentType(true);
        // 带参数开始
        connect.data("msg", msg);
        // 带参数结束
        //Document document = connect.post();
        String ans=connect.execute().body().toString();
        JSONArray array = new JSONArray(ans);
        JSONObject jsonOb = null;
        ArrayList<String> list=OSubmit.readfile("/input_2017081099/2017081099_模型.txt");
        HashMap<String,Integer> map=new HashMap<>();
        for(String i:list){
            String arr[]=i.split("\t");
            map.put(arr[0],Integer.parseInt(arr[1]));
        }
        double ans1=1;
        int cnt1=0,cnt2=0;
        for (int i = 0; i < array.length(); i++) {
            String word=array.get(i).toString();
            String h="好评"+"_"+word;
            String c="差评"+"_"+word;
            if(map.containsKey(h)) {
                int sum=map.get(h);
                ans1*=sum;
                cnt1++;
            }
            if(map.containsKey(c)) {
                int sum=map.get(c);
                ans1/=sum;
                cnt2++;
            }

        }
        System.out.println(ans1+" "+cnt1+"  "+"  "+cnt2);
        if(ans1>=1||cnt1>cnt2) {
            data=new data("1","好评");
        }
        else {
            data=new data("1","差评");
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
        System.setProperty("HADOOP_USER_NAME","root");
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
