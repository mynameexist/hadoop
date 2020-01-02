package mapreduce;

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

public class OSubmit {
    private List<data> test=null;

    public List<data> getTest() {
        return test;
    }

    public void setTest(List<data> test) {
        this.test = test;
    }

    public static HashMap<String,Integer> map=new HashMap<>();
    public static Integer cnt=0;
    public  static  double ans1=1,ans2=1;
    public static  int cnt1=0,cnt2=0;
    public static void main(String[] args) throws IOException,
            URISyntaxException, InterruptedException, ClassNotFoundException {
        //init();
        //jisuan();
        new OSubmit().xunlian();
        //readfile("/input_2017081098/test-1000.txt");
    }
    public String xunlian() throws InterruptedException, IOException, ClassNotFoundException {
        cnt1=0;
        map.clear();
        jisuan();
        List<String> list=OSubmit.readfile("/input_2017081099/testoutput/part-r-00000");
        test=new ArrayList<>();
        for(String i : list){
            String arr[]=i.split("\t");
            if(arr.length!=2) continue;
            test.add(new data(arr[0],arr[1]));
        }
        return "success";
    }
    public static  void jisuan() throws IOException, ClassNotFoundException, InterruptedException {
        ArrayList<String> list=readfile("/input_2017081099/2017081099_模型.txt");
        for(String i:list){
            String arr[]=i.split("\t");
            map.put(arr[0],Integer.parseInt(arr[1]));
        }
        solve();
    }
    private static void solve1() throws InterruptedException, IOException, ClassNotFoundException {
        //Path类为hadoop API定义，创建两个Path对象，一个输入文件的路径，一个输入结果的路径
        Path outPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\testoutput");
        //Path outPath = new Path("hdfs://119.3.167.84:9000/input_2017081099/output");


        //输入文件的路径为本地linux系统的文件路径
        //Path inPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\test\\1.txt");
        Path inPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\test-1000.txt");
        //Path inPath = new Path("hdfs://119.3.167.84:9000/input_2017081099/training-1000.txt");
        //创建默认的Configuration对象
        Configuration conf = new Configuration();
        //根据conf创建一个新的Job对象，代表要提交的作业，作业名为JSubmit.class.getSimpleName()

        FileSystem fileSystem = FileSystem.get(conf);
//        Path path = new Path("hdfs://119.3.167.84:9000/input_2017081099/output");
//        FileSystem fileSystem = path.getFileSystem(conf);//上下不同点
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }

        Job job = Job.getInstance(conf);

        //1.1
        //FileInputFormat类设置要读取的文件路径
        FileInputFormat.setInputPaths(job, inPath);
        //setInputFormatClass设置读取文件时使用的格式化类
        job.setInputFormatClass(TextInputFormat.class);

        //1.2调用自定义的Mapper类的map方法进行操作
        //设置处理的Mapper类
        job.setMapperClass(OMapper.class);
        //设置Mapper类处理完毕之后输出的键值对 的 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        //1.4排序，分组

        //1.5归约，这三步都有默认的设置，如果没有特殊的需求可以不管
        //2.1将数据传输到对应的Reducer

        //2.2使用自定义的Reducer类操作
        //设置Reducer类
        job.setReducerClass(OReducer.class);
        //设置Reducer处理完之后 输出的键值对 的数据类型
        //job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        //2.3将结果输出
        //FileOutputFormat设置输出的路径
        FileOutputFormat.setOutputPath(job, outPath);
        //setOutputFormatClass设置输出时的格式化类
        job.setOutputFormatClass(TextOutputFormat.class);


        conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator","\t");


        //将当前的job对象提交
        job.waitForCompletion(true);
    }
    public static void solve() throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(OSubmit.class);
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
    private static void init() throws IOException, ClassNotFoundException, InterruptedException {
        //ArrayList<String> list=readFromTextFile("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\output\\part-r-00000");
        ArrayList<String> list=readFromTextFile("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\new.txt");
        System.out.println(list.size());
        for(String str : list){
            String []array=str.split("\t");
            //System.out.println(array);
            map.put(array[0],Integer.parseInt(array[1]));
        }
        list=readFromTextFile("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\test-1000.txt");
        int b1=0,b2=0;
        for(int j=0;j<list.size();j++) {
            String str=list.get(j);
            String []array=str.split("\t");
            String []arr=array[1].split(" ");
            ans1=ans2=1;
            cnt1=cnt2=0;
            for(String i :arr){
                String h="好评"+"_"+i;
                String c="差评"+"_"+i;
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
            System.out.print("ans1="+ans1+"  "+cnt1+"  "+cnt2+"   ");
            if(ans1>=1||cnt1>cnt2) {
                System.out.println("好评");
                if(j>=1000) b2++;
            }
            else {
                System.out.println("差评");
                if(j<1000) b1++;
            }
        }
        System.out.println(b1+"   "+b2);
    }
    public static ArrayList<String> readFromTextFile(String pathname) throws IOException{
        ArrayList<String> strArray = new ArrayList<String>();
        File filename = new File(pathname);
        InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
        BufferedReader br = new BufferedReader(reader);
        String line = "";
        line = br.readLine();
        while(line != null) {
            //System.out.println(line);
            strArray.add(line);
            line = br.readLine();
        }
        return strArray;
    }

    public static ArrayList<String> readfile(String remoteFilePath) throws IOException {
        ArrayList<String> strArray = new ArrayList<String>();
        Configuration conf=new Configuration();
        conf.set("fs.default.name", "hdfs://119.3.167.84:9000");
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        FSDataInputStream in = fs.open(remotePath);

        BufferedReader d = new BufferedReader(new InputStreamReader(in,"utf-8"));
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