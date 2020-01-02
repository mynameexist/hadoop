package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class TSubmit {
    public static HashMap<String,Integer> map=new HashMap<>();
    public static Integer cnt1=0,cnt2=0;
    public  static  double ans1=1,ans2=1;
    public static void main(String[] args) throws IOException,
            URISyntaxException, InterruptedException, ClassNotFoundException {
        init();
        //solve();
    }
    private static void solve() throws InterruptedException, IOException, ClassNotFoundException {
        //Path类为hadoop API定义，创建两个Path对象，一个输入文件的路径，一个输入结果的路径
        Path outPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\testoutput");


        //输入文件的路径为本地linux系统的文件路径
        //Path inPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\test\\1.txt");
        Path inPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\output\\part-r-00000");
        //创建默认的Configuration对象
        Configuration conf = new Configuration();
        //根据conf创建一个新的Job对象，代表要提交的作业，作业名为JSubmit.class.getSimpleName()

        FileSystem fileSystem = FileSystem.get(conf);
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
        job.setMapperClass(TMapper.class);
        //设置Mapper类处理完毕之后输出的键值对 的 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        //1.4排序，分组

        //1.5归约，这三步都有默认的设置，如果没有特殊的需求可以不管
        //2.1将数据传输到对应的Reducer

        //2.2使用自定义的Reducer类操作
        //设置Reducer类
        job.setReducerClass(TReducer.class);
        //设置Reducer处理完之后 输出的键值对 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //2.3将结果输出
        //FileOutputFormat设置输出的路径
        FileOutputFormat.setOutputPath(job, outPath);
        //setOutputFormatClass设置输出时的格式化类
        job.setOutputFormatClass(TextOutputFormat.class);


        conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator","\t");


        //将当前的job对象提交
        job.waitForCompletion(true);
        //System.out.println("end");
        System.out.println("ans1="+ans1);
        if(ans1>=1||cnt1>cnt2) System.out.println("好评");
        else System.out.println("差评");
    }
    private static void init() throws IOException, ClassNotFoundException, InterruptedException {
        //ArrayList<String> list=readFromTextFile("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\test-1000.txt");
        ArrayList<String> list=readFromTextFile("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\new.txt");
        //System.out.println(list.size());
        for(String str : list){
            String []array=str.split("\t");
            System.out.println(array);
            //String bug="住 过 几次 东莞 酒店 海悦 地理位置 早餐 最棒 听说 朋友 说 请来 厨师 来头 呵呵 冲 这个 去";
            String []arr=array[1].split(" ");
            map.clear();
            ans1=ans2=1;
            cnt1=cnt2=0;
            for(String i :arr){
                map.put(i,0);
            }
            solve();
        }
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

}