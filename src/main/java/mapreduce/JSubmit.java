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

public class JSubmit {
    public static HashMap<String,String> a=new HashMap<>();

    public static void main(String[] args) throws IOException,
            URISyntaxException, InterruptedException, ClassNotFoundException {
        //init();
        //solve();
        hdfs();
    }
    public static  void hdfs() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(JSubmit.class);
        job.setMapperClass(JMapper.class);
        job.setReducerClass(JReducer.class);
        FileInputFormat.setInputPaths(job, new Path("hdfs://119.3.167.84:9000/input_2017081099/training-100000.txt"));
        Path outPath = new Path("hdfs://119.3.167.84:9000/input_2017081099/output100000");
        FileSystem fileSystem = outPath.getFileSystem(new Configuration());//上下不同点
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job, new Path("hdfs://119.3.167.84:9000/input_2017081099/output100000"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.waitForCompletion(true);
    }
    private static void solve() throws InterruptedException, IOException, ClassNotFoundException {
        //Path类为hadoop API定义，创建两个Path对象，一个输入文件的路径，一个输入结果的路径
        Path outPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\output");
        //Path outPath = new Path("hdfs://119.3.167.84:9000/input_2017081099/output");


        //输入文件的路径为本地linux系统的文件路径
        //Path inPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\test\\1.txt");
        Path inPath = new Path("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\training-1000.txt");
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
        job.setMapperClass(JMapper.class);
        //设置Mapper类处理完毕之后输出的键值对 的 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        //1.4排序，分组

        //1.5归约，这三步都有默认的设置，如果没有特殊的需求可以不管
        //2.1将数据传输到对应的Reducer

        //2.2使用自定义的Reducer类操作
        //设置Reducer类
        job.setReducerClass(JReducer.class);
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
    }
    private static void init() throws IOException {
        ArrayList<String> list=readFromTextFile("C:\\Users\\ASUS\\Desktop\\学习文件\\大三上\\hadoop\\output\\part-r-00000");
        for(String str : list){
            String []array=str.split("\t");
            //System.out.println(array.length);
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