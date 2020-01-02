package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//自定义的Mapper类必须继承Mapper类，并重写map方法实现自己的逻辑
public class TMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //处理输入文件的每一行都会调用一次map方法，文件有多少行就会调用多少次
    protected void map(
            LongWritable key,
            Text value,
            org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws java.io.IOException, InterruptedException {
        //key为每一行的起始偏移量
        //value为每一行的内容
        //每一行的内容分割，如hello   world，分割成一个String数组有两个数据，分别是hello，world
        //String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String line =value.toString();
        //String[] ss = value.toString().toString().split(" ");
        String[] s=line.split("\t");
        //System.out.println(s[0]+" === "+s[1]);
        context.write(new Text(s[0]),new LongWritable(Integer.parseInt(s[1])));
    };
}