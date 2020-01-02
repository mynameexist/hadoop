package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//自定义的Reducer类必须继承Reducer，并重写reduce方法实现自己的逻辑，泛型参数分别为输入的键类型，值类型；输出的键类型，值类型；之后的reduce类似
public class OReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    //处理每一个键值对都会调用一次reduce方法，有多少个键值对就调用多少次
    protected void reduce(
            IntWritable key,
            Iterable<Text> value,
            org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text>.Context context)
            throws java.io.IOException, InterruptedException {
        Text sum = null;
        //循环value，将其中的值相加，得到总次数
        for (Text v : value) {
            sum = v;
        }
        
        //context.write输入新的键值对（结果）
        context.write(key, sum);
    };
}