package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//自定义的Mapper类必须继承Mapper类，并重写map方法实现自己的逻辑
public class OMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    //处理输入文件的每一行都会调用一次map方法，文件有多少行就会调用多少次
    protected void map(
            LongWritable key,
            Text value,
            org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        //key为每一行的起始偏移量
        //value为每一行的内容
        //每一行的内容分割，如hello   world，分割成一个String数组有两个数据，分别是hello，world
        //String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String line =value.toString();

        //String[] ss = value.toString().toString().split(" ");
        String str=line.split("\t")[1];
        String []arr=str.split(" ");
        double ans1=1;
        int cnt1=0,cnt2=0;
        for(String i :arr){
            String h="好评"+"_"+i;
            String c="差评"+"_"+i;
            if(OSubmit.map.containsKey(h)) {
                int sum=OSubmit.map.get(h);
                ans1*=sum;
                cnt1++;
            }
            if(OSubmit.map.containsKey(c)) {
                int sum=OSubmit.map.get(c);
                ans1/=sum;
                cnt2++;
            }

        }
        int now=++OSubmit.cnt1;
        System.out.print("ans1="+ans1+"  "+cnt1+"  "+cnt2+"   ");
        if(ans1>=1||cnt1>cnt2) {
            System.out.println(now+"好评");
        }
        else {
            System.out.println(now+ "差评");
        }
        if(ans1>=1||cnt1>cnt2) {
            context.write(new IntWritable(now),new Text("好评"));
        }
        else {
            context.write(new IntWritable(now),new Text("差评"));
        }
    }


    ;
}