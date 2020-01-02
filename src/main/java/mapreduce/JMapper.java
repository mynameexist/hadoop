package mapreduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//自定义的Mapper类必须继承Mapper类，并重写map方法实现自己的逻辑
public class JMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //处理输入文件的每一行都会调用一次map方法，文件有多少行就会调用多少次
    protected void map(
            LongWritable key,
            Text value,
            org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws java.io.IOException, InterruptedException {
        //key为每一行的起始偏移量
        //value为每一行的内容
        //System.out.println("test:"+JSubmit.a.get("1"));
        //每一行的内容分割，如hello   world，分割成一个String数组有两个数据，分别是hello，world
        //String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String line =value.toString();
        //System.out.println(line);
        //String[] ss = value.toString().toString().split(" ");
        String[] s=line.split("\t");
        if(s.length<2) return ;
        String[] ss = s[1].split(" ");
        //循环数组，将其中的每个数据当做输出的键，值为1，表示这个键出现一次
        for (int i=0;i<ss.length;i++) {
            if(check(ss[i])==false) {
                System.out.println(ss[i]);
                continue;
            }
            //context.write方法可以将map得到的键值对输出
            String k=s[0].concat("_").concat(ss[i]);
            context.write(new Text(k), new LongWritable(1));
        }
        context.write(new Text(s[0]),new LongWritable(1));
    };
    public boolean check(String name)
    {
        int n = 0;
        for(int i = 0; i < name.length(); i++) {
            n = (int)name.charAt(i);
            if(!(19968 <= n && n <40869)) {
                return false;
            }
        }
        return true;
    }
}