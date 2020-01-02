package action;

import mapreduce.HdfsTest;
import mapreduce.JSubmit;
import mapreduce.OSubmit;
import org.apache.hadoop.fs.Hdfs;

import java.io.IOException;

public class xunlianAction {
    public String xunlian() throws InterruptedException, IOException, ClassNotFoundException {
        OSubmit.jisuan();
        return "success";
    }
}
