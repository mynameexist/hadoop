package mapreduce;

import java.util.ArrayList;
import java.util.List;

public class datalist {
    List<data> List=new ArrayList<>();
    List<String> errorList=new ArrayList<>();
    Integer hnum,cnum,num;
    String p;

    public void setP(String p) {
        this.p = p;
    }

    public String getP() {
        return p;
    }

    public Integer getHnum() {
        return hnum;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public void setHnum(Integer hnum) {
        this.hnum = hnum;
    }

    public void setCnum(Integer cnum) {
        this.cnum = cnum;
    }

    public Integer getCnum() {
        return cnum;
    }

    public void setList(java.util.List<data> list) {
        List = list;
    }

    public java.util.List<String> getErrorList() {
        return errorList;
    }

    public void setErrorList(java.util.List<String> errorList) {
        this.errorList = errorList;
    }

    public java.util.List<data> getList() {
        return List;
    }
}
