<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>HDFS</title>
    <script src="js/jquery-3.3.1.min%20(1).js"></script>
    <script type="text/javascript" src="js/layui.js"></script>
    <link rel="stylesheet" href="js/layui.css" type="text/css"/>
</head>
<body>
<div class="layui-col-md12" >
    <div class="layui-row grid-demo grid-demo-bg1">
        <fieldset class="layui-elem-field layui-field-title site-demo-button" style="margin-top: 30px;">
            <legend >HDFS</legend>
        </fieldset>
        <div class="layui-upload" style="margin-left: 20px">
            <button type="button" class="layui-btn" id="upload"><i class="layui-icon"></i>上传文件</button>
        </div>
        <fieldset class="layui-elem-field layui-field-title site-demo-button" style="margin-top: 30px;">
            <legend >预测结果</legend>
        </fieldset>
        <div class="layui-form-item layui-form-text" style="width: 800px;">
            <label class="layui-form-label"></label>
            <div class="layui-input-block">
                <p id="crate">好评正确率：</p><br>
                <p id="wrate">差评正确率：</p><br>
                <textarea placeholder="请输入内容" class="layui-textarea"  id="res" readonly></textarea>
                <a  class="layui-btn layui-btn-normal" id="copy" style="float: right;margin-top: 5px;">复制</a>
            </div>
        </div>
    </div>
</div>
<script>
    $(document).ready(function () {
        var ele = layui.element;
        ele.render();
    });
    layui.use('form', function(){
        var form = layui.form;
        form.render();
    });
    layui.use('upload', function(){
        var $ = layui.jquery
            ,upload = layui.upload;

        upload.render({
            elem: '#upload'
            ,url: ''
            ,accept: 'file'
            ,async:true
            ,done: function(res){
                console.log(res)
                if(res.status ==="fail"){
                    layui.use('layer', function(){
                        var layer = layui.layer;
                        layer.alert("<h2 style='color:black'>" + res.data.errMsg + "</h2>",{title: "Tips"});
                    });
                }else if(res.status ==="success"){
                    layui.use('layer', function () {
                        var layer = layui.layer;
                        layer.alert("<h2 style='color:black'>" + '上传成功！' + "</h2>", {title: "Tips"});
                        setTimeout(function () {
                            //刷新
                            location.reload();
                        }, 1000);
                    });
                }
                console.log(res)  //返回对象数据
            }
        })
    })

    $("#copy").on("tap",function(){
        $("#res").select();
        try{
            if(document.execCommand("Copy",false,null)){
                //如果复制成功
                alert("复制成功！");
            }else{
                //如果复制失败
                alert("复制失败！");
            }
        }catch(err){
            //如果报错
            alert("复制错误！")
        }
    })

</script>
</body>
</html>