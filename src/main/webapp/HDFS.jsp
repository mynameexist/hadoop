<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>HDFS</title>
    <meta name="renderer" content="webkit">
    <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1">
    <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
    <script src="js/jquery.js"></script>
    <script type="text/javascript" src="layui/layui.js"></script>
    <link rel="stylesheet" href="layui/css/layui.css" type="text/css"/>
    <script src="http://code.highcharts.com/highcharts.js"></script>
    <script src="http://code.highcharts.com/highcharts-3d.js"></script>
</head>
<body>
<div class="layui-col-md12" >
    <div class="layui-row grid-demo grid-demo-bg1">
        <fieldset class="layui-elem-field layui-field-title site-demo-button" style="margin-top: 30px;">
            <legend >HDFS</legend>
        </fieldset>
        <div class="layui-upload" style="margin-left: 20px">
            <button type="button" class="layui-btn" id="upload">上传文件</button>
            <span id="wait"></span>
            <button type="button" class="layui-btn layui-btn-normal" onclick="predictAll()">开始预测</button>
        </div>
        <form class="layui-form" action="" style="margin-top: 30px;" name="form" >
            <div class="layui-form-item" >
                <div class="layui-input-block layui-input-inline" style="margin-left: 20px;">
                    <input type="text" placeholder="请输入需预测的内容" id="content" class="layui-input" style="width: 500px;">
                </div>
                <div class="layui-input-block layui-input-inline" style="margin-left: 315px">
                    <button type="button" class="layui-btn layui-btn-normal" id="prerdit2" onclick="single()">开始预测</button>
                </div>
            </div>
        </form>
        <fieldset class="layui-elem-field layui-field-title site-demo-button" style="margin-top: 30px;">
            <legend ><b>预测结果</b></legend>
        </fieldset>
        <div class="layui-container">
            <div class="layui-row">
                <div class="layui-col-xs6">
                    <div class="grid-demo grid-demo-bg1">
                        <div class="layui-input-block" style="margin-left: 20px;">
                            <p id="prate"></p><br>
                            <textarea  class="layui-textarea"  id="res" readonly style="width: 550px;height: 300px"></textarea>
                            <a class="layui-btn layui-btn-normal" id="copy" style="float: right;margin: 5px auto;">复制</a>
                            <textarea  class="layui-textarea"  id="err" readonly style="width: 550px;height: 300px;margin-top: 10px;"></textarea>
                        </div>
                    </div>
                </div>
                <div class="layui-col-xs6">
                    <div class="grid-demo">
                        <div id="container" style="width: 550px; height: 400px; margin: 0 auto"></div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<script language="JavaScript">
    function predictAll(){
        $("#res").html("正在预测，等待时间可能较长，请稍后！")
        layui.use('layer', function () {
            var layer = layui.layer;
            layer.alert("<h2 style='color:black'>" + '正在预测，等待时间可能较长，请稍后！' + "</h2>", {title: "Tips"});
        });
        $.post("xunlian.action",function(res){
            console.log(res)
            var pstr = "";
            for(let i=0; i<res.list.length-1; i++){
                pstr += res.list[i].id +"   "+res.list[i].type+"\n";
            }
            $("#res").html(pstr)
            $("#prate").html("预测正确率："+res.list[res.list.length-1].type*100+"%")
            let error = "预测错误列表：\n"
            for(let i=0; i<res.errorList.length; i++){
                error += res.errorList[i]+"\n";
            }
            $("#err").html(error)
            $(document).ready(function() {
                var chart = {
                    type: 'pie',
                    options3d: {
                        enabled: true,
                        alpha: 45,
                        beta: 0
                    }
                };
                var title = {
                    text: '预测占比图'
                };
                var tooltip = {
                    pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
                };
                var plotOptions = {
                    pie: {
                        allowPointSelect: true,
                        cursor: 'pointer',
                        depth: 35,
                        dataLabels: {
                            enabled: true,
                            format: '{point.name}'
                        }
                    }
                };
                var series= [{
                    type: 'pie',
                    name: 'rate',
                    data: [
                        ['差评数', res.cnum],
                        {
                            name: '好评数',
                            y: res.hnum,
                            sliced: true,
                            selected: true
                        },
                    ]
                }];

                var json = {};
                json.chart = chart;
                json.title = title;
                json.tooltip = tooltip;
                json.plotOptions = plotOptions;
                json.series = series;
                $('#container').highcharts(json);
            });
        })
    }

</script>

<script type="text/javascript">
    layui.use('upload', function(){
        var $ = layui.jquery
        ,upload = layui.upload;

        upload.render({
            elem: '#upload'
            ,url: 'upload.action'
            ,accept: 'file'
            ,async:true
            ,method:"post"
            ,choose: function(obj){
                $("#wait").html("正在上传，请稍候！")
                layui.use('layer', function(){
                    var layer = layui.layer;
                    layer.alert("<h2 style='color:black'>" + '正在上传！' + "</h2>", {title: "Tips"});
                });
            }
            ,done: function(res){
                console.log(res)
                if(res.id ==="1"){
                    layui.use('layer', function(){
                        $("#wait").html("上传成功！")
                        var layer = layui.layer;
                        layer.alert("<h2 style='color:black'>" + '上传成功！' + "</h2>", {title: "Tips"});
                    });
                }else{
                    layui.use('layer', function () {
                        var layer = layui.layer;
                        layer.alert("<h2 style='color:black'>" + '上传失败！' + "</h2>", {title: "Tips"});
                    });
                }
            }
        })
    })

    function single() {
        $("#res").html("请稍后...")
        layui.use('layer', function () {
            var layer = layui.layer;
            layer.alert("<h2 style='color:black'>" + '请稍后...' + "</h2>", {title: "Tips"});
        });
        $.post("test.action",{msg:$("#content").val()},function(res){
            console.log(res)
            $("#res").html(res.type+" !")
        })
    }



    $(function () {
        const btn = $("#copy");
        btn.click( () => {
            const input = $("#res");
            input.select();
            if (document.execCommand('copy')) {
                document.execCommand('copy');
                alert('复制成功');
            }
        })
    })


</script>
</body>
</html>