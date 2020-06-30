"27.38.5.159"	"-"	"31/Aug/2015:00:04:37 +0800"	"GET /course/view.php?id=27 HTTP/1.1"	"303"	"440"	-	"http://www.ibeifeng.com/user.php?act=mycourse"	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"	"-"	"learn.ibeifeng.com"

CREATE TABLE apachelog (
remote_addr string,
remote_user string,
time_local string,
request string,
status string,
body_bytes_set string,
request_body string,
http_referer string,
http_user_agent string,
http_x_forwarded_for string,
host string
 )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "(\"[^ ]*\") (\"-|[^ ]*\") (\"[^\]]*\") (\"[^\]]*\") (\"[0-9]*\") (\"[0-9]*\") (-|[^ ]*) (\"[^ ]*\") (\"[^\"]*\") (\"-|[^ ]*\") (\"[^ ]*\")"
)
STORED AS TEXTFILE;

--需求1：去除字段双引号
--1.创建自定义函数
add jar /home/hadoop/jars/RemoveQuote.jar;

create temporary function RemoveQuote as 'com.learn.hive.RemoveQuoteUdf';
--需求2：对时间格式转化
dfs -mkdir /hive/jars;
dfs -put /home/hadoop/jars/FormatTime.jar /hive/jars;
CREATE temporary FUNCTION FormatTime AS 'com.learn.hive.FormatTimeUDF' USING JAR 'hdfs://bigdata0707:8020/hive/jars/FormatTime.jar';


create table templog(
addr string,
time string,
request string,
status string,
host string
);

insert overwrite table templog select removequote(remote_addr),formattime(time_local),removequote(request),removequote(status),removequote(host) from apachelog;

select * from templog limit 10;





