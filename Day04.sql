--需求：每日各时段的pv uv 平均pv
--实现：
	--创建外部表，原始日志需要保留
	--创建二级分区表 一级为天，二级为小时

--1.创建数据库
create database db_track;

--2.创建表
create external table tracklog(
id              string,
url             string,
referer         string,
keyword         string,
type            string,
guid            string,
pageId          string,
moduleId        string,
linkId          string,
attachedInfo    string,
sessionId       string,
trackerU        string,
trackerType     string,
ip              string,
trackerSrc      string,
cookie          string,
orderCode       string,
trackTime       string,
endUserId       string,
firstLink       string,
sessionViewNo   string,
productId       string,
curMerchantId   string,
provinceId      string,
cityId          string,
fee             string,
edmActivity     string,
edmEmail        string,
edmJobId        string,
ieVersion       string,
platform        string,
internalKeyword string,
resultSum       string,
currentPage     string,
linkPosition    string,
buttonPosition  string
)
partitioned by(date string,hour string)
row format delimited fields terminated by '\t'
location '/hive/external/tracklog';

--3.加载数据
load data local inpath '/home/hadoop/tracklogs/20150828/2015082818' overwrite into table tracklog partition(date="20150828",hour="18");


 dfs -mkdir -p  /hive/external/tracklog/20150828/19;
 
 dfs -put /home/hadoop/tracklogs/20150828/2015082819 /hive/external/tracklog/20150828/19;
 
alter table tracklog add partition(date="20150828",hour="19") location '/hive/external/tracklog/20150828/19';

--4.查看分区信息
show partitions tracklog;                                    

date=20150828/hour=18
date=20150828/hour=19

--5.验证数据
select * from tracklog where hour='18' limit 5;
select * from tracklog where hour like 19 limit 5;

--6.统计分析，实现需求
	--创建结果表 -》 sqoop 导出到Mysql -》 可视化展示
create table result(
date string,
hour string,
pv int,
uv int,
avgpv float
)
row format delimited fields terminated by ',';

insert overwrite table result select date,hour,count(url) pv, count(distinct guid) uv, count(url)/count(distinct guid) avgpv from tracklog group by date,hour;

--7.导出数据到MySQL
create table resultFromHive(
date varchar(8),
hour varchar(2),
pv int,
uv int,
avgpv decimal(3,2)
);




export
--connect
jdbc:mysql://bigdata0707:3306/testdb
--username
root
--password-file
file:///home/hadoop/mysql.pwd
--table
resultFromHive
-m
1
--export-dir
/user/hive/warehouse/db_track.db/result/
--fields-terminated-by 
','

--8.实现作业自动化


------------------------
case when then

select empid,empname,job,salary, case job when  'PRESIDENT' then salary+1000 when 'MANAGER' then salary+800 else salary+400 end from emp;


----sort by
insert overwrite  local directory '/home/hadoop/hive/export1' row format delimited fields terminated by ',' select * from temperature sort by year;

2013 34.0
2015 19.9
2015 31.0
2015 15.9
2015 33.0
2015 32.0

2008 17.0
2008 31.5
2008 21.0
2015 27.0
2016 32.0
2016 23.0

2008 32.0
2016 39.9

---distribute by 
insert overwrite  local directory '/home/hadoop/hive/export2' row format delimited fields terminated by ',' select * from temperature distribute by year ;

2016,32.0
2016,39.9
2016,23.0
2013,34.0

2008,32.0
2008,17.0
2008,31.5
2008,21.0

2015,32.0
2015,27.0
2015,33.0
2015,31.0
2015,15.9
2015,19.9

----distribute by col1 sort by col2
insert overwrite  local directory '/home/hadoop/hive/export2' row format delimited fields terminated by ',' select * from temperature distribute by year sort by temper;

2016,23.0
2016,32.0
2013,34.0
2016,39.9

2008,17.0
2008,21.0
2008,31.5
2008,32.0

2015,15.9
2015,19.9
2015,27.0
2015,31.0
2015,32.0
2015,33.0

---distribute by col sort by col
insert overwrite  local directory '/home/hadoop/hive/export3' row format delimited fields terminated by ',' select * from temperature distribute by year sort by year;

2013,34.0    
2016,32.0
2016,39.9
2016,23.0   

----
2013,34.0
2016,32.0
2016,39.9
2016,23.0

2008,32.0
2008,17.0
2008,31.5
2008,21.0
---
2008,32.0
2008,17.0
2008,31.5
2008,21.0

2015,32.0
2015,27.0
2015,33.0
2015,31.0
2015,15.9
2015,19.9
---
2015,32.0
2015,27.0
2015,33.0
2015,31.0
2015,15.9
2015,19.9


---cluster by col = distribute by col sort by col
insert overwrite  local directory '/home/hadoop/hive/export4' row format delimited fields terminated by ',' select * from temperature cluster by year;


create table dyemp(
empId int,
empName string,
job string,
manId int,
hireDate string,
salary float,
boners float
)
partitioned by(deptId int)
row format delimited fields terminated by '\t';

insert overwrite table dyemp partition(deptId) select empId,empName,job,manId,hireDate,salary,boners,deptId as deptId from emp;

create table dyemp2(
empId int,
empName string,
job string,
manId int,
salary float,
boners float,
deptId int
)
partitioned by(addr string,hireDate string)
row format delimited fields terminated by '\t';

insert overwrite table dyemp2 partition(addr="shanghai",hireDate) select empId,empName,job,manId,salary,boners,deptId, substr(hireDate,1,4) as hireDate from emp;


create table bucketemp(
empId int,
empName string,
job string,
manId int,
hireDate string,
salary float,
boners float,
deptId int
)
clustered by(hireDate) into 3 buckets
row format delimited fields terminated by '\t';

insert overwrite table bucketemp select empId,empName,job,manId,substr(hireDate,1,4) as hireDate,salary,boners,deptId from emp;

--分组排序
select empId,empName,job,salary, deptId,row_number() over(partition by deptId order by salary desc) rank from emp;

select empId,empName,job,salary, deptId, rank() over(partition by deptId order by salary desc) rank from emp;

select empId,empName,job,salary, deptId, dense_rank() over(partition by deptId order by salary desc) rank from emp;

select * from (select empId,empName,job,salary, deptId,row_number() over(partition by deptId order by salary desc) rank from emp) a where a.rank <= 3;
