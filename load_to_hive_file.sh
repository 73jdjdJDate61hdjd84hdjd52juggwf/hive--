#! /bin/bash

#定义昨天的日期时间
YESTERDAY=`date -d '-1 days' +%Y%m%d`

#定义数据目录
ACCESS_LOG_DIR=/opt/datas/access_logs/$YESTERDAY

#定义HIVE_HOME
HIVE_HOME=/opt/cdh5.3.6/hive-0.13.1-cdh5.3.6

#定义遍历目录下文件名称,获取日期和时间指定分区
for FILE in `ls $ACCESS_LOG_DIR`
do
    DAY=${FILE:0:8}
	HOUR=${FILE:8:2}
	#echo "${DAY}${HOUR}"
    $HIVE_HOME/bin/hive --hiveconf log_dir=$ACCESS_LOG_DIR --hiveconf file_path=$FILE --hiveconf DAY=$DAY --hiveconf HOUR=$HOUR -f '/opt/datas/hive_shell/load.sql'
done
    $HIVE_HOME/bin/hive -e "show partitions load_hive.load_tb"









