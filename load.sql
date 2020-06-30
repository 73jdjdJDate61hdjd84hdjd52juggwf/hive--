load data local inpath '${hiveconf:log_dir}/${hiveconf:file_path}' into table load_hive.load_tb partition (date='${hiveconf:DAY}',hour='${hiveconf:HOUR}')
