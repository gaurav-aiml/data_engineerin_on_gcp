#!/bin/bash

#eval command
bucket="gs://gmp-etl"
pwd_file="gs://gmp-etl/pwd/pwd.txt"
cluster_name="sqoop-cluster-ephemeral"

table_name="flights"
target_dir=$bucket/sqoop_output1
inc_update_dir="/incremental_updates"


if [ $1 = '1' ]; then
    #----------------------------- EVAL Command to run queries -----------------------------------------
    gcloud dataproc jobs submit hadoop \
    --cluster $cluster_name \
    --region "us-central1" \
    --class "org.apache.sqoop.Sqoop" \
    --jars $bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-5.1.49.jar,$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar \
    -- eval \
    -Dmapreduce.job.user.classpath.first=true \
    --driver com.mysql.jdbc.Driver \
    --connect "jdbc:mysql://localhost:3307/airports" \
    --username root \
    --password-file $pwd_file \
    --query "select flight_date, count(*) from flights group by flight_date"

elif [ $1 = '2' ]; then
    ##----------------------------- Import Command to import data from SQL to S3 bucket as .TXT Files -----------------------------------------
    gcloud dataproc jobs submit hadoop \
    --cluster $cluster_name \
    --region "us-central1" \
    --class "org.apache.sqoop.Sqoop" \
    --jars $bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-5.1.49.jar,$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar \
    -- import \
    -Dmapreduce.job.user.classpath.first=true \
    --driver com.mysql.jdbc.Driver \
    --connect "jdbc:mysql://localhost:3307/airports" \
    --username root \
    --password-file $pwd_file \
    --split-by id \
    --table $table_name \
    -m 4 \
    --target-dir $target_dir


elif [ $1 = '3' ]; then
    ##----------------------------- Import Command to import data from SQL to S3 bucket as AVRO Files -----------------------------------------
    gcloud dataproc jobs submit hadoop \
    --cluster $cluster_name \
    --region "us-central1" \
    --class "org.apache.sqoop.Sqoop" \
    --jars $bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-5.1.49.jar,$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar \
    -- import \
    -Dmapreduce.job.classloader=true \
    --driver com.mysql.jdbc.Driver \
    --connect "jdbc:mysql://localhost:3307/airports" \
    --username root \
    --password-file $pwd_file \
    --split-by id \
    --table $table_name \
    -m 4 \
    --warehouse-dir $target_dir --as-avrodatafile

elif [ $1 = '4' ]; then
    gcloud dataproc jobs submit hadoop \
        --cluster $cluster_name \
        --region "us-central1" \
        --class "org.apache.sqoop.Sqoop" \
        --jars $bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-5.1.49.jar,$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar \
        -- import \
        -Dmapreduce.job.classloader=true \
        --driver com.mysql.jdbc.Driver \
        --connect "jdbc:mysql://localhost:3307/airports" \
        --username root \
        --password-file $pwd_file \
        --split-by id \
        --table $table_name \
        --check-column flight_date \
        --last-value 2019-05-13 \
        --incremental append \
        -m 4 \
        --warehouse-dir $inc_update_dir --as-avrodatafile &&
    
    gcloud compute ssh $cluster_name-m \
    --zone us-central1-a \
    -- -T 'hadoop distcp /incremental_updates/'$table_name"/*.avro gs://gmp-etl/incremental_updates"

else
    echo "Please Specify type of command"
    echo "1. eval" 
    echo "2. simple-import-as-text"
    echo "3. simple-import-as-avro" 
    echo "4. inc-import" 

fi


