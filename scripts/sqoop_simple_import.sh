bucket="gs://gmp-etl"
pwd_file="gs://gmp-etl/pwd/pwd.txt"

table_name="flights"
target_dir=$bucket/sqoop_output
inc_update_dir="/incremental_update"


gcloud dataproc jobs submit hadoop \
    --cluster $1 \
    --region "us-central1" \
    --class "org.apache.sqoop.Sqoop" \
    --jars $bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-8.0.23.jar,$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar \
    -- import \
    -Dmapreduce.job.classloader=true \
    --driver com.mysql.jdbc.Driver \
    --connect "jdbc:mysql://localhost:3307/airports" \
    --username root \
    --password-file $pwd_file \
    --split-by id \
    --table $table_name \
    -m 4 \
    --target-dir $target_dir --as-avrodatafile
