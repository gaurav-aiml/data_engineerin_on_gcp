# bq mk -t \
# --schema schema.json \
# flight_analysis.flight_delays_np_2019-04-27 &&

# bq load --source_format=NEWLINE_DELIMITED_JSON \
# flight_analysis.flight_delays_np_2019-04-27 \
# gs://gmp-etl/json/2019-04-27.json &&

#----------------------------------------------------------
# Both commands above can be combined into \
# a single command by using the autodetect feature


# bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect \
# flight_analysis.flight_delays_np \
# gs://gmp-etl/json/2019-04-28.json


#----------------------------------------------------------
#In order to create a partitioning based on date, 
#another option called time-partition-field has to be provided

# bq load --source_format=NEWLINE_DELIMITED_JSON \
# --autodetect \
# --time_partitioning_field flight_date \
# flight_analysis.flight_delays \
# gs://gmp-etl/json/2019-04-28.json && 

# bq load --source_format=NEWLINE_DELIMITED_JSON \
# --autodetect \
# --time_partitioning_field flight_date \
# flight_analysis.flight_delays \
# gs://gmp-etl/json/2019-04-29.json 


#----------------------------------------------------------
# FINAL COMMAND TO GET ALL THE JSON FILES UP INTO THE DATASET

bq mk -t \
--schema schema.json \
--time_partitioning_field flight_date \
flight_analysis.flight_delays &&

bq load --source_format=NEWLINE_DELIMITED_JSON \
flight_analysis.flight_delays \
gs://gmp-etl/json/2019-04-27.json && 

bq load --source_format=NEWLINE_DELIMITED_JSON \
flight_analysis.flight_delays \
gs://gmp-etl/json/2019-04-28.json && 

bq load --source_format=NEWLINE_DELIMITED_JSON \
flight_analysis.flight_delays \
gs://gmp-etl/json/2019-04-29.json && 

bq load --source_format=NEWLINE_DELIMITED_JSON \
flight_analysis.flight_delays \
gs://gmp-etl/json/2019-04-30.json



