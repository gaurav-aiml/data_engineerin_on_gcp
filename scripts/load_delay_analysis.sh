current_date=$(date +"%Y-%m-%d")

bq load --source_format=NEWLINE_DELIMITED_JSON \
--autodetect \
--time_partitioning_field flight_date \
flight_analysis.flight_delays_by_airline \
gs://gmp-etl/flight_analysis_outputs/${current_date}"_delay_by_airline/*.json"

bq load --source_format=NEWLINE_DELIMITED_JSON \
--autodetect \
--time_partitioning_field flight_date \
flight_analysis.flight_delays_by_route \
gs://gmp-etl/flight_analysis_outputs/${current_date}"_delay_by_route/*.json"


bq load --source_format=NEWLINE_DELIMITED_JSON \
--autodetect \
--time_partitioning_field flight_date \
flight_analysis.flight_delays_by_dist_cat \
gs://gmp-etl/flight_analysis_outputs/${current_date}"_delay_by_dist_cat/*.json"