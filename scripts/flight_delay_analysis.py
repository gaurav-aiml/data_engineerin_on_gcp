
# coding: utf-8

# In[16]:


import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from datetime import date


# In[2]:


# sc = SparkContext()
spark = SparkSession(sc)


# In[39]:


current_date = str(date.today())


# In[21]:


flight_data = spark.read.json("gs://gmp-etl/json/"+current_date+".json")


# In[22]:


flight_data.registerTempTable("flight_data") 


# In[23]:


base_query = """
             select
                 *
             from 
                 flight_data

             """


# In[24]:


spark.sql(base_query).show()


# In[25]:


query1 = """
         select
             airline_code,
             AVG(arrival_delay) as avg_arrival_delay,
             AVG(departure_delay) as avg_departure_delay
        from
            flight_data
        group by
            airline_code
         """


# In[26]:


query2 = """
         select 
             source_airport, 
             destination_airport,
             AVG(arrival_delay) as avg_arrival_delay,
             AVG(departure_delay) as avg_departure_delay
        from 
             flight_data
        group by
            source_airport, destination_airport
        """


# In[27]:


query3 = """
         select 
             *,
             case 
                 when distance between 0 and 500 then 1
                 when distance between 501 and 1000 then 2
                 when distance between 1001 and 1500 then 3
                 when distance between 1501 and 2000 then 4
                 when distance between 2001 and 2500 then 5
                 when distance between 2501 and 3000 then 6
                 when distance between 3001 and 3500 then 7
                 when distance between 3501 and 4000 then 8
                 when distance between 4001 and 4500 then 9
                 when distance between 4501 and 5000 then 10
            end distance_category
        from 
            flight_data
                 
        
         """


# In[28]:


flight_delay_by_airline = spark.sql(query1)


# In[29]:


flight_delay_by_route = spark.sql(query2)


# In[30]:


flight_delay_dist_cat = spark.sql(query3)
flight_delay_dist_cat.registerTempTable("flight_delay_dist_cat")


# In[46]:


query4 = """
         select distance_category,
         AVG(arrival_delay) as avg_arrival_delay,
         AVG(departure_delay) as avg_departure_delay
         from flight_delay_dist_cat
         group by distance_category 
         order by distance_category
         """
flight_delay_distance_cat = spark.sql(query4)


# In[49]:


#Variables for path names
output_path = "gs://gmp-etl/flight_analysis_outputs/"+current_date
output_delay_by_airline = output_path+"_delay_by_airline"
output_delay_by_route = output_path+"_delay_by_route"
output_delay_by_dist_cat = output_path+"_delay_by_dist_cat"


# In[50]:


flight_delay_by_airline.coalesce(1).write.format("json").save(output_delay_by_airline)
flight_delay_by_route.coalesce(1).write.format("json").save(output_delay_by_route)
flight_delay_distance_cat.coalesce(1).write.format("json").save(output_delay_by_dist_cat)

