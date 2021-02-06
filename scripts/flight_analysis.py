
# coding: utf-8

# In[4]:


import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession


# In[9]:


# sc = SparkContext()
spark = SparkSession(sc)


# In[10]:


flight_data = spark.read.json("gs://gmp-etl/json/2019-04-27.json")


# In[11]:


flight_data.registerTempTable("flight_data") 


# In[53]:


base_query = """
             select
                 *
             from 
                 flight_data

             """


# In[54]:


spark.sql(base_query).show()


# In[23]:


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


# In[27]:


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


# In[67]:


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


# In[64]:


flight_delay_by_airline = spark.sql(query1)


# In[65]:


flight_delay_by_route = spark.sql(query2)


# In[68]:


flight_delay_dist_cat = spark.sql(query3)
flight_delay_dist_cat.registerTempTable("flight_delay_dist_cat")


# In[70]:


query4 = """
         select distance_category,
         AVG(arrival_delay) as avg_arrival_delay,
         AVG(departure_delay) as avg_departure_delay
         from flight_delay_dist_cat
         group by distance_category 
         order by distance_category
         """
flight_delay_distance_cat = spark.sql(query4)

