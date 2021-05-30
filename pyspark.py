#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# In[2]:


pg_url = "jdbc:postgresql://172.31.144.135:5432/pagila"
pg_properties = {"user": "pguser", "password": "secret", "driver": "org.postgresql.Driver"}


# In[3]:


spark = SparkSession.builder    .config('spark.driver.extraClassPath', '/mnt/opt/postgresql-42.2.20.jar')    .master('local')    .appName('SparkHW')    .getOrCreate()


# In[4]:


film_df = spark.read.jdbc(pg_url, 'public.film', properties=pg_properties)


# In[6]:


film_df.count()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





