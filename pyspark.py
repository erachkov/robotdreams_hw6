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


# In[5]:


category_df = spark.read.jdbc(pg_url, 'public.category', properties=pg_properties)


# In[6]:


film_category_df = spark.read.jdbc(pg_url, 'public.film_category', properties=pg_properties)


# In[50]:


actor_df = spark.read.jdbc(pg_url, 'public.actor', properties=pg_properties)


# In[51]:


film_actor_df = spark.read.jdbc(pg_url, 'public.film_actor', properties=pg_properties)


# In[52]:


inventory_df = spark.read.jdbc(pg_url, 'public.inventory', properties=pg_properties)


# In[53]:


rental_df = spark.read.jdbc(pg_url, 'public.rental', properties=pg_properties)


# In[ ]:





# In[7]:


# 1    вывести количество фильмов в каждой категории, отсортировать по убыванию.
#select cat.name, count(*) count_films
#from category cat
#join film_category fcat ON fcat.category_id = cat.category_id
#join film f ON f.film_id = fcat.film_id
#group by cat.name
#order by 2 desc;


# In[ ]:





# In[11]:


stg_film_category_df = film_category_df.join(film_df, film_category_df.film_id == film_df.film_id, 'inner'  ).select(film_category_df.category_id)


# In[12]:


core_film_category_df = stg_film_category_df.join(category_df, stg_film_category_df.category_id == category_df.category_id, 'inner').select(category_df.name)


# In[48]:


final_df = core_film_category_df.groupBy('name').agg(F.count("name").alias("count_df")).orderBy(F.col("count_df").desc())


# In[49]:


final_df.show()


# In[ ]:


#-- 2   вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

#select * from (
#    select a.first_name, a.last_name, count(*) rent_quantity
#    from
#      actor a
#      join film_actor fa ON a.actor_id = fa.actor_id
#      join c f ON f.film_id = fa.film_id
#      join inventory i ON i.film_id = f.film_id
#      join rental r on r.inventory_id = i.inventory_id
#    group by a.first_name, a.last_name
#    order by 3 desc
#    ) sub limit 10;


# In[58]:


stg_film_actor_df = film_actor_df.join(actor_df,actor_df.actor_id == film_actor_df.actor_id, 'inner' ).select(actor_df.first_name,actor_df.last_name, film_actor_df.film_id)


# In[60]:


stg_film_actor_df = stg_film_actor_df.join(film_df, film_df.film_id == stg_film_actor_df.film_id, 'inner').select(stg_film_actor_df.first_name,stg_film_actor_df.last_name, film_df.film_id)


# In[62]:


stg_inventory_df = inventory_df.join(stg_film_actor_df, inventory_df.film_id == stg_film_actor_df.film_id, 'inner').select(stg_film_actor_df.first_name,stg_film_actor_df.last_name, stg_film_actor_df.film_id,inventory_df.inventory_id)


# In[65]:


stg_rental_df = rental_df.join(stg_inventory_df,stg_inventory_df.inventory_id == rental_df.inventory_id, 'inner').select(stg_inventory_df.first_name,stg_inventory_df.last_name, stg_inventory_df.film_id,rental_df.inventory_id)


# In[70]:


stg_rental_df.groupBy('first_name','last_name').agg(F.count("*").alias("rent_quantity")).orderBy(F.col("rent_quantity").desc()).show(10)

