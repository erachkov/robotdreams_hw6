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


# In[7]:


actor_df = spark.read.jdbc(pg_url, 'public.actor', properties=pg_properties)


# In[8]:


film_actor_df = spark.read.jdbc(pg_url, 'public.film_actor', properties=pg_properties)


# In[9]:


inventory_df = spark.read.jdbc(pg_url, 'public.inventory', properties=pg_properties)


# In[10]:


rental_df = spark.read.jdbc(pg_url, 'public.rental', properties=pg_properties)


# In[11]:


payment_df = spark.read.jdbc(pg_url, 'public.payment', properties=pg_properties)


# In[12]:


# 1    вывести количество фильмов в каждой категории, отсортировать по убыванию.
#select cat.name, count(*) count_films
#from category cat
#join film_category fcat ON fcat.category_id = cat.category_id
#join film f ON f.film_id = fcat.film_id
#group by cat.name
#order by 2 desc;


# In[ ]:





# In[13]:


stg_film_category_df = film_category_df.join(film_df, film_category_df.film_id == film_df.film_id, 'inner'  ).select(film_category_df.category_id)


# In[14]:


core_film_category_df = stg_film_category_df.join(category_df, stg_film_category_df.category_id == category_df.category_id, 'inner').select(category_df.name)


# In[15]:


final_df = core_film_category_df.groupBy('name').agg(F.count("name").alias("count_df")).orderBy(F.col("count_df").desc())


# In[16]:


final_df.show()


# In[17]:


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


# In[18]:


stg_film_actor_df = film_actor_df.join(actor_df,actor_df.actor_id == film_actor_df.actor_id, 'inner' ).select(actor_df.first_name,actor_df.last_name, film_actor_df.film_id)


# In[19]:


stg_film_actor_df = stg_film_actor_df.join(film_df, film_df.film_id == stg_film_actor_df.film_id, 'inner').select(stg_film_actor_df.first_name,stg_film_actor_df.last_name, film_df.film_id)


# In[20]:


stg_inventory_df = inventory_df.join(stg_film_actor_df, inventory_df.film_id == stg_film_actor_df.film_id, 'inner').select(stg_film_actor_df.first_name,stg_film_actor_df.last_name, stg_film_actor_df.film_id,inventory_df.inventory_id)


# In[21]:


stg_rental_df = rental_df.join(stg_inventory_df,stg_inventory_df.inventory_id == rental_df.inventory_id, 'inner').select(stg_inventory_df.first_name,stg_inventory_df.last_name, stg_inventory_df.film_id,rental_df.inventory_id)


# In[22]:


stg_rental_df.groupBy('first_name','last_name').agg(F.count("*").alias("rent_quantity")).orderBy(F.col("rent_quantity").desc()).show(10)


# In[23]:


#-- 3    вывести категорию фильмов, на которую потратили больше всего денег.

#select cat.name,
#       sum(p.amount) film_amount
#from category cat
#join film_category fcat ON fcat.category_id = cat.category_id
#join film f ON f.film_id = fcat.film_id
#join inventory i ON i.film_id = f.film_id
#join rental r on r.inventory_id = i.inventory_id
#join payment p on p.rental_id = r.rental_id
#group by cat.name
#order by 2 desc;


# In[24]:


stg_film_category_df = film_category_df.join(film_df, film_category_df.film_id == film_df.film_id, 'inner'  ).select(film_category_df.category_id,film_category_df.film_id)


# In[25]:


core_film_category_df = stg_film_category_df.join(category_df, stg_film_category_df.category_id == category_df.category_id, 'inner').select(stg_film_category_df.film_id, category_df.name)


# In[26]:


stg_inventory_df = inventory_df.join(core_film_category_df, inventory_df.film_id == core_film_category_df.film_id, 'inner').select(core_film_category_df.name,core_film_category_df.film_id,inventory_df.inventory_id)


# In[27]:


stg_rental_df = rental_df.join(stg_inventory_df,stg_inventory_df.inventory_id == rental_df.inventory_id, 'inner').select(stg_inventory_df.name,stg_inventory_df.film_id,stg_inventory_df.inventory_id,rental_df.rental_id)


# In[28]:


stg_payment_df = stg_rental_df.join(payment_df, payment_df.rental_id == stg_rental_df.rental_id, 'inner').select(stg_rental_df.name,stg_rental_df.film_id, payment_df.amount)


# In[29]:


final_df = stg_payment_df.groupBy('name').agg(F.sum("amount").alias("film_amount")).orderBy(F.col("film_amount").desc())


# In[30]:


final_df.show()


# In[31]:


#--  4   вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

#select f.title from
#    film f
#where not exists
#    (select 1 from inventory i where i.film_id = f.film_id) order by  f.title;


# In[32]:


final_df = film_df.join(inventory_df, film_df.film_id == inventory_df.film_id, 'left_anti').select(film_df.title).orderBy(F.col('title'))


# In[33]:


final_df.show()


# In[34]:


#-- 5    вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех


# In[47]:


query = """select * from (
select top.first_name, top.last_name, top.film_count from (
select a.first_name, a.last_name,  count(*) film_count
    from
      public.actor a
      join public.film_actor fa ON a.actor_id = fa.actor_id
      join public.film f ON f.film_id = fa.film_id
      join public.film_category fcat on f.film_id = fcat.film_id
      join public.category cat ON fcat.category_id = cat.category_id
where
     cat.name = 'Children'
group by a.first_name, a.last_name
order by  3 desc) top limit 3) top3
union all
--Если у нескольких актеров одинаковое кол-во фильмов, вывести всех
select top.first_name, top.last_name, top.film_count from (
select a.first_name, a.last_name,  count(*) film_count
    from
      public.actor a
      join public.film_actor fa ON a.actor_id = fa.actor_id
      join public.film f ON f.film_id = fa.film_id
      join public.film_category fcat on f.film_id = fcat.film_id
      join public.category cat ON fcat.category_id = cat.category_id
where
     cat.name = 'Children'
group by a.first_name, a.last_name
order by  3 desc) top where top.film_count in (
select sub.film_count from (
select a.first_name, a.last_name,  count(*) film_count
    from
      public.actor a
      join public.film_actor fa ON a.actor_id = fa.actor_id
      join public.film f ON f.film_id = fa.film_id
      join public.film_category fcat on f.film_id = fcat.film_id
      join public.category cat ON fcat.category_id = cat.category_id
where
     cat.name = 'Children'
group by a.first_name, a.last_name
order by  3 desc) sub group by sub.film_count having  count(film_count) >1)"""


# In[49]:


my_result = spark.read.jdbc(url=pg_url, table=f'({query}) as my_result', properties= pg_properties)


# In[50]:


my_result.show()


# In[ ]:


#--  6  вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.

#SELECT c.city,
#       cy.country,
#       cust.active,
#       count(*)
#FROM customer cust
#         JOIN rental r ON r.customer_id = cust.customer_id
#         JOIN inventory i ON r.inventory_id = i.inventory_id
#         JOIN store s ON i.store_id = s.store_id
#         JOIN address a ON s.address_id = a.address_id
#         JOIN city c ON a.city_id = c.city_id
#         JOIN country cy ON c.country_id = cy.country_id
#GROUP BY cy.country, c.city, cust.active
#ORDER BY cust.active, cy.country, c.city;


# In[54]:


query = """SELECT c.city,
       cy.country,
       cust.active,
       count(*)
FROM customer cust
         JOIN rental r ON r.customer_id = cust.customer_id
         JOIN inventory i ON r.inventory_id = i.inventory_id
         JOIN store s ON i.store_id = s.store_id
         JOIN address a ON s.address_id = a.address_id
         JOIN city c ON a.city_id = c.city_id
         JOIN country cy ON c.country_id = cy.country_id
GROUP BY cy.country, c.city, cust.active
ORDER BY cust.active, cy.country, c.city"""


# In[55]:


my_result = spark.read.jdbc(url=pg_url, table=f'({query}) as my_result', properties= pg_properties)


# In[56]:


my_result.show()


# In[ ]:


#-  7   вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.#

#select cat.name,
#       c.city,
#       sum(round(extract(epoch from (r.return_date) - (r.rental_date))/3600)) as sum_hours
#from category cat
#join film_category fcat ON fcat.category_id = cat.category_id
#join film f ON f.film_id = fcat.film_id
#join inventory i ON i.film_id = f.film_id
#join rental r on r.inventory_id = i.inventory_id
#join customer cust on r.customer_id = cust.customer_id
#JOIN address a ON cust.address_id = a.address_id
#JOIN city c ON a.city_id = c.city_id
#where city like 'a%' or city like '%-%'
#group by cat.name, c.city having sum(round(extract(epoch from (r.return_date) - (r.rental_date))/3600)) >0
#order by 3 desc;


# In[57]:


query = """select cat.name,
       c.city,
       sum(round(extract(epoch from (r.return_date) - (r.rental_date))/3600)) as sum_hours
from category cat
join film_category fcat ON fcat.category_id = cat.category_id
join film f ON f.film_id = fcat.film_id
join inventory i ON i.film_id = f.film_id
join rental r on r.inventory_id = i.inventory_id
join customer cust on r.customer_id = cust.customer_id
JOIN address a ON cust.address_id = a.address_id
JOIN city c ON a.city_id = c.city_id
where city like 'a%' or city like '%-%'
group by cat.name, c.city having sum(round(extract(epoch from (r.return_date) - (r.rental_date))/3600)) >0
order by 3 desc"""


# In[58]:


my_result = spark.read.jdbc(url=pg_url, table=f'({query}) as my_result', properties= pg_properties)


# In[59]:


my_result.show()

