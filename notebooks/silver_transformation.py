#!/usr/bin/env python
# coding: utf-8

# ## silver_transformation
# 
# New notebook

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode

spark = SparkSession.builder.getOrCreate()

# Bronze'dan oku
df_weather_raw = spark.read.option("multiline", "true").json(
    "Files/bronze/weather_raw.json"
)

df_weather_raw.printSchema()
df_weather_raw.show(5)


# In[ ]:


from pyspark.sql.functions import arrays_zip, explode, col, to_timestamp

# hourly array'lerini zip'le ve explode et
df_weather = df_weather_raw.select(
    explode(
        arrays_zip(
            col("hourly.time"),
            col("hourly.temperature_2m"),
            col("hourly.wind_speed_10m"),
            col("hourly.direct_radiation")
        )
    ).alias("hourly_data")
).select(
    to_timestamp(col("hourly_data.time"), "yyyy-MM-dd'T'HH:mm").alias("datetime"),
    col("hourly_data.temperature_2m").alias("temperature_celsius"),
    col("hourly_data.wind_speed_10m").alias("wind_speed_kmh"),
    col("hourly_data.direct_radiation").alias("radiation_wm2")
)

df_weather.printSchema()
df_weather.show(5)


# In[3]:


# Silver'a kaydet
df_weather.write.mode("overwrite").parquet(
    "Files/silver/weather_clean"
)

print("weather_clean kaydedildi!")


# In[20]:


df_air = df_air_raw.select(
    explode(
        arrays_zip(
            col("hourly.time"),
            col("hourly.carbon_monoxide"),
            col("hourly.nitrogen_dioxide"),
            col("hourly.pm10")
        )
    ).alias("hourly_data")
).select(
    to_timestamp(col("hourly_data.time"), "yyyy-MM-dd'T'HH:mm").alias("datetime"),
    col("hourly_data.carbon_monoxide").alias("carbon_monoxide_ugm3"),
    col("hourly_data.nitrogen_dioxide").alias("nitrogen_dioxide_ugm3"),
    col("hourly_data.pm10").alias("pm10_ugm3")
)

df_air.show(5)


# In[21]:


df_air.write.mode("overwrite").parquet(
    "Files/silver/airquality_clean"
)

print("airquality_clean kaydedildi!")


# In[22]:


from pyspark.sql.functions import from_json, schema_of_json

df_energy_raw = spark.read.option("multiline", "true").json(
    "Files/bronze/energy_raw.json"
)

df_energy_raw.printSchema()


# In[33]:


df_energy = df_energy_raw.select(
    explode(col("Prices")).alias("price_data")
).select(
    to_timestamp(col("price_data.readingDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("datetime"),
    col("price_data.price").alias("price_eur_kwh")
)

df_energy.show(5)


# In[34]:


df_energy.write.mode("overwrite").parquet(
    "Files/silver/energy_clean"
)

print("energy_clean kaydedildi!")


# In[35]:


# Read Silver
df_w = spark.read.parquet("Files/silver/weather_clean")
df_a = spark.read.parquet("Files/silver/airquality_clean")
df_e = spark.read.parquet("Files/silver/energy_clean")

# JOIN
df_gold = df_w.join(df_a, on="datetime", how="inner") \
              .join(df_e, on="datetime", how="inner")

df_gold.show(5)


# In[37]:


df_gold.write.mode("overwrite").parquet(
    "Files/gold/smart_city_analysis"
)

print("smart_city_analysis saved!")


# In[38]:


df_gold.write.mode("overwrite").saveAsTable("smart_city_analysis")

