# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC -------------------------Farida Adamu
# MAGIC -------------------------19 January 2023

# COMMAND ----------

import pandas as pd
import requests
import json

# COMMAND ----------

#import csv file 

portfolio = spark.read.format("delta").load("dbfs:/user/hive/warehouse/client_portfolio_xlsx_ptf", header=True, inferSchema=True)


# COMMAND ----------

#convert csv column to list
tickers = portfolio.rdd.map(lambda x: x.ticker).collect()
#portfolio_list = list(map(lambda x: x[0], portfolio,list))


# COMMAND ----------

#confirm conversion
type(tickers)

# COMMAND ----------

print (tickers)


# COMMAND ----------

import requests

# API endpoint and API key
api_endpoint = "https://finnhub.io/api/v1/stock/profile2?symbol="
api_key = "cf3al0qad3i4fg34oak0cf3al0qad3i4fg34oakg"

# List to store the data
data_list = []

#symols we are interested in based on imported CSv or client's investment portfolio. 
symbols = ['NPK', 'FOXF', 'DAN', 'RIDE', 'OCFC', 'CIZN', 'TFC', 'HOPE', 'CCXI', 'NVAX', 'OCGN', 'TVTX', 'CERE', 'SLDB', 'ASMB', 'NAVB', 'RIGL', 'CARR', 'SSD', 'CNR', 'FUL', 'IPI', 'ARC', 'CECE', 'QUAD', 'BKTI', 'EXTR', 'IESC', 'TPX', 'EDUC', 'GHC', 'DAIO', 'OAS', 'LSBK', 'CNNE', 'SCU', 'NFBK', 'FOA', 'BG', 'CALM', 'ITGR', 'SOLY', 'SWAV', 'SEM', 'CZR', 'AESE', 'LVS', 'BRP', 'CINF', 'Y', 'MPX', 'MTD', 'QTRX', 'CVGI', 'CIR', 'GNK', 'FOXA', 'OMC', 'SLGG', 'CARG', 'MP', 'KALA', 'PTPI', 'TFFP', 'ETON', 'J', 'MG', 'KRG', 'NSA', 'INN', 'CLI', 'KW', 'ULTA', 'BURL', 'EXPR', 'BURL', 'EXPR', 'LOW', 'PI', 'DSPG', 'AAPL', 'ASAN', 'BSY', 'AAPL', 'ASAN', 'BSY', 'DUOT', 'BR', 'IBEX', 'PAYA', 'PFSW', 'SGC', 'VGR', 'DXPE', 'GIC', 'TRNS', 'ALTG', 'BECN', 'SO', 'WEC', 'AGR', 'OTTR', 'LNT', 'CDZI', 'TDW']

data_list = []

#run a for loop to iterate through both lists and extract the symbols of interest alongside their financial information
for symbol in symbols:
    url = api_endpoint + symbol + "&token=" + api_key
    response = requests.get(url)
    data = response.json()
    data_list.append(data)

print(data_list)




# COMMAND ----------

df = pd.DataFrame(data_list)
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC -- create a delta laske to store your data
# MAGIC -- Create your schema before creating your delta table, you should have 3 schema
# MAGIC -- Diamond = raw data | Pearl = Check type and transformation |Emerald = Aggregation to use directly in your Power BI 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA Diamond

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA Pearl

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA Emerald

# COMMAND ----------

# Convert the DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(pd.DataFrame(data_list))


# COMMAND ----------

# Write the data to a Delta table in the Diamond schema
spark_df.write.format("delta").save("/path/to/delta/table/Diamond")

# COMMAND ----------

spark.createDataFrame(data_list).createOrReplaceTempView("temp_table")

# COMMAND ----------

spark.sql("""CREATE TABLE Diamond.spark_df USING delta AS SELECT * FROM temp_table""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Diamond.spark_df
