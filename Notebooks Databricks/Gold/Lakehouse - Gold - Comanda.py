# Databricks notebook source
# MAGIC %md
# MAGIC ##Validando a SparkSession

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##Conectando Azure ADLS Gen2 no Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mostrando os pontos de montagem no cluster Databricks

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definindo uma função para montar um ADLS com um ponto de montagem com ADLS SAS 

# COMMAND ----------

storageAccountName = ""
storageAccountAccessKey = ""
sasToken = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mostrando os pontos de montagem no cluster Databricks

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrando todos os arquivos da camada bronze

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gerando um dataframe dos delta lake no container bronze do Azure Data Lake Storage

# COMMAND ----------

df_comanda = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Comanda")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Adicionando metadados de data e hora de processamento e nome do arquivo de origem

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df_comanda = df_comanda.withColumn("DATA_HORA_GOLD", current_timestamp()).withColumn("NOME_ARQUIVO", lit("comanda"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Restante das transformações

# COMMAND ----------

# MAGIC %md
# MAGIC -------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando os dataframes em delta lake (formato de arquivo) no data lake (repositorio cloud)

# COMMAND ----------

df_comanda.write.format('delta').save(f"/mnt/{storageAccountName}/gold/Comanda")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Verificando os dados gravados em delta na camada gold

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/gold/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo um exemplo de um delta lake para validar a existencia dos dados e das colunas do metadados

# COMMAND ----------

spark.read.format('delta').load(f'/mnt/{storageAccountName}/gold/Comanda').limit(10).display()
