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

def mount_adls(blobContainerName):
    try:
      dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = f"/mnt/{storageAccountName}/{blobContainerName}",
        #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
        extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
      )
      print("OK!")
    except Exception as e:
      print("Falha", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Montando todos os containers

# COMMAND ----------

mount_adls('landing-zone')
mount_adls('bronze')
mount_adls('silver')
mount_adls('gold')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mostrando os pontos de montagem no cluster Databricks

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrando todos os arquivos da camada landing-zone

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gerando um dataframe para cada arquivo a partir dos arquivos CSV gravado no container landing-zone do Azure Data Lake Storage

# COMMAND ----------

df_funcionarios = spark.read.option("infeschema", "true").option("header", "true").csv(f"/mnt/{storageAccountName}/landing-zone/Funcionarios/FileFuncionarios.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Adicionando metadados de data e hora de processamento e nome do arquivo de origem

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df_funcionarios = df_funcionarios.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("funcionarios.csv"))



# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando os dataframes em delta lake (formato de arquivo) no data lake (repositorio cloud)

# COMMAND ----------

df_funcionarios.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Funcionarios")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Verificando os dados gravados em delta na camada bronze

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/bronze/Funcionarios"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo um exemplo de um delta lake para validar a existencia dos dados e das colunas do metadados

# COMMAND ----------

spark.read.format('delta').load(f'/mnt/{storageAccountName}/bronze/Funcionarios').limit(10).display()
