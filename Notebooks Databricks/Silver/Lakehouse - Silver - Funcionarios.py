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
# MAGIC ### Mostrando todos os arquivos da camada bronze

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gerando um dataframe dos delta lake no container bronze do Azure Data Lake Storage

# COMMAND ----------

df_funcionarios = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/Funcionarios")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Adicionando metadados de data e hora de processamento e nome do arquivo de origem

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df_funcionarios = df_funcionarios.withColumn("DATA_HORA_SILVER", current_timestamp()).withColumn("NOME_ARQUIVO", lit("funcionarios"))


# COMMAND ----------

# Obtenha todas as colunas do DataFrame
colunas = df_funcionarios.columns

# Converta todas as colunas para maiúsculas
colunas_maiusculas = [coluna.upper() for coluna in colunas]

# Imprima as colunas em maiúsculas
print("Colunas em maiúsculas:")
for coluna in colunas_maiusculas:
    print(coluna)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mudando as colunas para maiscula e ajustanto os nomes das colunas de acordo com o dicionario de dados

# COMMAND ----------

df_funcionarios = (df_funcionarios
                   .withColumnRenamed("id_funcionario", "CODIGO_FUNCIONARIO") 
                    .withColumnRenamed("nome", "NOME") 
                    .withColumnRenamed("sobrenome", "SOBRENOME") 
                    .withColumnRenamed("telefone", "TELEFONE") 
                    .withColumnRenamed("email", "EMAIL") 
                    .withColumnRenamed("cargo", "CARGO") 
                    .withColumnRenamed("data_contratacao", "DATA_CONTRATACAO"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Visualiza o dataframe atualizado para silver

# COMMAND ----------

df_funcionarios.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Removendo dados duplicados

# COMMAND ----------

df_funcionarios = df_funcionarios.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando os dataframes em delta lake (formato de arquivo) no data lake (repositorio cloud)

# COMMAND ----------

df_funcionarios.write.format('delta').save(f"/mnt/{storageAccountName}/silver/Funcionarios")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Verificando os dados gravados em delta na camada silver

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/silver/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo um exemplo de um delta lake para validar a existencia dos dados e das colunas do metadados

# COMMAND ----------

spark.read.format('delta').load(f'/mnt/{storageAccountName}/silver/Funcionarios').limit(10).display()
