# Databricks notebook source
# MAGIC
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
# MAGIC ### Mostrando todos os arquivos da camada landing-zone

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gerando um dataframe para cada arquivo a partir dos arquivos CSV gravado no container landing-zone do Azure Data Lake Storage

# COMMAND ----------

df_cardapio = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Cardapio")
df_comanda = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Comanda")
df_estoque = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Estoque")
df_funcionarios = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Funcionarios")
df_ingredientes = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Ingredientes")
df_mesas = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Mesas")
df_pagamento = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Pagamento")
df_pedido = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Pedido")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Adicionando metadados de data e hora de processamento e nome do arquivo de origem

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df_cardapio = df_cardapio.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("cardapio.csv"))
df_comanda = df_comanda.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("comanda.csv"))
df_estoque = df_estoque.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("estoque.csv"))
df_funcionarios = df_funcionarios.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("funcionarios.csv"))
df_ingredientes = df_ingredientes.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("ingredientes.csv"))
df_mesas = df_mesas.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("mesas.csv"))
df_pagamento = df_pagamento.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("pagamento.csv"))
df_pedido = df_pedido.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("pedido.csv"))



# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando os dataframes em delta lake (formato de arquivo) no data lake (repositorio cloud)

# COMMAND ----------

df_cardapio.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Cardapio")
df_comanda.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Comanda")
df_estoque.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Estoque")
df_funcionarios.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Funcionarios")
df_ingredientes.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Ingredientes")
df_mesas.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Mesas")
df_pagamento.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Pagamento")
df_pedido.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Pedido")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Verificando os dados gravados em delta na camada bronze

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo um exemplo de um delta lake para validar a existencia dos dados e das colunas do metadados

# COMMAND ----------

spark.read.format('delta').load(f'/mnt/{storageAccountName}/bronze/Cardapio').limit(10).display()
