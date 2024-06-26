# Databricks notebook source
# MAGIC %md
# MAGIC ##Validando a SparkSession

# COMMAND ----------

spark

# COMMAND ----------

Host = ""
Port = 1433
Database = "pingado-database"
Username = ""
Password = ""
Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
Url = f"jdbc:sqlserver://{Host}:{Port};databaseName={Database}"

# COMMAND ----------

df_cardapio = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "cardapio").option("user", Username).option("password", Password).load()
df_comanda = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "comanda").option("user", Username).option("password", Password).load() 
df_estoque = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "estoque").option("user", Username).option("password", Password).load() 
df_funcionarios = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "funcionarios").option("user", Username).option("password", Password).load() 
df_ingredientes = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "ingredientes").option("user", Username).option("password", Password).load()
df_mesas = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "mesas").option("user", Username).option("password", Password).load()
df_pagamento = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "pagamento").option("user", Username).option("password", Password).load()
df_pedido = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "pedido").option("user", Username).option("password", Password).load()

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

storageAccountName = "datalakec9a8951eabdc0653"
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

try:
    dbutils.fs.unmount((f"/mnt/{storageAccountName}/landing-zone"))
    dbutils.fs.unmount((f"/mnt/{storageAccountName}/bronze"))
    dbutils.fs.unmount((f"/mnt/{storageAccountName}/silver"))
    dbutils.fs.unmount((f"/mnt/{storageAccountName}/gold"))
except:
    print("Sem pontos montados.")

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

df_cardapio.show(10)
df_comanda.show(10)
df_estoque.show(10)
df_funcionarios.show(10)
df_ingredientes.show(10)
df_mesas.show(10)
df_pagamento.show(10)
df_pedido.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando os dataframes em delta lake (formato de arquivo) no data lake (repositorio cloud)

# COMMAND ----------

df_cardapio.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Cardapio")
df_comanda.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Comanda")
df_estoque.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Estoque")
df_funcionarios.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Funcionarios")
df_ingredientes.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Ingredientes")
df_mesas.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Mesas")
df_pagamento.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Pagamento")
df_pedido.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Pedido")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Verificando os dados gravados em delta na camada bronze

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Cardapio"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Comanda"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Estoque"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Funcionarios"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Ingredientes"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Mesas"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Pagamento"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Pedido"))
