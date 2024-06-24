# Databricks notebook source
# MAGIC %md
# MAGIC ##Validando a SparkSession

# COMMAND ----------

spark

# COMMAND ----------

import pandas as pd
import pyodbc

# Configurações da conexão com o SQL Server
server = ''
database = ''
username = ''
password = ''


# COMMAND ----------

try:
    # Conectando ao SQL Server

    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Connection Timeout=180')


    # Especificando a query SQL ou a tabela que você deseja ler
    query_cardapio = "SELECT * FROM cardapio"

    # Lendo os dados do SQL Server para um DataFrame do pandas
    df_cardapio = pd.read_sql(query_cardapio, conn)
    
    print("conectou ebaa")
    # Fechar a conexão
    conn.close()
except pyodbc.Error as e:
    print("Erro ao conectar-se ao banco de dados:", e)

try:
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    query_comanda = "SELECT * FROM comanda"

    df_comanda = pd.read_sql(query_comanda, conn)
    
    conn.close()
except pyodbc.Error as e:
    print("Erro ao conectar-se ao banco de dados:", e)

try:
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    query_estoque = "SELECT * FROM estoque"

    df_estoque = pd.read_sql(query_estoque, conn)
    
    conn.close()
except pyodbc.Error as e:
    print("Erro ao conectar-se ao banco de dados:", e)

try:
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    query_funcionarios = "SELECT * FROM funcionarios"

    df_funcionarios = pd.read_sql(query_funcionarios, conn)
    
    conn.close()
except pyodbc.Error as e:
    print("Erro ao conectar-se ao banco de dados:", e)

try:
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    query_ingredientes = "SELECT * FROM ingredientes"

    df_ingredientes = pd.read_sql(query_ingredientes, conn)
    
    conn.close()
except pyodbc.Error as e:
    print("Erro ao conectar-se ao banco de dados:", e)

try:
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    query_mesas = "SELECT * FROM mesas"

    df_mesas = pd.read_sql(query_mesas, conn)
    
    conn.close()
except pyodbc.Error as e:
    print("Erro ao conectar-se ao banco de dados:", e)

try:
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    query_pagamento = "SELECT * FROM pagamento"

    df_pagamento = pd.read_sql(query_pagamento, conn)
    
    conn.close()
except pyodbc.Error as e:
    print("Erro ao conectar-se ao banco de dados:", e)

try:
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    query_pedido = "SELECT * FROM pedido"

    df_pedido = pd.read_sql(query_pedido, conn)
    
    conn.close()
except pyodbc.Error as e:
    print("Erro ao conectar-se ao banco de dados:", e)


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
# MAGIC ###Salvando os dataframes em delta lake (formato de arquivo) no data lake (repositorio cloud)

# COMMAND ----------

df_cardapio.write.format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Cardapio")
df_comanda.write.format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Comanda")
df_estoque.write.format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Estoque")
df_funcionarios.write.format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Funcionarios")
df_ingredientes.write.format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Ingredientes")
df_mesas.write.format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Mesas")
df_pagamento.write.format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Pagamento")
df_pedido.write.format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Pedido")


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
