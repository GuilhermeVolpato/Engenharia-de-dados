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
# MAGIC ### Mostrando todos os arquivos da camada silver

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storageAccountName}/silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gerando um dataframe dos delta lake no container bronze do Azure Data Lake Storage

# COMMAND ----------

df_cardapio = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Cardapio")
df_comanda = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Comanda")
df_estoque = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Estoque")
df_funcionarios = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Funcionarios")
df_ingredientes = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Ingredientes")
df_mesas = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Mesas")
df_pagamento = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Pagamento")
df_pedido = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Pedido")

# COMMAND ----------

df_obt = df_comanda \
    .join(df_pagamento, df_comanda.CODIGO_PAGAMENTO == df_pagamento.CODIGO_PAGAMENTO) \
    .join(df_funcionarios, df_comanda.CODIGO_FUNCIONARIO == df_funcionarios.CODIGO_FUNCIONARIO) \
    .join(df_mesas, df_comanda.CODIGO_MESA == df_mesas.CODIGO_MESA) \
    .join(df_pedido, df_comanda.CODIGO_COMANDA == df_pedido.CODIGO_COMANDA) \
    .join(df_cardapio, df_pedido.CODIGO_CARDAPIO == df_cardapio.CODIGO_ITEM_CARDAPIO) \
    .select(
        df_comanda.CODIGO_COMANDA.alias("CODIGO_COMANDA"),
        df_comanda.CODIGO_MESA.alias("CODIGO_MESA"),
        df_comanda.CODIGO_PAGAMENTO.alias("CODIGO_PAGAMENTO"),
        df_comanda.CODIGO_FUNCIONARIO.alias("CODIGO_FUNCIONARIO"),
        df_comanda.VALOR_TOTAL.alias("VALOR_TOTAL"),
        df_pagamento.TIPO.alias("TIPO_PAGAMENTO"),
        df_pagamento.STATUS.alias("STATUS_PAGAMENTO"),
        df_pagamento.TAXA.alias("TAXA_PAGAMENTO"),
        df_funcionarios.NOME.alias("NOME_FUNCIONARIO"),
        df_funcionarios.SOBRENOME.alias("SOBRENOME_FUNCIONARIO"),
        df_funcionarios.TELEFONE.alias("TELEFONE_FUNCIONARIO"),
        df_funcionarios.EMAIL.alias("EMAIL_FUNCIONARIO"),
        df_funcionarios.CARGO.alias("CARGO_FUNCIONARIO"),
        df_funcionarios.DATA_CONTRATACAO.alias("DATA_CONTRATACAO_FUNCIONARIO"),
        df_mesas.QUANTIDADE_LUGARES.alias("QUANTIDADE_LUGARES_MESA"),
        df_mesas.LOCAL.alias("LOCAL_MESA"),
        df_mesas.STATUS.alias("STATUS_MESA"),
        df_pedido.CODIGO_PEDIDO.alias("CODIGO_PEDIDO"),
        df_pedido.CODIGO_CARDAPIO.alias("CODIGO_CARDAPIO"),
        df_pedido.STATUS.alias("STATUS_PEDIDO"),
        df_pedido.DATA_HORA_PEDIDO.alias("DATA_HORA_PEDIDO"),
        df_pedido.QUANTIDADE.alias("QUANTIDADE_PEDIDO"),
        df_cardapio.NOME_ITEM_CARDAPIO.alias("NOME_ITEM_CARDAPIO"),
        df_cardapio.VALOR.alias("VALOR_ITEM_CARDAPIO"),
        df_cardapio.DESCRICAO.alias("DESCRICAO_ITEM_CARDAPIO"),
        df_cardapio.CATEGORIA.alias("CATEGORIA_ITEM_CARDAPIO"),
        df_cardapio.DISPONIBILIDADE.alias("DISPONIBILIDADE_ITEM_CARDAPIO"),
        df_comanda.DATA_HORA_BRONZE.alias("DATA_HORA_BRONZE"),
        df_comanda.DATA_HORA_SILVER.alias("DATA_HORA_SILVER"),
    )

# COMMAND ----------

df_obt.write.format('delta').mode('overwrite').save(f'/mnt/{storageAccountName}/gold/obt_cafeteria')

# COMMAND ----------

spark.read.format('delta').load(f'/mnt/{storageAccountName}/gold/obt_cafeteria').limit(10).display()
