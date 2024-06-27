# Como Executar



## Scripts que serão executados:

<br>
### Escrevendo dados na camada landing.

```
spark



Host = ""
Port = 1433
Database = "pingado-database"
Username = ""
Password = ""
Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
Url = f"jdbc:sqlserver://{Host}:{Port};databaseName={Database}"



df_cardapio = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "cardapio").option("user", Username).option("password", Password).load()
df_comanda = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "comanda").option("user", Username).option("password", Password).load() 
df_estoque = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "estoque").option("user", Username).option("password", Password).load() 
df_funcionarios = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "funcionarios").option("user", Username).option("password", Password).load() 
df_ingredientes = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "ingredientes").option("user", Username).option("password", Password).load()
df_mesas = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "mesas").option("user", Username).option("password", Password).load()
df_pagamento = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "pagamento").option("user", Username).option("password", Password).load()
df_pedido = spark.read.format("jdbc").option("driver", Driver).option("url", Url).option("dbtable", "pedido").option("user", Username).option("password", Password).load()



display(dbutils.fs.mounts())



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



try:
    dbutils.fs.unmount((f"/mnt/{storageAccountName}/landing-zone"))
    dbutils.fs.unmount((f"/mnt/{storageAccountName}/bronze"))
    dbutils.fs.unmount((f"/mnt/{storageAccountName}/silver"))
    dbutils.fs.unmount((f"/mnt/{storageAccountName}/gold"))
except:
    print("Sem pontos montados.")



mount_adls('landing-zone')
mount_adls('bronze')
mount_adls('silver')
mount_adls('gold')



display(dbutils.fs.mounts())



df_cardapio.show(10)
df_comanda.show(10)
df_estoque.show(10)
df_funcionarios.show(10)
df_ingredientes.show(10)
df_mesas.show(10)
df_pagamento.show(10)
df_pedido.show(10)



df_cardapio.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Cardapio")
df_comanda.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Comanda")
df_estoque.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Estoque")
df_funcionarios.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Funcionarios")
df_ingredientes.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Ingredientes")
df_mesas.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Mesas")
df_pagamento.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Pagamento")
df_pedido.write.option("header", "true").format('csv').save(f"/mnt/{storageAccountName}/landing-zone/Pedido")



display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Cardapio"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Comanda"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Estoque"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Funcionarios"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Ingredientes"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Mesas"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Pagamento"))
display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone/Pedido"))

```
<br>
### Passando os dados para a camada bronze.
```
spark



display(dbutils.fs.mounts())


storageAccountName = ""
storageAccountAccessKey = ""
sasToken = ""



display(dbutils.fs.mounts())



display(dbutils.fs.ls(f"/mnt/{storageAccountName}/landing-zone"))



df_cardapio = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Cardapio")
df_comanda = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Comanda")
df_estoque = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Estoque")
df_funcionarios = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Funcionarios")
df_ingredientes = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Ingredientes")
df_mesas = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Mesas")
df_pagamento = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Pagamento")
df_pedido = spark.read.format('csv').option("header", "true").load(f"/mnt/{storageAccountName}/landing-zone/Pedido")



from pyspark.sql.functions import current_timestamp, lit

df_cardapio = df_cardapio.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("cardapio.csv"))
df_comanda = df_comanda.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("comanda.csv"))
df_estoque = df_estoque.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("estoque.csv"))
df_funcionarios = df_funcionarios.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("funcionarios.csv"))
df_ingredientes = df_ingredientes.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("ingredientes.csv"))
df_mesas = df_mesas.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("mesas.csv"))
df_pagamento = df_pagamento.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("pagamento.csv"))
df_pedido = df_pedido.withColumn("DATA_HORA_BRONZE", current_timestamp()).withColumn("NOME_ARQUIVO", lit("pedido.csv"))



df_cardapio.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Cardapio")
df_comanda.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Comanda")
df_estoque.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Estoque")
df_funcionarios.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Funcionarios")
df_ingredientes.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Ingredientes")
df_mesas.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Mesas")
df_pagamento.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Pagamento")
df_pedido.write.format('delta').save(f"/mnt/{storageAccountName}/bronze/Pedido")



spark.read.format('delta').load(f'/mnt/{storageAccountName}/bronze/Cardapio').limit(10).display()

```

<br>
###Passando os dados para a camada prata.

```
spark



display(dbutils.fs.mounts())



storageAccountName = ""
storageAccountAccessKey = ""
sasToken = ""



display(dbutils.fs.ls(f"/mnt/{storageAccountName}/bronze"))



df_cardapio = spark.read.format('delta').load(f"/mnt/{storageAccountName}/bronze/Cardapio")



from pyspark.sql.functions import current_timestamp, lit

df_cardapio = df_cardapio.withColumn("DATA_HORA_SILVER", current_timestamp()).withColumn("NOME_ARQUIVO", lit("cardapio"))



colunas = df_cardapio.columns


colunas_maiusculas = [coluna.upper() for coluna in colunas]


print("Colunas em maiúsculas:")
for coluna in colunas_maiusculas:
    print(coluna)



df_cardapio = (df_cardapio
               .withColumnRenamed("id_item_cardapio","CODIGO_ITEM_CARDAPIO")
               .withColumnRenamed("nome_item" , "NOME_ITEM_CARDAPIO")
               .withColumnRenamed("valor" , "VALOR")
               .withColumnRenamed("descricao" , "DESCRICAO")
               .withColumnRenamed("categoria" , "CATEGORIA")
               .withColumnRenamed("disponibilidade" , "DISPONIBILIDADE")
               .withColumnRenamed("data_hora_bronze" , "DATA_HORA_BRONZE")
               .withColumnRenamed("nome_arquivo" , "NOME_ARQUIVO")
               .withColumnRenamed("data_hora_silver" , "DATA_HORA_SILVER"))



df_cardapio.display()



df_cardapio = df_cardapio.dropDuplicates()



df_cardapio = df_cardapio.fillna({"VALOR": 0, "DISPONIBILIDADE": "False"})



df_cardapio.write.format('delta').save(f"/mnt/{storageAccountName}/silver/Cardapio")



display(dbutils.fs.ls(f"/mnt/{storageAccountName}/silver/"))


spark.read.format('delta').load(f'/mnt/{storageAccountName}/silver/Cardapio').limit(10).display()

```

<br>
### Passando os dados para a camada gold.

```
spark



display(dbutils.fs.mounts())



storageAccountName = ""
storageAccountAccessKey = ""
sasToken = "s"



display(dbutils.fs.ls(f"/mnt/{storageAccountName}/silver"))



df_cardapio = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Cardapio")
df_comanda = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Comanda")
df_estoque = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Estoque")
df_funcionarios = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Funcionarios")
df_ingredientes = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Ingredientes")
df_mesas = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Mesas")
df_pagamento = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Pagamento")
df_pedido = spark.read.format('delta').load(f"/mnt/{storageAccountName}/silver/Pedido")



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



df_obt.write.format('delta').mode('overwrite').save(f'/mnt/{storageAccountName}/gold/obt_cafeteria')



spark.read.format('delta').load(f'/mnt/{storageAccountName}/gold/obt_cafeteria').limit(10).display()

```

<br>
### Criar as tabelas para o dashboard.

```
spark

storageAccountName = ""
storageAccountAccessKey = ""
sasToken = ""
%pip install matplotlib pandas

obt = spark.read.format('delta').load(f'/mnt/{storageAccountName}/gold/obt_cafeteria')
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, month
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("FaturamentoItem").getOrCreate()

data_inicio = datetime.now().replace(day=1) - timedelta(days=1)
data_fim = datetime(data_inicio.year, data_inicio.month, 1)

obt_ultimo_mes = obt.filter((col("data_hora_pedido") >= data_fim)
                            & (col("data_hora_pedido") <= data_inicio))

faturamento_por_item = obt_ultimo_mes.groupBy("nome_item_cardapio") \
                                     .agg(sum("valor_item_cardapio").alias("faturamento_total")) \
                                     .orderBy("faturamento_total", ascending=False) \
                                     .toPandas()

nomes_itens = faturamento_por_item["nome_item_cardapio"]
faturamentos = faturamento_por_item["faturamento_total"]

plt.figure(figsize=(10, 6))
plt.bar(nomes_itens, faturamentos, color='skyblue')
plt.xlabel('Nome do Item')
plt.ylabel('Faturamento Total')
plt.title('Faturamento Total por Item - Último Mês')
plt.xticks(rotation=45)
plt.tight_layout()

item_maior_faturamento = faturamento_por_item.iloc[0]
plt.annotate(f'Maior Faturamento: R$ {item_maior_faturamento["faturamento_total"]:.2f}',
             xy=(item_maior_faturamento.name, item_maior_faturamento["faturamento_total"]),
             xytext=(0, 20),
             textcoords="offset points",
             ha='center',
             fontsize=10,
             bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.5))

plt.show()

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, month
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("KPI_Forma_Pagamento_Ultimo_Mes").getOrCreate()

data_inicio = datetime.now().replace(day=1) - timedelta(days=1)
data_fim = datetime(data_inicio.year, data_inicio.month, 1)

obt_ultimo_mes = obt.filter((col("data_hora_pedido") >= data_fim)
                            & (col("data_hora_pedido") <= data_inicio))

faturamento_por_forma_pagamento = obt_ultimo_mes.groupBy("tipo_pagamento") \
                                                .agg(sum("valor_total").alias("faturamento_total")) \
                                                .orderBy("faturamento_total", ascending=False) \
                                                .toPandas()  # Converte para Pandas para facilitar a plotagem com matplotlib

tipos_pagamento = faturamento_por_forma_pagamento["tipo_pagamento"]
faturamentos = faturamento_por_forma_pagamento["faturamento_total"]

plt.figure(figsize=(4, 4))
plt.pie(faturamentos, labels=tipos_pagamento, autopct='%1.1f%%', startangle=140)
plt.title('Faturamento por Tipo de Pagamento - Último Mês')
plt.axis('equal')  # Mantém o aspecto de um círculo
plt.tight_layout()


plt.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("KPI_PedidosEmAberto").getOrCreate()

pedidos_em_aberto = obt.filter(col("status_pagamento") == False).count()

plt.figure(figsize=(4, 2))  # Tamanho da KPI
plt.text(0.5, 0.5, f'Pedidos em Aberto:\n{pedidos_em_aberto}',
         fontsize=14, ha='center', va='center', bbox=dict(facecolor='lightcoral', alpha=0.5))

plt.axis('off')  # Remove os eixos

plt.show()

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, month
from datetime import datetime, timedelta


spark = SparkSession.builder.appName("KPI_SomaTaxaCartaoCredito").getOrCreate()


data_inicio = datetime.now().replace(day=1) - timedelta(days=1)
data_fim = datetime(data_inicio.year, data_inicio.month, 1)


obt_ultimo_mes_cartao_credito = obt.filter((col("data_hora_pedido") >= data_fim)
                                          & (col("data_hora_pedido") <= data_inicio)
                                          & (col("tipo_pagamento") == "Cartão de Crédito"))


soma_taxa_cartao_credito = obt_ultimo_mes_cartao_credito.agg(sum("taxa_pagamento").alias("soma_taxa")).collect()[0]["soma_taxa"]

plt.figure(figsize=(4, 2))  # Tamanho da KPI
plt.text(0.5, 0.5, f'Taxa de recebimento\nde Cartão de Crédito:\nR$ {soma_taxa_cartao_credito:.2f}',
         fontsize=14, ha='center', va='center', bbox=dict(facecolor='lightblue', alpha=0.5))

plt.axis('off')  # Remove os eixos


plt.show()

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("FaturamentoUltimosSeteDias").getOrCreate()

data_atual = datetime.now().date()
datas_para_exibir = [(data_atual.replace(day=23) - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

faturamento_ultimos_sete_dias = obt.filter(
    F.col('data_hora_pedido').cast('date').isin(datas_para_exibir)
).groupBy(
    F.col('data_hora_pedido').cast('date').alias('data')
).agg(
    F.sum('valor_total').alias('faturamento_total')
).orderBy(
    'data'
)

faturamento_ultimos_sete_dias_pd = faturamento_ultimos_sete_dias.toPandas()

faturamento_ultimos_sete_dias_pd['data'] = pd.to_datetime(faturamento_ultimos_sete_dias_pd['data'])
faturamento_ultimos_sete_dias_pd['data'] = faturamento_ultimos_sete_dias_pd['data'].dt.strftime('%d/%m/%Y')

faturamento_ultimos_sete_dias_pd = faturamento_ultimos_sete_dias_pd.sort_values(by='data')

plt.figure(figsize=(10, 6))
plt.bar(faturamento_ultimos_sete_dias_pd['data'], faturamento_ultimos_sete_dias_pd['faturamento_total'], color='green')
plt.title('Faturamento Diário nos Últimos 7 Dias')
plt.xlabel('Data')
plt.ylabel('Faturamento Total (R$)')
plt.xticks(rotation=45)
plt.tight_layout()

total_faturado = faturamento_ultimos_sete_dias_pd['faturamento_total'].sum()
plt.annotate(f'Total faturado: R$ {total_faturado:.2f}',
             xy=(0.5, 0.5),
             xycoords='axes fraction',
             xytext=(0, 20),
             textcoords='offset points',
             ha='center',
             fontsize=12,
             bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.5))

plt.show()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("KPIComandasComBebida").getOrCreate()

comandas_com_bebida = obt.filter(F.col('categoria_item_cardapio') == 'Bebida')

total_comandas = obt.select('codigo_comanda').distinct().count()

total_comandas_com_bebida = comandas_com_bebida.select('codigo_comanda').distinct().count()

porcentagem_comandas_com_bebida = (total_comandas_com_bebida / total_comandas) * 100

import matplotlib.pyplot as plt

labels = ['Comandas com Bebida E Comida', 'Comandas Individuais']
sizes = [porcentagem_comandas_com_bebida, 100 - porcentagem_comandas_com_bebida]
colors = ['#ff9999','#66b3ff']
explode = (0.1, 0)  # explode 1st slice

plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
        shadow=True, startangle=140)

plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.title('Porcentagem de Comandas com Bebida')

plt.show()

spark.read.format('delta').load(f'/mnt/{storageAccountName}/gold/obt_cafeteria').limit(10).display()
obt.createOrReplaceTempView("obt_view")
%sql
select count(codigo_comanda) from obt_view
```

## Ir para ...

- -> **[Apresentação](./index.md)**.
- -> **[Pré-Requisitos e Ferramentas](./prerequisitos.md)**.
- -> **[Execução](./comoExecutar.md)**.





