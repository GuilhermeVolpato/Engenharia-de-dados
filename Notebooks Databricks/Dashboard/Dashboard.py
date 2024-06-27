
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

# Inicializa uma SparkSession
spark = SparkSession.builder.appName("FaturamentoItem").getOrCreate()

# Calcula a data do último mês
data_inicio = datetime.now().replace(day=1) - timedelta(days=1)
data_fim = datetime(data_inicio.year, data_inicio.month, 1)

# Filtra os dados para o último mês
obt_ultimo_mes = obt.filter((col("data_hora_pedido") >= data_fim)
                            & (col("data_hora_pedido") <= data_inicio))

# Calcula o faturamento total por item no último mês
faturamento_por_item = obt_ultimo_mes.groupBy("nome_item_cardapio") \
                                     .agg(sum("valor_item_cardapio").alias("faturamento_total")) \
                                     .orderBy("faturamento_total", ascending=False) \
                                     .toPandas()

# Prepara os dados para o gráfico
nomes_itens = faturamento_por_item["nome_item_cardapio"]
faturamentos = faturamento_por_item["faturamento_total"]

# Cria o gráfico de barras
plt.figure(figsize=(10, 6))
plt.bar(nomes_itens, faturamentos, color='skyblue')
plt.xlabel('Nome do Item')
plt.ylabel('Faturamento Total')
plt.title('Faturamento Total por Item - Último Mês')
plt.xticks(rotation=45)
plt.tight_layout()

# Destaca o item com maior faturamento
item_maior_faturamento = faturamento_por_item.iloc[0]
plt.annotate(f'Maior Faturamento: R$ {item_maior_faturamento["faturamento_total"]:.2f}',
             xy=(item_maior_faturamento.name, item_maior_faturamento["faturamento_total"]),
             xytext=(0, 20),
             textcoords="offset points",
             ha='center',
             fontsize=10,
             bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.5))

# Exibe o gráfico
plt.show()

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, month
from datetime import datetime, timedelta

# Inicializa uma SparkSession
spark = SparkSession.builder.appName("KPI_Forma_Pagamento_Ultimo_Mes").getOrCreate()

# Calcula a data do último mês
data_inicio = datetime.now().replace(day=1) - timedelta(days=1)
data_fim = datetime(data_inicio.year, data_inicio.month, 1)

# Filtra os dados para o último mês
obt_ultimo_mes = obt.filter((col("data_hora_pedido") >= data_fim)
                            & (col("data_hora_pedido") <= data_inicio))

# Calcula o faturamento total por tipo de pagamento no último mês
faturamento_por_forma_pagamento = obt_ultimo_mes.groupBy("tipo_pagamento") \
                                                .agg(sum("valor_total").alias("faturamento_total")) \
                                                .orderBy("faturamento_total", ascending=False) \
                                                .toPandas()  # Converte para Pandas para facilitar a plotagem com matplotlib

# Prepara os dados para o gráfico de pizza (pie chart)
tipos_pagamento = faturamento_por_forma_pagamento["tipo_pagamento"]
faturamentos = faturamento_por_forma_pagamento["faturamento_total"]

# Cria o gráfico de pizza
plt.figure(figsize=(4, 4))
plt.pie(faturamentos, labels=tipos_pagamento, autopct='%1.1f%%', startangle=140)
plt.title('Faturamento por Tipo de Pagamento - Último Mês')
plt.axis('equal')  # Mantém o aspecto de um círculo
plt.tight_layout()

# Exibe o gráfico
plt.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializa uma SparkSession
spark = SparkSession.builder.appName("KPI_PedidosEmAberto").getOrCreate()

# Calcula o número de pedidos em aberto
pedidos_em_aberto = obt.filter(col("status_pagamento") == False).count()

# Prepara a figura para a KPI
plt.figure(figsize=(4, 2))  # Tamanho da KPI
plt.text(0.5, 0.5, f'Pedidos em Aberto:\n{pedidos_em_aberto}',
         fontsize=14, ha='center', va='center', bbox=dict(facecolor='lightcoral', alpha=0.5))

#plt.title('KPI - Pedidos em Aberto', fontsize=16)
plt.axis('off')  # Remove os eixos

# Exibe a KPI
plt.show()

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, month
from datetime import datetime, timedelta

# Inicializa uma SparkSession
spark = SparkSession.builder.appName("KPI_SomaTaxaCartaoCredito").getOrCreate()

# Calcula a data do último mês
data_inicio = datetime.now().replace(day=1) - timedelta(days=1)
data_fim = datetime(data_inicio.year, data_inicio.month, 1)

# Filtra os dados para o último mês e apenas para tipo_pagamento sendo "cartão de crédito"
obt_ultimo_mes_cartao_credito = obt.filter((col("data_hora_pedido") >= data_fim)
                                          & (col("data_hora_pedido") <= data_inicio)
                                          & (col("tipo_pagamento") == "Cartão de Crédito"))

# Calcula a soma da taxa de pagamento para cartão de crédito no último mês
soma_taxa_cartao_credito = obt_ultimo_mes_cartao_credito.agg(sum("taxa_pagamento").alias("soma_taxa")).collect()[0]["soma_taxa"]

# Prepara a figura para a KPI
plt.figure(figsize=(4, 2))  # Tamanho da KPI
plt.text(0.5, 0.5, f'Taxa de recebimento\nde Cartão de Crédito:\nR$ {soma_taxa_cartao_credito:.2f}',
         fontsize=14, ha='center', va='center', bbox=dict(facecolor='lightblue', alpha=0.5))

#plt.title('KPI - Taxa de Recebimento para Cartão de Crédito', fontsize=16)
plt.axis('off')  # Remove os eixos

# Exibe a KPI
plt.show()

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Inicializa uma SparkSession
spark = SparkSession.builder.appName("FaturamentoUltimosSeteDias").getOrCreate()

# Calcula a data de hoje e a data sete dias atrás
data_atual = datetime.now().date()
datas_para_exibir = [(data_atual.replace(day=23) - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

# Filtra os dados para os últimos 7 dias e extrai a data do campo data_hora_pedido
faturamento_ultimos_sete_dias = obt.filter(
    F.col('data_hora_pedido').cast('date').isin(datas_para_exibir)
).groupBy(
    F.col('data_hora_pedido').cast('date').alias('data')
).agg(
    F.sum('valor_total').alias('faturamento_total')
).orderBy(
    'data'
)

# Converte para Pandas DataFrame para plotagem com matplotlib
faturamento_ultimos_sete_dias_pd = faturamento_ultimos_sete_dias.toPandas()

# Formata as datas para dd/mm/aaaa
faturamento_ultimos_sete_dias_pd['data'] = pd.to_datetime(faturamento_ultimos_sete_dias_pd['data'])
faturamento_ultimos_sete_dias_pd['data'] = faturamento_ultimos_sete_dias_pd['data'].dt.strftime('%d/%m/%Y')

# Ordena novamente o DataFrame para garantir a ordem correta no gráfico
faturamento_ultimos_sete_dias_pd = faturamento_ultimos_sete_dias_pd.sort_values(by='data')

# Plotar o gráfico de barras
plt.figure(figsize=(10, 6))
plt.bar(faturamento_ultimos_sete_dias_pd['data'], faturamento_ultimos_sete_dias_pd['faturamento_total'], color='green')
plt.title('Faturamento Diário nos Últimos 7 Dias')
plt.xlabel('Data')
plt.ylabel('Faturamento Total (R$)')
plt.xticks(rotation=45)
plt.tight_layout()

# Adicionar anotação com o total faturado nos últimos 7 dias
total_faturado = faturamento_ultimos_sete_dias_pd['faturamento_total'].sum()
plt.annotate(f'Total faturado: R$ {total_faturado:.2f}',
             xy=(0.5, 0.5),
             xycoords='axes fraction',
             xytext=(0, 20),
             textcoords='offset points',
             ha='center',
             fontsize=12,
             bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.5))

# Exibir o gráfico
plt.show()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inicializa uma SparkSession
spark = SparkSession.builder.appName("KPIComandasComBebida").getOrCreate()

# Supondo que 'obt' é o DataFrame que contém os dados das comandas e que tem as colunas 'codigo_comanda' e 'categoria_item_pedido' Filtra as comandas que têm itens de categoria "bebida"
comandas_com_bebida = obt.filter(F.col('categoria_item_cardapio') == 'Bebida')

#Conta o número total de comandas
total_comandas = obt.select('codigo_comanda').distinct().count()

#Conta o número de comandas que têm pelo menos uma bebida
total_comandas_com_bebida = comandas_com_bebida.select('codigo_comanda').distinct().count()

#Calcula a porcentagem de comandas com bebida
porcentagem_comandas_com_bebida = (total_comandas_com_bebida / total_comandas) * 100

import matplotlib.pyplot as plt

# Valores dos KPIs
labels = ['Comandas com Bebida E Comida', 'Comandas Individuais']
sizes = [porcentagem_comandas_com_bebida, 100 - porcentagem_comandas_com_bebida]
colors = ['#ff9999','#66b3ff']
explode = (0.1, 0)  # explode 1st slice

# Criação do gráfico de pizza
plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
        shadow=True, startangle=140)

# Configurações adicionais do gráfico
plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.title('Porcentagem de Comandas com Bebida')

# Exibição do gráfico
plt.show()

spark.read.format('delta').load(f'/mnt/{storageAccountName}/gold/obt_cafeteria').limit(10).display()
obt.createOrReplaceTempView("obt_view")
%sql
select count(codigo_comanda) from obt_view