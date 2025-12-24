import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# tive que subir o hadoop pq estou usando o win
os.environ['HADOOP_HOME'] = r"C:\hadoop" 
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

# start no spark
spark = SparkSession.builder \
    .appName("Teste_Engenheiro_Dados_PySpark") \
    .config("spark.driver.host", "localhost") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp") \
    .getOrCreate()

# estava dando erro de permissão no meu win
try:
    sc = spark.sparkContext
    jvm = sc._gateway.jvm
    hadoop_config = sc._jsc.hadoopConfiguration()
    jvm.org.apache.hadoop.io.nativeio.NativeIO.Windows.access0(None, 0)
except:
    # se tudo der errado ele implementa o java
    pass

spark.sparkContext.setLogLevel("ERROR")

# leitura dos dados
df_clients = spark.read.json("dados/clients")
df_pedidos = spark.read.json("dados/pedidos")

# coletor das "falhas"
df_falhas = df_pedidos.select(
    "id",
    F.when(F.col("id").isNull(), "ID do pedido nulo")
     .when(F.col("client_id").isNull(), "Client_ID nulo")
     .when(F.col("value") <= 0, "Valor invalido (zero ou negativo)")
     .otherwise(None).alias("motivo")
).filter(F.col("motivo").isNotNull())

print("--- Relatorio de Falhas ---")
df_falhas.show()

# limpando o DF de pedido para a proxima analise (restando apenas os validos)
df_pedidos_validos = df_pedidos.filter(F.col("id").isNotNull() & (F.col("value") > 0))

# juntando os dados (usei o broadcast join aqui pq a df_clients é pequeno (~10k)
df_analise_clientes = df_pedidos_validos.join(
    F.broadcast(df_clients), 
    df_pedidos_validos.client_id == df_clients.id
).groupBy(df_clients.id, "name") \
 .agg(
    F.count(df_pedidos_validos.id).alias("qtd_pedidos"),
    F.sum("value").cast("decimal(11,2)").alias("valor_total")
).orderBy(F.col("valor_total").desc())

print("--- Agregação por Cliente ---")
df_analise_clientes.show()

# calculo do valor total acumulado por cliente
estatisticas = df_analise_clientes.select(
    F.avg("valor_total").alias("media"),
    F.percentile_approx("valor_total", 0.5).alias("mediana"),
    F.percentile_approx("valor_total", 0.1).alias("p10"),
    F.percentile_approx("valor_total", 0.9).alias("p90")
)

print("--- Analise ---")
estatisticas.show()

valores = estatisticas.first()
media_val = valores['media']
p10_val = valores['p10']
p90_val = valores['p90']

# acima da media
df_acima_media = df_analise_clientes.filter(F.col("valor_total") > media_val) \
    .orderBy("valor_total")

# dona Ines tinha um volume massivo de pedidos, ela elevou a media, fazendo com que todos os outros clientes normais ficassem abaixo dela
print("--- Clientes Acima da Media ---")
df_acima_media.show()

# media entre p10 e p90
df_media_semOut = df_analise_clientes.filter(
    (F.col("valor_total") >= p10_val) & (F.col("valor_total") <= p90_val)
).orderBy("valor_total")

print("--- Media(Sem Outliers) ---")
df_media_semOut.show()

# fiiim <3
spark.stop()