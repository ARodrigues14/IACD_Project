import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, count, sum as spark_sum, avg, udf, lit, from_json, window
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, BooleanType, IntegerType, FloatType
from math import radians, sin, cos, sqrt, atan2

# --- CONFIGURAÇÃO ---
KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "smtuc_demand"
DB_HOST = "postgres"
DB_PORT = 5432
DB_NAME = "simulacao_db"
DB_USER = "postgres"
DB_PASSWORD = "19Fevereiro2002"
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
STOPS_FILE = "/opt/spark/work-dir/data/stops_smtuc.txt"

# --- FUNÇÕES DE NEGÓCIO ---
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcula distância em metros entre dois pontos (Haversine)"""
    R = 6371000  # Raio da Terra em metros
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(phi1) * cos(phi2) * sin(dlambda/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1-a))

def load_bus_stops_broadcast(spark):
    """Carrega paragens e faz broadcast para todos workers"""
    stops = []
    try:
        with open(STOPS_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stops.append({
                    'lat': float(row['stop_lat']),
                    'lon': float(row['stop_lon']),
                    'name': row['stop_name']
                })
        print(f"✅ Carregadas {len(stops)} paragens SMTUC")
    except Exception as e:
        print(f"⚠️ Erro ao carregar paragens: {e}")
    
    return spark.sparkContext.broadcast(stops)

def check_stop_needed(user_lat, user_lon, stops_broadcast):
    """UDF: Verifica se precisa de paragem (True) ou não (False)"""
    stops = stops_broadcast.value
    for stop in stops:
        distance = haversine_distance(user_lat, user_lon, stop['lat'], stop['lon'])
        if distance <= 100:  # 100m threshold
            return False  # Paragem existe
    return True  # Precisa de paragem

def find_nearest_stop_distance(user_lat, user_lon, stops_broadcast):
    """UDF: Retorna distância à paragem mais próxima"""
    stops = stops_broadcast.value
    min_distance = float('inf')
    for stop in stops:
        distance = haversine_distance(user_lat, user_lon, stop['lat'], stop['lon'])
        if distance < min_distance:
            min_distance = distance
    return float(min_distance)

def main():
    print("="*80)
    print("🚀 SPARK STRUCTURED STREAMING - Processamento em Tempo Real")
    print("="*80)
    
    # 1. Criar Spark Session com Streaming
    spark = SparkSession.builder \
        .appName("SMTUC_Streaming_Analysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"🎯 Spark Streaming Mode: Kafka → Real-time Processing")
    print(f"🔧 Cores disponíveis: {spark.sparkContext.defaultParallelism}")
    
    # 2. Carregar paragens SMTUC (broadcast)
    stops_broadcast = load_bus_stops_broadcast(spark)
    
    # 3. Registar UDFs
    check_stop_udf = udf(lambda lat, lon: check_stop_needed(lat, lon, stops_broadcast), BooleanType())
    nearest_distance_udf = udf(lambda lat, lon: find_nearest_stop_distance(lat, lon, stops_broadcast), FloatType())
    
    # 4. Schema do JSON do Kafka
    kafka_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("user_lat", DoubleType(), True),
        StructField("user_lon", DoubleType(), True),
        StructField("zone_hint", StringType(), True)
    ])
    
    # 5. Ler STREAM do Kafka
    print(f"\n📡 Conectando ao Kafka Stream: {KAFKA_SERVER}/{KAFKA_TOPIC}...")
    try:
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print(f"✅ Conectado ao Kafka Stream")
    except Exception as e:
        print(f"❌ Erro ao conectar ao Kafka: {e}")
        spark.stop()
        return
    
    # 6. Parse do JSON do Kafka
    print("\n🔬 Processando mensagens do Kafka...")
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), kafka_schema).alias("data")) \
        .select("data.*")
    
    # 7. Aplicar UDFs (validação de proximidade)
    validated_df = parsed_df \
        .withColumn("extra_stop_needed", check_stop_udf(col("user_lat"), col("user_lon"))) \
        .withColumn("distance_to_nearest_stop", nearest_distance_udf(col("user_lat"), col("user_lon"))) \
        .withColumn("grid_lat", spark_round(col("user_lat"), 3)) \
        .withColumn("grid_lon", spark_round(col("user_lon"), 3))
    
    # 8. Escrever STREAM para PostgreSQL (raw_requests em tempo real)
    print("\n💾 Iniciando streaming para PostgreSQL (raw_requests)...")
    
    def write_to_postgres_raw(batch_df, batch_id):
        """Escreve batch para raw_requests"""
        if batch_df.isEmpty():
            return
        
        batch_df.select(
            col("timestamp").alias("timestamp_pedido"),
            col("user_lat").alias("latitude"),
            col("user_lon").alias("longitude"),
            col("zone_hint").alias("zona_sugerida")
        ).write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "raw_requests") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"✅ Batch {batch_id}: {batch_df.count()} registos escritos em raw_requests")
    
    # 9. Escrever STREAM agregado para PostgreSQL (aggregated_demand)
    def write_to_postgres_aggregated(batch_df, batch_id):
        """Escreve agregações para aggregated_demand"""
        if batch_df.isEmpty():
            return
        
        # Agregar o batch
        aggregated = batch_df \
            .groupBy("zone_hint", "grid_lat", "grid_lon") \
            .agg(
                count("*").alias("total_requests"),
                spark_sum(col("extra_stop_needed").cast("int")).alias("requests_needing_stop"),
                spark_sum((~col("extra_stop_needed")).cast("int")).alias("requests_with_stop"),
                spark_round((spark_sum(col("extra_stop_needed").cast("int")) / count("*") * 100), 2).alias("percentage_gap"),
                spark_round(avg(col("distance_to_nearest_stop")), 2).alias("avg_distance_to_stop")
            )
        
        # Gravar (append - PostgreSQL vai consolidar)
        aggregated.select(
            col("zone_hint").alias("zona_sugerida"),
            col("grid_lat"),
            col("grid_lon"),
            col("total_requests"),
            col("requests_needing_stop"),
            col("requests_with_stop"),
            col("percentage_gap"),
            col("avg_distance_to_stop")
        ).write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "aggregated_demand") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        # Mostrar zonas críticas do batch
        critical = aggregated.filter((col("percentage_gap") > 50) & (col("total_requests") >= 5))
        if critical.count() > 0:
            print(f"\n🚨 Batch {batch_id}: {critical.count()} zonas críticas detectadas")
            critical.show(5, truncate=False)
    
    # 10. Iniciar streaming queries
    query_raw = validated_df \
        .writeStream \
        .foreachBatch(write_to_postgres_raw) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    query_agg = validated_df \
        .writeStream \
        .foreachBatch(write_to_postgres_aggregated) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("\n" + "="*80)
    print("✅ STREAMING ATIVO - Processando em tempo real...")
    print("📡 Raw data: a cada 10 segundos")
    print("📊 Agregações: a cada 30 segundos")
    print("="*80)
    
    # 11. Aguardar término (Ctrl+C para parar)
    query_raw.awaitTermination()
    query_agg.awaitTermination()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()