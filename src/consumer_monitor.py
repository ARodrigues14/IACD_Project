import json
from kafka import KafkaConsumer
from datetime import datetime

# --- CONFIGURAÇÃO ---
KAFKA_SERVER = 'kafka:9092'  
TOPIC_NAME = 'smtuc_demand'

def start_consumer():
    print("📥 CONSUMER MONITOR INICIADO: Monitorando Kafka...")
    print("⚡ Spark Streaming processa os dados em tempo real")
    
    # Ligar ao Kafka
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"🎧 A escutar o tópico: {TOPIC_NAME}\n")
    except Exception as e:
         print(f"❌ Erro ao ligar ao Kafka: {e}")
         return

    # Loop de monitoramento (apenas logs)
    count = 0
    for message in consumer:
        dados = message.value
        count += 1
        
        zona = dados.get('zone_hint')
        lat = dados.get('user_lat')
        lon = dados.get('user_lon')
        ts = dados.get('timestamp')
        
        print(f"📨 [{count}] {zona} ({lat:.4f}, {lon:.4f}) às {ts}")

if __name__ == "__main__":
    start_consumer()
