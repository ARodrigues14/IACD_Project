import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# CONFIGURAÇÃO
KAFKA_SERVER = 'kafka:9092' 
TOPIC_NAME = 'smtuc_demand'

# Simular zonas com muitos pedidos
ZONAS_ALVO = [
    # Zonas Originais
    {"nome": "Zona ESEC", "lat": 40.2090, "lon": -8.4200},
    {"nome": "Planalto Sta Clara", "lat": 40.1950, "lon": -8.4450},

    # 🎓 Zonas Estudantis (Picos às 8h e 18h)
    {"nome": "Pólo II - Engenharia", "lat": 40.1865, "lon": -8.4160},
    {"nome": "Pólo I - Alta Universitária", "lat": 40.2076, "lon": -8.4260},
    {"nome": "IPC - Escola Agrária (Bencanta)", "lat": 40.2140, "lon": -8.4520},

    # 🏥 Saúde e Serviços (Tráfego constante)
    {"nome": "HUC - Hospitais Univ.", "lat": 40.2118, "lon": -8.4165},
    {"nome": "Hospital dos Covões", "lat": 40.1975, "lon": -8.4370},
    {"nome": "Loja Cidadão / Baixa", "lat": 40.2105, "lon": -8.4285},

    # 🛍️ Comércio e Lazer (Picos ao fim de semana/noite)
    {"nome": "Solum - Alma Shopping", "lat": 40.2015, "lon": -8.4120},
    {"nome": "Forum Coimbra", "lat": 40.2025, "lon": -8.4468},
    {"nome": "Retail Park Eiras", "lat": 40.2330, "lon": -8.4245},

    # 🚆 Interfaces de Transporte (Chegadas de comboios)
    {"nome": "Estação Coimbra-B", "lat": 40.2243, "lon": -8.4410},
    {"nome": "Estação Coimbra-A (Rio)", "lat": 40.2085, "lon": -8.4310},
    {"nome": "Rodoviária (Av. Fernão Magalhães)", "lat": 40.2160, "lon": -8.4340},
    
    # 🏠 Zonas Residenciais Densas (Início/Fim do dia)
    {"nome": "Quinta da Portela", "lat": 40.1910, "lon": -8.4050},
    {"nome": "Vale das Flores", "lat": 40.1960, "lon": -8.4090},
    {"nome": "Monte Formoso", "lat": 40.2205, "lon": -8.4320}
]

def serializer(message):
    return json.dumps(message).encode('utf-8')

print("📱 PRODUCER INICIADO: A simular utilizadores SMTUC...")

try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=serializer)
    print("✅ Ligado ao Kafka! A enviar pedidos...")
    
    while True:
        # Escolhe uma zona e gera um ponto com ligeira variação (simula pessoas diferentes)
        zona = random.choice(ZONAS_ALVO)
        lat = zona["lat"] + random.uniform(-0.0005, 0.0005)
        lon = zona["lon"] + random.uniform(-0.0005, 0.0005)

        msg = {
            "event_type": "request_stop",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user_lat": round(lat, 5),
            "user_lon": round(lon, 5),
            "zone_hint": zona["nome"]
        }

        producer.send(TOPIC_NAME, msg)
        print(f"👉 Pedido em: {zona['nome']}")
        time.sleep(0.5) # Envia a cada meio segundo para encher rápido

except Exception as e:
    print(f"❌ Erro: {e}")