# 🚍 SMTUC - Análise de Gaps de Paragens em Tempo Real

Sistema de análise em tempo real para identificar zonas de Coimbra que necessitam de novas paragens de autocarro, utilizando **Spark Structured Streaming**, **Kafka** e **PostgreSQL** em **Kubernetes**.

---

## 🎯 Visão Geral

Este projeto implementa um **sistema de análise em tempo real** que:

- 📡 **Recebe pedidos de viagem** via streaming (Kafka)
- 🔍 **Valida proximidade** de paragens SMTUC (raio de 100m usando fórmula de Haversine)
- 📊 **Agrega dados** por zona e grid geográfico (111m × 111m)
- 🚨 **Identifica zonas críticas** onde >50% dos pedidos não têm paragem próxima
- 💾 **Armazena resultados** em PostgreSQL com dados persistentes

**Caso de Uso:** Planeamento de novas paragens de autocarro com base em padrões de procura real.

---

## 🏗️ Arquitetura

### Componentes Kubernetes

| Pod | Função | Recursos |
|-----|--------|----------|
| **producer** | Gera dados simulados a cada 1s | 512Mi RAM, 0.5 CPU |
| **kafka** | Message broker para streaming | 1Gi RAM, 1 CPU |
| **zookeeper** | Coordenação do Kafka | 512Mi RAM, 0.5 CPU |
| **consumer** | Monitor de mensagens (logs) | 256Mi RAM, 0.25 CPU |
| **smtuc-processor** | Spark Streaming (UDFs + agregação) | 2Gi RAM, 2 CPU |
| **postgres** | Base de dados persistente (PVC 2Gi) | 1Gi RAM, 1 CPU |

---

## 🛠️ Tecnologias

- **Kubernetes** / Minikube - Orquestração de containers
- **Apache Spark 3.5.0** - Structured Streaming com UDFs Python
- **Apache Kafka 2.8.1** - Message broker
- **PostgreSQL 15** - Base de dados com PersistentVolume
- **Docker** - Containerização

---

## 📦 Requisitos

- **Minikube** instalado
- **kubectl** configurado
- **Docker** instalado
- Mínimo: **6GB RAM**, **4 CPUs**

---

## 🚀 Quick Start

```bash
# 1. Start Minikube
minikube start --memory=6144 --cpus=4
eval $(minikube docker-env)

# 2. Build
docker build -t smtuc-spark:v1 .

# 3. Deploy (ordem importa!)
kubectl apply -f k8s/zookeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/postgres.yaml
sleep 30  # Aguardar infraestrutura
kubectl apply -f k8s/producer.yaml
kubectl apply -f k8s/consumer.yaml
kubectl apply -f k8s/processor.yaml

# 4. Monitorar
kubectl logs -f deployment/smtuc-processor
```

---

## 💻 Consultas SQL

```sql
-- Zonas Críticas
SELECT zona_sugerida, total_requests, percentage_gap
FROM aggregated_demand
WHERE percentage_gap > 50
ORDER BY percentage_gap DESC
LIMIT 20;
```

**Acesso PostgreSQL:**
```bash
kubectl exec -it deployment/postgres -- psql -U postgres -d simulacao_db
# Password: ****
```

---

## 📂 Estrutura

```
Projeto/
├── k8s/              # Deployments Kubernetes
├── src/
│   ├── producer/     # Gerador de dados
│   ├── processor/    # Spark Streaming
│   └── consumer_monitor.py
├── data/
│   └── stops_smtuc.txt  # 1,511 paragens
└── README.md
```

---

**Happy Streaming! 🚍📊**
