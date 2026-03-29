from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
import time


# -------------------------------
KAFKA_TOPIC = "transacoes"
KAFKA_SERVER = "kafka:9092"

# -------------------------------
DB_CONFIG = {
    "host": "postgres",
    "database": "pipeline_transacoes",
    "user": "postgres",
    "password": "1234"
}

# -------------------------------
# conexão com retry
conn = None

for i in range(10):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 🔥 GARANTE SCHEMA
        cursor.execute("SET search_path TO staging;")
        conn.commit()

        print("✅ Conectado ao PostgreSQL!")
        break
    except Exception as e:
        print(f"⏳ Tentando conectar ao banco... {e}")
        time.sleep(3)

if conn is None:
    raise Exception("❌ Não conseguiu conectar ao banco")

# -------------------------------
consumer = None

for i in range(10):
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="consumer-bronze"
        )
        print("✅ Conectado ao Kafka!")
        break
    except Exception as e:
        print(f"⏳ Tentando conectar ao Kafka... {e}")
        time.sleep(5)

if consumer is None:
    raise Exception("❌ Não conectou no Kafka")

# -------------------------------
def salvar_lote(lista_dados):
    try:
        registros = [(json.dumps(d),) for d in lista_dados]

        cursor.executemany("""
            INSERT INTO staging.transacoes_raw (payload)
            VALUES (%s)
        """, registros)

        conn.commit()
        print(f"🚀 Inserido lote RAW com {len(lista_dados)} registros!")

    except Exception as e:
        conn.rollback()
        print("❌ ERRO NO LOTE:", e)

# -------------------------------
print("🚀 Iniciando ingestão...")

buffer = []
BATCH_SIZE = 100
ultimo_envio = time.time()
INTERVALO = 5  # segundos (simples pra demo)

for message in consumer:
    dados = message.value

    print(f"📥 Recebendo... total buffer: {len(buffer)}")
    # agora salva direto o JSON
    buffer.append(dados)

    # 🔥 condição 1: lote cheio
    if len(buffer) >= BATCH_SIZE:
        salvar_lote(buffer)
        buffer = []
        ultimo_envio = time.time()

    # 🔥 condição 2: tempo (pra não travar)
    elif time.time() - ultimo_envio > INTERVALO:
        if buffer:
            salvar_lote(buffer)
            buffer = []
            ultimo_envio = time.time()
    #limpar cache
    #docker-compose build --no-cache