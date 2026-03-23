from kafka import KafkaProducer
import json
import time
from kafka.admin import KafkaAdminClient, NewTopic

def criar_topico():
    try:
        admin = KafkaAdminClient(bootstrap_servers="kafka:9092")

        topic = NewTopic(
            name="transacoes",
            num_partitions=1,
            replication_factor=1
        )

        admin.create_topics([topic])
        print("Tópico criado com sucesso!")

        admin.close()

    except Exception as e:
        print(f"Tópico já existe ou erro: {e}")


# ⏳ espera Kafka subir
time.sleep(5)

criar_topico()

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
