from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from kafka import KafkaProducer
import json
import time
from threading import Lock

app = FastAPI()

historico = []

KAFKA_SERVER = "kafka:9092"
producer = None

# 🔥 NORMALIZAÇÃO (ADICIONADO)
def normalizar(dados):
    return {
        k: (v if v not in ["", None] else None)
        for k, v in dados.items()
    }
# 🔥 CONEXÃO NO STARTUP (CORRETO)
@app.on_event("startup")
def startup_event():
    global producer

    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Conectado ao Kafka!")
            return
        except Exception as e:
            print(f"⏳ Tentando conectar ao Kafka... {e}")
            time.sleep(3)

    print("❌ Kafka não disponível, API continuará sem producer")


# -------------------------------
@app.get("/monitor", response_class=HTMLResponse)
def monitor():
    return """
    <html>
        <head>
            <title>Monitor de Transações</title>
            <meta http-equiv="refresh" content="2">
        </head>
        <body>
            <h1>Transações em tempo real</h1>
            <script>
                fetch('/historico')
                .then(response => response.json())
                .then(data => {
                    document.body.innerHTML += "<pre>" + JSON.stringify(data, null, 2) + "</pre>";
                });
            </script>
        </body>
    </html>
    """


# -------------------------------


inicio = None
contador = 0
LIMITE = 100_000
TEMPO_LIMITE = 60

lock = Lock()

@app.post("/transacao")
def receber_transacao(transacao: dict):
    global contador

    if time.time() - inicio > TEMPO_LIMITE:
        return {"status": "tempo limite atingido"}

    if contador >= LIMITE:
        return {"status": "limite de transações atingido"}

    transacao = normalizar(transacao)

    historico.append(transacao)

    if producer:
        producer.send("transacoes", transacao)
        contador += 1
        print(f"📊 Total enviado: {contador}")
    else:
        return {"status": "kafka indisponível"}

    return {"status": "enviado para kafka"}
# -------------------------------
@app.get("/historico")
def ver_historico():
    return historico