from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from kafka import KafkaProducer
import json
import time

app = FastAPI()

# -------------------------------
# CONFIG
KAFKA_SERVER = "kafka:9092"
TEMPO_LIMITE = 60
LIMITE = 100_000

# -------------------------------
# ESTADO
historico = []
producer = None
inicio = None
contador = 0

# -------------------------------
def normalizar(dados):
    return {
        k: (v if v not in ["", None] else None)
        for k, v in dados.items()
    }

# -------------------------------
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

    print("❌ Kafka não disponível")

# -------------------------------
@app.get("/monitor", response_class=HTMLResponse)
def monitor():
    return """
    <html>
        <head>
            <title>Monitor</title>
            <meta http-equiv="refresh" content="2">
        </head>
        <body>
            <h1>📊 Transações</h1>
            <div id="dados"></div>

            <script>
                fetch('/historico')
                .then(r => r.json())
                .then(data => {
                    document.getElementById("dados").innerHTML =
                        "<pre>" + JSON.stringify(data, null, 2) + "</pre>";
                });
            </script>
        </body>
    </html>
    """

# -------------------------------
@app.post("/transacao")
def receber_transacao(transacao: dict):
    global contador, inicio

    # 🔥 GARANTIA ABSOLUTA (resolve seu erro)
    if inicio is None:
        inicio = time.time()

    # 🔥 proteção extra (caso raro)
    if inicio is None:
        return {"status": "erro ao iniciar contador"}

    # ⏱ limite de tempo
    if (time.time() - inicio) > TEMPO_LIMITE:
        return {"status": "tempo limite atingido"}

    # 🔢 limite de volume
    if contador >= LIMITE:
        return {"status": "limite de transações atingido"}

    transacao = normalizar(transacao)
    historico.append(transacao)

    if producer:
        producer.send("transacoes", transacao)
        contador += 1

        print(f"📤 Total enviado: {contador}")

        return {"status": "enviado para kafka"}
    else:
        return {"status": "kafka indisponível"}

# -------------------------------
@app.get("/historico")
def ver_historico():
    return historico