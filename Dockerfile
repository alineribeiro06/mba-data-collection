FROM python:3.10

WORKDIR /app

# copia só as dependências primeiro (melhor pra cache)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# depois copia o resto do projeto
COPY . .

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]