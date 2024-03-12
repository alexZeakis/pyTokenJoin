FROM python:3.8-slim
WORKDIR /app
COPY . /app/
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["python3", "main.py"]
