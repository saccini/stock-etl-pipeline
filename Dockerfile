FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY stock_etl.py .
RUN mkdir /data
ENV PYTHONUNBUFFERED=1
CMD ["python", "stock_etl.py"]