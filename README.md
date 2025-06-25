# Stock Market ETL Pipeline

A Dockerized ETL pipeline that fetches daily stock market data for AAPL from Alpha Vantage, transforms it by calculating a 7-day moving average, and loads it into a SQLite database.

## Prerequisites
- Docker
- Alpha Vantage API key

## Setup
1. Clone the repository:
```
git clone https://github.com/saccini/stock-etl-pipeline
cd stock-etl-pipeline
```

2. Create a .env file with your API key:
```
ALPHA_VANTAGE_API_KEY=your_api_key
```

3. Build and run the Docker container:
```
docker-compose up --build
```


