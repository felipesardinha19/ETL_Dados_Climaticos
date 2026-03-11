import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from sqlalchemy import create_engine

# criar sessão com cache e retry
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

openmeteo = openmeteo_requests.Client(session=retry_session)

#Buscar Dados
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": [-23.5475, -22.7253],
    "longitude": [-46.6361, -47.6492],
    "hourly": "temperature_2m"
}

responses = openmeteo.weather_api(url, params=params)

#Mapear e Tratar dados
cidades = {
    (-23.50, -46.625): "Sao Paulo",
    (-22.75, -47.625): "Piracicaba"
}
dados = []
for response in responses:
    latitude = response.Latitude()
    longitude = response.Longitude()
    hourly = response.Hourly()
    temperatura = hourly.Variables(0).ValuesAsNumpy()

    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s"),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )

    df = pd.DataFrame(
        {"data": times,
        "temperatura": temperatura,
        "latitude": latitude,
        "longitude": longitude}
    )
    dados.append(df)
    

clima_df = pd.concat(dados)

#criando coluna cidade
clima_df["cidade"] = clima_df.apply(
    lambda x: cidades[(x["latitude"], x["longitude"])], axis=1
)

clima_df["cidade"] = clima_df["cidade"].astype(str)

#criando conexão e carregando dados para o banco
engine = create_engine(
    "postgresql+psycopg2://postgres:12345@localhost:5432/clima_db",
    connect_args={"client_encoding": "utf8"}
)
clima_df.to_sql("tempo", engine, if_exists="append", index=False)