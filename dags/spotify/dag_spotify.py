from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests
import json

from spotify.config import CLIENTE_ID, CLIENTE_SECRETO

# Funcion para obtener el token
def generarToken(cliente_id:str, cliente_secreto:str)->str:

	url_token="https://accounts.spotify.com/api/token"

	data={"grant_type": "client_credentials", "client_id": cliente_id, "client_secret":cliente_secreto}

	respuesta=requests.post(url_token, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})

	contenido=json.loads(respuesta.content)

	return contenido["access_token"]


# Funcion para extraer los datos de la API de Spotify
def extraccion()->None:

	token=generarToken(CLIENTE_ID, CLIENTE_SECRETO)

	id_artista="2R21vXR83lH98kGeO99Y66"

	url=f"https://api.spotify.com/v1/artists/{id_artista}/top-tracks?market=ES"

	respuesta=requests.get(url, headers={"Authorization":f"Bearer {token}"})

	if respuesta.status_code==200:

		print(respuesta.json())

	else:
		
		print(respuesta.status_code)



with DAG("spotify_dag", start_date=datetime(2023,12,12), description="DAG para obtener datos de la API de Spotify",
		schedule_interval=timedelta(minutes=60), catchup=False) as dag:

	extraccion_data=PythonOperator(task_id="extracion_data", python_callable=extraccion)

	extraccion_data