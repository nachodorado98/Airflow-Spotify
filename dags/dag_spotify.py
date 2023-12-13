from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests


# Funcion para extraer los datos de la API de Spotify
def extraccion()->None:

	token=""

	id_artista="2R21vXR83lH98kGeO99Y66"

	url=f"https://api.spotify.com/v1/artists/{id_artista}/top-tracks?market=ES"

	respuesta=requests.get(url, headers={"Authorization":f"Bearer {token}"})

	if respuesta.status_code==200:

		print(respuesta.json())

	else:
		
		print(respuesta.status_code)



with DAG("spotify_dag", start_date=datetime(2023,12,12), description="DAG para obtener datos de la API de Spotify",
		schedule_interval=timedelta(minutes=1), catchup=False) as dag:

	extraccion_data=PythonOperator(task_id="extracion_data", python_callable=extraccion)

	extraccion_data