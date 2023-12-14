from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
import requests

from spotify.autenticacion.auth import generarToken
from spotify.autenticacion.config import CLIENTE_ID, CLIENTE_SECRETO

# Funcion para extraer los datos de la API de Spotify
def extraccion(**kwarg)->None:

	token=generarToken(CLIENTE_ID, CLIENTE_SECRETO)

	id_playlist="1oe0dg71NvXWxdmr91BKPb"

	url=f"https://api.spotify.com/v1/playlists/{id_playlist}/tracks"

	respuesta=requests.get(url, headers={"Authorization":f"Bearer {token}"})

	if respuesta.status_code==200:

		contenido=respuesta.json()

		kwarg["ti"].xcom_push(key="data", value=contenido)

		print("Extraccion correcta")

	else:
		
		print(respuesta.status_code)

		raise AirflowSkipException("Error en la extraccion")

# Funcion para transformar los datos de la API de Spotify
def transformacion(**kwarg)->None:

	data=kwarg["ti"].xcom_pull(key="data", task_ids="extracion_data")

	print(data)
	

with DAG("spotify_dag", start_date=datetime(2023,12,12), description="DAG para obtener datos de la API de Spotify",
		schedule_interval=timedelta(minutes=60), catchup=False) as dag:

	extraccion_data=PythonOperator(task_id="extracion_data", python_callable=extraccion)

	transformacion_data=PythonOperator(task_id="transformacion_data", python_callable=transformacion)

	extraccion_data >> transformacion_data
