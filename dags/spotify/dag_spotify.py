from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

from spotify.autenticacion.auth import generarToken
from spotify.autenticacion.config import CLIENTE_ID, CLIENTE_SECRETO

from spotify.database.conexion import crearHook

# Funcion para crear la tabla de las canciones
def crearTabla(hook:PostgresHook=crearHook())->None:

	hook.run("""CREATE TABLE IF NOT EXISTS canciones (id VARCHAR(30) PRIMARY KEY,
														nombre VARCHAR(100),
														artistas VARCHAR(100),
														album VARCHAR(100),
														fecha DATE);""")

# Funcion para extraer los datos de la API de Spotify
def extraccion(**kwarg)->None:

	token=generarToken(CLIENTE_ID, CLIENTE_SECRETO)

	id_playlist="1oe0dg71NvXWxdmr91BKPb"

	url=f"https://api.spotify.com/v1/playlists/{id_playlist}/tracks?limit=100&offset=0"

	respuesta=requests.get(url, headers={"Authorization":f"Bearer {token}"})

	if respuesta.status_code==200:

		contenido=respuesta.json()

		kwarg["ti"].xcom_push(key="data_extraida", value=contenido)

		print("Extraccion correcta")

	else:
		
		print(respuesta.status_code)

		raise AirflowSkipException("Error en la extraccion")

# Funcion para transformar los datos de la API de Spotify
def transformacion(**kwarg)->None:

	data=kwarg["ti"].xcom_pull(key="data_extraida", task_ids="extracion_data")

	# Funcion para limpiar la cancion
	def limpiarCancion(cancion:dict)->tuple:

		id=cancion["track"]["id"]

		nombre=cancion["track"]["name"]

		artistas=[artista["name"] for artista in cancion["track"]["artists"]]

		artistas_cadena=", ".join(artistas)

		album=cancion["track"]["album"]["name"]

		fecha=cancion["added_at"].split("T")[0]

		return id, nombre, artistas_cadena, album, fecha

	canciones=list(map(limpiarCancion, data["items"]))

	kwarg["ti"].xcom_push(key="data_transformada", value=canciones)

	print("Transformacion correcta")

# Funcion para cargar los datos de la API de Spotify
def carga(hook:PostgresHook=crearHook(), **kwarg)->None:

	data=kwarg["ti"].xcom_pull(key="data_transformada", task_ids="transformacion_data")

	canciones=[tuple(cancion) for cancion in data]

	for cancion in canciones:

		print(f"Insertando la cancion '{cancion[1]}'...")

		hook.run("INSERT INTO canciones VALUES %s", parameters=(cancion,))
		

with DAG("spotify_dag", start_date=datetime(2023,12,12), description="DAG para obtener datos de la API de Spotify",
		schedule_interval=timedelta(days=1), catchup=False) as dag:

	creacion_tabla=PythonOperator(task_id="creacion_tabla", python_callable=crearTabla)

	extraccion_data=PythonOperator(task_id="extracion_data", python_callable=extraccion)

	transformacion_data=PythonOperator(task_id="transformacion_data", python_callable=transformacion)

	carga_data=PythonOperator(task_id="carga_data", python_callable=carga)

	creacion_tabla >> extraccion_data >> transformacion_data >> carga_data
