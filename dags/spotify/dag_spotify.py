from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowSkipException
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Optional, Dict, List
import os

from spotify.autenticacion.auth import generarToken
from spotify.autenticacion.config import CLIENTE_ID, CLIENTE_SECRETO

from spotify.database.conexion import crearHook

from spotify.config import TABLA, PLAYLIST, LOGS


# Funcion para crear la carpeta de los logs
def crearCarpetaLogs()->None:

	ruta_carpeta_logs=os.path.join(os.getcwd(), LOGS)

	if not os.path.exists(ruta_carpeta_logs):

		os.makedirs(ruta_carpeta_logs)

# Funcion para crear el archivo txt y almacenarlo en la carpeta
def crearLogs(inicio:bool, canciones_extraidas:int, canciones_anadidas:int=0)->None:

	ruta_carpeta_logs=os.path.join(os.getcwd(), LOGS)

	archivo_log=f"log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"

	ruta_archivo_log=os.path.join(ruta_carpeta_logs, archivo_log)

	with open(ruta_archivo_log, "w") as archivo:

		archivo.write(f"Fecha de ejecucion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
		archivo.write(f"Inicio: {inicio}\n")
		archivo.write(f"Canciones extraidas: {canciones_extraidas}\n")
		archivo.write(f"Canciones añadidas: {canciones_anadidas}\n")
		
# Funcion para comprobar que existe la tabla
def existe_tabla(hook:PostgresHook=crearHook())->str:

	tablas=hook.get_records("SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';")

	return "no_creacion_tabla" if (TABLA,) in tablas else "creacion_tabla"

# Funcion que no crea la tabla de las canciones
def noCrearTabla(hook:PostgresHook=crearHook(), **kwarg)->None:

	print(f"Tabla {TABLA} existente")

	numero_registros=hook.get_records(f"SELECT COUNT(*) FROM {TABLA};")[0][0]

	kwarg["ti"].xcom_push(key="numero_registros", value=numero_registros)

# Funcion para crear la tabla de las canciones
def crearTabla(hook:PostgresHook=crearHook(), **kwarg)->None:

	crearCarpetaLogs()

	hook.run(f"""CREATE TABLE {TABLA} (id VARCHAR(30) PRIMARY KEY,
										nombre VARCHAR(200),
										artistas VARCHAR(200),
										album VARCHAR(200),
										fecha DATE);""")
	
	print(f"Tabla {TABLA} creada correctamente")

	kwarg["ti"].xcom_push(key="numero_registros", value=0)

# Funcion para realizar una peticion a la API
def peticion_API(token:str, salto:int=0, limite:int=100)->Optional[Dict]:

	url=f"https://api.spotify.com/v1/playlists/{PLAYLIST}/tracks?limit={limite}&offset={salto}"

	respuesta=requests.get(url, headers={"Authorization":f"Bearer {token}"})

	if respuesta.status_code==200:

		return respuesta.json()

	else:
		
		print(respuesta.status_code)

		raise AirflowSkipException("Error en la extraccion")

# Funcion para la iteracion en la extraccion desde el salto hasta el final
def extraerCanciones(salto:int=0)->List[Dict]:

	token=generarToken(CLIENTE_ID, CLIENTE_SECRETO)

	items=[]

	while True:

		contenido=peticion_API(token, salto)

		if len(contenido["items"])==0:

			break

		items+=contenido["items"]

		salto+=100

	return items

# Funcion para extraer los datos de la API de Spotify
def extraccion(**kwarg)->None:

	salto=kwarg["ti"].xcom_pull(key="numero_registros", task_ids=["creacion_tabla", "no_creacion_tabla"])[0]

	canciones=extraerCanciones(salto)

	if len(canciones)==0:

		crearLogs(False, 0)		

		raise AirflowSkipException("Canciones actualizadas")

	kwarg["ti"].xcom_push(key="data_extraida", value=canciones)

	print("Extraccion correcta")

	kwarg["ti"].xcom_push(key="inicio", value=True if salto==0 else False)

	kwarg["ti"].xcom_push(key="canciones_extraidas", value=len(canciones))

# Funcion para transformar los datos de la API de Spotify
def transformacion(**kwarg)->None:

	data=kwarg["ti"].xcom_pull(key="data_extraida", task_ids="extraccion_data")

	# Funcion para limpiar la cancion
	def limpiarCancion(cancion:Dict)->tuple:

		id_cancion=cancion["track"]["id"]

		nombre=cancion["track"]["name"]

		artistas=[artista["name"] for artista in cancion["track"]["artists"]]

		artistas_cadena=", ".join(artistas)

		album=cancion["track"]["album"]["name"]

		fecha=cancion["added_at"].split("T")[0]

		return id_cancion, nombre, artistas_cadena, album, fecha

	canciones=list(map(limpiarCancion, data))

	kwarg["ti"].xcom_push(key="data_transformada", value=canciones)

	print("Transformacion correcta")

# Funcion para cargar los datos de la API de Spotify
def carga(hook:PostgresHook=crearHook(), **kwarg)->None:

	data=kwarg["ti"].xcom_pull(key="data_transformada", task_ids="transformacion_data")

	canciones=[tuple(cancion) for cancion in data]

	canciones_anadidas=0

	for cancion in canciones:

		print(f"Insertando la cancion '{cancion[1]}'...")

		try:

			hook.run(f"INSERT INTO {TABLA} VALUES %s", parameters=(cancion,))

			canciones_anadidas+=1		

		except:

			print(f"Error en la insercion de la cancion '{cancion[1]}' con ID '{cancion[0]}'")
		
	print("Carga correcta")

	inicio=kwarg["ti"].xcom_pull(key="inicio", task_ids="extraccion_data")

	canciones_extraidas=kwarg["ti"].xcom_pull(key="canciones_extraidas", task_ids="extraccion_data")

	crearLogs(inicio, canciones_extraidas, canciones_anadidas)


with DAG("spotify_dag", start_date=datetime(2023,12,18), description="DAG para obtener datos de la API de Spotify",
			schedule_interval=timedelta(days=7), catchup=False) as dag:

	comprobacion_tabla=BranchPythonOperator(task_id="existe_tabla", python_callable=existe_tabla)

	creacion_tabla=PythonOperator(task_id="creacion_tabla", python_callable=crearTabla)

	no_creacion_tabla=PythonOperator(task_id="no_creacion_tabla", python_callable=noCrearTabla)

	extraccion_data=PythonOperator(task_id="extraccion_data", python_callable=extraccion, trigger_rule="none_failed_min_one_success")

	transformacion_data=PythonOperator(task_id="transformacion_data", python_callable=transformacion)

	carga_data=PythonOperator(task_id="carga_data", python_callable=carga)

	comprobacion_tabla >> [creacion_tabla, no_creacion_tabla] >> extraccion_data >> transformacion_data >> carga_data