from airflow.providers.postgres.hooks.postgres import PostgresHook

def crearHook()->PostgresHook:

	"""
	Configuracion de la conexion con Postgres

	Connection Id: conexion_postgres
	Connection Type: Postgres
	Host: postgres
	Database: airflow
	Login: airflow
	Password: airflow
	Post: 5432
	"""
	return PostgresHook(postgres_conn_id="conexion_postgres")