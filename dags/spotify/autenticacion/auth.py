import requests
import json

# Funcion para obtener el token
def generarToken(cliente_id:str, cliente_secreto:str)->str:

	url_token="https://accounts.spotify.com/api/token"

	data={"grant_type": "client_credentials", "client_id": cliente_id, "client_secret":cliente_secreto}

	respuesta=requests.post(url_token, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})

	contenido=json.loads(respuesta.content)

	return contenido["access_token"]