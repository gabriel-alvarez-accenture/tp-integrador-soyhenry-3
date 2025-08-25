import time
import streamlit as st
import requests
from airbyte_api import AirbyteAPI
from airbyte_api.models import Security
from airbyte_api.api import getconnection
from airbyte_api.models.jobcreaterequest import JobCreateRequest


# Configuraciones
CLIENT_ID = "7f21e084-d530-47bb-968a-49e9bd02d2ed"
CLIENT_SECRET = "c4xwmHqrTl3eOKGyefRHC8tvY64fiH9x"
WORKSPACE_ID = "423df4bb-cdd0-4be5-8862-affe7f59c2c4"
BASE_URL = "https://api.airbyte.com"
connection_id = "8761ba9a-8e98-49ea-8a98-ca3ee3fe4243"

def refresh_airbyte_token():
    try:
        last_refresh = st.session_state.get('last_token_refresh', 0)
        if time.time() - last_refresh < 120:
            return st.session_state.get('current_token')

        st.info("Obteniendo nuevo access token...")

        response = requests.post(
            f"{BASE_URL}/v1/applications/token",
            json={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            }
        )
        response.raise_for_status()

        new_token = response.json().get("access_token")
        if not new_token:
            raise ValueError("No se recibió access_token en la respuesta")

        # Inicializar cliente Airbyte con el nuevo token
        client = AirbyteAPI(security=Security(bearer_auth=new_token))

        # Guardar en session_state
        st.session_state.client = client
        st.session_state.last_token_refresh = time.time()
        st.session_state.current_token = new_token

        client = st.session_state.client
        try:
            get_request = getconnection.GetConnectionRequest(connection_id=connection_id)
            
            connection = client.connections.get_connection(request=get_request)
            
            st.write("Conexión obtenida exitosamente:")
            
        except Exception as e:
            st.error(f"Error al obtener la conexión: {e}")



    except Exception as e:
        st.error(f"Error al refrescar el token: {str(e)}")
        raise


def run_connection_job():
    """
    Inicia una nueva sincronización para la conexión especificada.
    """
    if 'client' not in st.session_state:
        st.error("El cliente de la API de Airbyte no está inicializado. Por favor, recarga la página.")
        return

    client = st.session_state.client
    
    st.info(f"Iniciando sincronización para la conexión: {connection_id}...")
    
    try:
        job_request = JobCreateRequest(
            connection_id=connection_id,
            job_type="sync"
        )
        
        job_response = client.jobs.create_job(request=job_request)
                
        st.success("Sincronización iniciada!")

    except Exception as e:
        st.error(f"Error al iniciar la sincronización: {e}")



def main():
    refresh_airbyte_token()
    run_connection_job()

if __name__ == "__main__":
    main()
