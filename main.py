import os
import json
import requests
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

def executar_pipeline():
    try:
        # 1. Carregar as credenciais
        google_secrets = os.getenv("GOOGLE_CREDENTIALS")
        if not google_secrets:
            raise ValueError("Secret GOOGLE_CREDENTIALS não encontrada!")
        
        creds_dict = json.loads(google_secrets)
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        gc = gspread.authorize(creds)

        # 2. EXTRAÇÃO: Busca o preço em USD
        url_api = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
        response = requests.get(url_api, timeout=10)
        response.raise_for_status()
        preco_usd = response.json()['bitcoin']['usd']

        # 3. TRANSFORMAÇÃO: A inteligência do Analista
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        # Exemplo de conversão (Poderia vir de outra API, mas aqui usamos uma taxa fixa para o estudo)
        taxa_cambio_brl = 5.15 
        preco_brl = preco_usd * taxa_cambio_brl
        
        # Lógica de Alerta/Status
        if preco_usd > 65000:
            status = "ALTA CRÍTICA"
        elif preco_usd < 55000:
            status = "OPORTUNIDADE"
        else:
            status = "ESTÁVEL"

        # 4. CARGA: Salva os dados transformados
        SPREADSHEET_NAME = 'Estudo_Integração_Pipiline'
        sh = gc.open(SPREADSHEET_NAME)
        aba_historico = sh.worksheet("Historico_Precos")
        
        # A linha agora vai com os dados enriquecidos: [Data, ID, USD, BRL, Status]
        aba_historico.append_row([agora, 1, preco_usd, preco_brl, status])
        
        print(f"✅ Sucesso! Gravado: USD {preco_usd} | BRL {preco_brl:.2f} | Status: {status}")

    except Exception as e:
        print(f"❌ Erro no Pipeline: {e}")
        exit(1)

if __name__ == "__main__":
    executar_pipeline()
