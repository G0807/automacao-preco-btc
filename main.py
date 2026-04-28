import os
import json
import requests
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

# 1. Carregar as credenciais da 'Secret' do GitHub
google_secrets = os.getenv("GOOGLE_CREDENTIALS")
if not google_secrets:
    raise ValueError("A Secret GOOGLE_CREDENTIALS não foi encontrada!")

creds_dict = json.loads(google_secrets)

# 2. Configurar o acesso
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
gc = gspread.authorize(creds)

# 3. Definições
SPREADSHEET_NAME = 'Estudo_Integração_Pipiline' # Certifique-se que o nome está idêntico à sua planilha
url_api = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

try:
    # EXTRAÇÃO
    response = requests.get(url_api, timeout=10)
    response.raise_for_status()
    preco_usd = response.json()['bitcoin']['usd']
    
    # TRANSFORMAÇÃO (Lógica simples de negócio)
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    status = "ALTA" if preco_usd > 65000 else "ESTÁVEL"
    
    # CARGA
    sh = gc.open(SPREADSHEET_NAME)
    aba_historico = sh.worksheet("Historico_Precos") # Nome da aba na planilha
    aba_historico.append_row([agora, 1, preco_usd, status])
    
    print(f"✅ Sucesso: {agora} | BTC: ${preco_usd}")

except Exception as e:
    print(f"❌ Erro no Pipeline: {e}")
    exit(1)
