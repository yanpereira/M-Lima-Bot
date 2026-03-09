import pdfplumber
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Conecta no MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{os.getenv('MINIO_ENDPOINT')}",
    aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
    region_name='us-east-1'
)

data_hoje = datetime.now().strftime('%Y-%m-%d')
nome_arquivo = f"venda_vendedores_{data_hoje}.pdf"
caminho_minio = f"venda_vendedores/{nome_arquivo}"
caminho_local = "temp_raiox.pdf"

print(f"Baixando {nome_arquivo} direto da nuvem (Bronze)...")
s3_client.download_file('marialimabronze', caminho_minio, caminho_local)

print("Lendo as entranhas do PDF...\n")
with pdfplumber.open(caminho_local) as pdf:
    # Pega só a primeira página
    pagina = pdf.pages[0]
    texto = pagina.extract_text(layout=True)
    
    linhas = texto.split('\n')
    print("--- RAIO-X DO PDF ---")
    for i, linha in enumerate(linhas[:35]): # Puxando 35 linhas para garantir
        print(f"Linha {i}: '{linha}'")

# Limpa a sujeira
if os.path.exists(caminho_local):
    os.remove(caminho_local)