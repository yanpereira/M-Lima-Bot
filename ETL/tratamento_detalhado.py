import os
import boto3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import re

# 1. Carrega as variáveis do seu arquivo .env
load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

# 2. Configura a conexão com o MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=f'https://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)

def limpar_nome_colunas(df):
    """Padroniza os nomes das colunas para o formato snake_case"""
    novas_colunas = []
    for col in df.columns:
        # Remove espaços no início/fim, coloca minúsculo e troca espaços internos por underline
        nome_limpo = str(col).strip().lower()
        nome_limpo = re.sub(r'[^a-z0-9]', '_', nome_limpo) # Troca caracteres especiais por _
        nome_limpo = re.sub(r'_+', '_', nome_limpo) # Remove underlines duplicados
        nome_limpo = nome_limpo.strip('_')
        novas_colunas.append(nome_limpo)
    df.columns = novas_colunas
    return df

def processar_bronze_para_silver_detalhado():
    # Define os nomes dos arquivos baseado no dia da rodada
    data_hoje = datetime.now().strftime('%Y-%m-%d')
    nome_arquivo_xlsx = f"venda_detalhada_{data_hoje}.xlsx"
    nome_arquivo_parquet = f"venda_detalhada_{data_hoje}.parquet"

    # Define os caminhos nos buckets
    bucket_bronze = 'marialimabronze'
    bucket_silver = 'marialimasilver'
    
    caminho_minio_xlsx = f"venda_detalhado/{nome_arquivo_xlsx}"
    caminho_minio_parquet = f"venda_detalhado/{nome_arquivo_parquet}"
    
    # Nomes dos arquivos temporários locais
    caminho_temp_xlsx = f"temp_{nome_arquivo_xlsx}"
    caminho_temp_parquet = f"temp_{nome_arquivo_parquet}"

    print(f"📥 1. Baixando o XLSX mais recente ({caminho_minio_xlsx}) da camada Bronze...")
    try:
        s3_client.download_file(bucket_bronze, caminho_minio_xlsx, caminho_temp_xlsx)
    except Exception as e:
        print(f"❌ Erro ao baixar XLSX do MinIO: {e}")
        return

    print("⚙️  2. Lendo o arquivo Excel e padronizando os dados...")
    # Lê o Excel usando o motor openpyxl
    df = pd.read_excel(caminho_temp_xlsx, engine='xlrd')
    
    # Padroniza os nomes das colunas para facilitar consultas analíticas
    df = limpar_nome_colunas(df)
    print(f"   ✅ {len(df)} linhas lidas com sucesso. Colunas padronizadas.")

    print("💾 3. Convertendo e salvando em formato Parquet...")
    # Salva localmente em Parquet usando compressão (snappy é excelente para Big Data)
    df.to_parquet(caminho_temp_parquet, engine='pyarrow', index=False, compression='snappy')

    print(f"📤 4. Enviando Parquet para a camada Silver ({bucket_silver}/{caminho_minio_parquet})...")
    try:
        s3_client.upload_file(caminho_temp_parquet, bucket_silver, caminho_minio_parquet)
        print("🚀 Sucesso absoluto! Pipeline Detalhado Bronze -> Silver concluído.")
    except Exception as e:
        print(f"❌ Erro ao fazer upload do Parquet: {e}")

    print("🧹 5. Limpando arquivos temporários...")
    if os.path.exists(caminho_temp_xlsx):
        os.remove(caminho_temp_xlsx)
    if os.path.exists(caminho_temp_parquet):
        os.remove(caminho_temp_parquet)
    print("✨ Processo finalizado com sucesso!")

if __name__ == "__main__":
    processar_bronze_para_silver_detalhado()