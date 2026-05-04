import argparse
import os
import boto3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from utils import prefixo_perfil

load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

s3_client = boto3.client(
    's3',
    endpoint_url=f'https://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)


def processar_ouro_vendas(perfil: str):
    data_hoje = datetime.now().strftime('%Y-%m-%d')

    # ⚠️ IMPORTANTE: Crie esse bucket 'marialimagold' no seu MinIO se ainda não existir!
    bucket_silver = 'marialimasilver'
    bucket_gold = 'marialimagold' 

    caminho_minio_silver = f"venda_detalhado/{prefixo_perfil(perfil)}venda_detalhada_{data_hoje}.parquet"
    caminho_minio_master = f"venda_detalhado/{prefixo_perfil(perfil)}master_vendas_detalhada.parquet"

    # Arquivos temporários
    caminho_local_silver = f"temp_silver_{data_hoje}.parquet"
    caminho_local_master = "temp_master.parquet"

    print(f"📥 1. Baixando arquivo Silver do dia atual ({caminho_minio_silver})...")
    try:
        s3_client.download_file(bucket_silver, caminho_minio_silver, caminho_local_silver)
        df_silver_hoje = pd.read_parquet(caminho_local_silver)
    except Exception as e:
        print(f"❌ Erro: Arquivo Silver não encontrado. Detalhes: {e}")
        return

    print("⚙️ 2. Verificando existência do Arquivo Mestre na camada Ouro...")
    try:
        s3_client.download_file(bucket_gold, caminho_minio_master, caminho_local_master)
        df_master = pd.read_parquet(caminho_local_master)
        arquivo_mestre_existe = True
        print("   ✅ Arquivo Mestre encontrado. Iniciando mesclagem...")
    except Exception:
        print("   ⚠️ Arquivo Mestre não encontrado (primeira execução). Criando um novo...")
        df_master = pd.DataFrame()
        arquivo_mestre_existe = False

    if arquivo_mestre_existe:
        coluna_data = 'data'

        tamanho_antes = len(df_master)

        # Remove do master TODAS as datas presentes no Silver (não apenas hoje),
        # garantindo idempotência mesmo quando o Silver contém mais de um dia.
        datas_no_silver = set(df_silver_hoje[coluna_data].astype(str).unique())
        df_master = df_master[~df_master[coluna_data].astype(str).isin(datas_no_silver)]

        linhas_removidas = tamanho_antes - len(df_master)
        print(f"   🧹 Removendo {linhas_removidas} linhas das datas {datas_no_silver} do master.")

    print("🔄 3. Consolidando os dados...")
    df_ouro = pd.concat([df_master, df_silver_hoje], ignore_index=True)

    print(f"💾 4. Salvando Mestre Ouro com {len(df_ouro)} linhas no total...")
    # Salvando com snappy, mantendo o padrão do seu Silver
    df_ouro.to_parquet(caminho_local_master, index=False, engine='pyarrow', compression='snappy')
    
    try:
        s3_client.upload_file(caminho_local_master, bucket_gold, caminho_minio_master)
        print(f"🚀 Sucesso absoluto! Camada Ouro atualizada em {bucket_gold}/{caminho_minio_master}")
    except Exception as e:
        print(f"❌ Erro ao enviar para o MinIO: {e}")

    print("🧹 5. Limpando arquivos temporários...")
    if os.path.exists(caminho_local_silver): 
        os.remove(caminho_local_silver)
    if os.path.exists(caminho_local_master): 
        os.remove(caminho_local_master)

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--perfil", default="default")
    args = parser.parse_args()

    processar_ouro_vendas(args.perfil)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())