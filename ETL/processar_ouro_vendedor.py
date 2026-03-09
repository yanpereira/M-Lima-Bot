import os
import boto3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# 1. Carrega as variáveis do .env
load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

# 2. Configura a conexão usando a lógica que validamos
s3_client = boto3.client(
    's3',
    endpoint_url=f'https://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)

def processar_ouro_vendedores():
    # Pega as datas com os tracinhos para o nome do arquivo
    data_hoje = datetime.now().strftime('%Y-%m-%d')
    # Formato de data capturado pelo Regex do PDF (DD/MM/YYYY) para filtrarmos
    data_filtro_str = datetime.now().strftime('%d/%m/%Y') 

    bucket_silver = 'marialimasilver'
    bucket_gold = 'marialimagold' 

    # Caminhos no MinIO (Pasta venda_vendedores)
    caminho_minio_silver = f"venda_vendedores/venda_vendedores_{data_hoje}.parquet"
    caminho_minio_master = "venda_vendedores/master_venda_vendedores.parquet"

    # Arquivos temporários (com nomes específicos para não conflitar com o outro script)
    caminho_local_silver = f"temp_silver_vend_{data_hoje}.parquet"
    caminho_local_master = "temp_master_vend.parquet"

    print(f"📥 1. Baixando arquivo Silver do dia atual ({caminho_minio_silver})...")
    try:
        s3_client.download_file(bucket_silver, caminho_minio_silver, caminho_local_silver)
        df_silver_hoje = pd.read_parquet(caminho_local_silver)
    except Exception as e:
        print(f"❌ Erro: Arquivo Silver não encontrado. O script de tratamento rodou? Detalhes: {e}")
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
        # Coluna que criamos no script de tratamento da Silver
        coluna_data = 'data_venda' 
        
        tamanho_antes = len(df_master)
        
        # Converte a coluna para string DD/MM/YYYY de forma segura para comparar
        datas_como_string = pd.to_datetime(df_master[coluna_data], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
        
        # Remove as linhas do dia de hoje (Garante a Idempotência das parciais)
        df_master = df_master[datas_como_string != data_filtro_str]
        
        linhas_removidas = tamanho_antes - len(df_master)
        print(f"   🧹 Limpando parciais antigas: {linhas_removidas} linhas de hoje removidas.")

    print("🔄 3. Consolidando os dados...")
    df_ouro = pd.concat([df_master, df_silver_hoje], ignore_index=True)

    print(f"💾 4. Salvando Mestre Ouro com {len(df_ouro)} linhas no total...")
    # Salvando com snappy para manter o padrão de performance
    df_ouro.to_parquet(caminho_local_master, index=False, engine='pyarrow', compression='snappy')
    
    try:
        # Agora sim, a linha certa e única para o upload:
        s3_client.upload_file(caminho_local_master, bucket_gold, caminho_minio_master)
        print(f"🚀 Sucesso absoluto! Camada Ouro atualizada em {bucket_gold}/{caminho_minio_master}")
    except Exception as e:
        print(f"❌ Erro ao enviar para o MinIO: {e}")

    print("🧹 5. Limpando arquivos temporários...")
    if os.path.exists(caminho_local_silver): 
        os.remove(caminho_local_silver)
    if os.path.exists(caminho_local_master): 
        os.remove(caminho_local_master)
    print("✨ Processo finalizado com sucesso!")

if __name__ == "__main__":
    processar_ouro_vendedores()