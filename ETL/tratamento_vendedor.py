import os
import boto3
import pdfplumber
import pandas as pd
import re
from datetime import datetime
from dotenv import load_dotenv

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

# A Expressão Regular para encontrar os dados de vendas
padrao_linha = re.compile(
    r"^(.*?)\s+"
    r"([A-Za-z\-]+)\s+"
    r"(\d{2}/\d{2}/\d{4})\s+"
    r"(\d+)\s+"
    r"(R\$\s*[\d\.,]+)\s+"
    r"(R\$\s*[\d\.,]+)\s+"
    r"(R\$\s*[\d\.,]+)$"
)

def processar_bronze_para_silver():
    data_hoje = datetime.now().strftime('%Y-%m-%d')
    nome_arquivo_pdf = f"venda_vendedores_{data_hoje}.pdf"
    nome_arquivo_parquet = f"venda_vendedores_{data_hoje}.parquet"

    bucket_bronze = 'marialimabronze'
    bucket_silver = 'marialimasilver' 
    
    caminho_minio_pdf = f"venda_vendedores/{nome_arquivo_pdf}"
    caminho_minio_parquet = f"venda_vendedores/{nome_arquivo_parquet}"
    
    caminho_temp_pdf = f"temp_{nome_arquivo_pdf}"
    caminho_temp_parquet = f"temp_{nome_arquivo_parquet}"

    print(f"📥 1. Baixando o PDF mais recente ({caminho_minio_pdf}) da camada Bronze...")
    try:
        s3_client.download_file(bucket_bronze, caminho_minio_pdf, caminho_temp_pdf)
    except Exception as e:
        print(f"❌ Erro ao baixar PDF do MinIO: {e}")
        return

    print("⚙️  2. Extraindo e estruturando os dados do PDF...")
    dados_estruturados = []
    
    vendedor_atual = "Desconhecido"
    esperando_vendedor = True  # O robô inicia esperando a primeira vendedora do relatório

    with pdfplumber.open(caminho_temp_pdf) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text(layout=True)
            if not texto: continue
            
            linhas = texto.split('\n')
            for linha in linhas:
                linha_limpa = linha.strip()
                if not linha_limpa: continue
                
                linha_upper = linha_limpa.upper()
                
                # Bateu no Total? Ativa a captura do próximo vendedor
                if "TOTAL VENDA:" in linha_upper:
                    esperando_vendedor = True
                    continue
                
                if esperando_vendedor:
                    linha_compacta = re.sub(r'\s+', ' ', linha_upper).strip()
                    
                    # Filtro 1: Ignora cabeçalhos e rodapés do relatório (Agora incluindo SOBRAL)
                    if any(x in linha_compacta for x in [
                        "SILVA E AGUIAR", "CNPJ", "BOULEVARD", "SOBRAL", "PERÍODO:", 
                        "RELATÓRIO", "TOTAL", "MARIA LIMA",
                        "CLIENTE", "TIPO DATA", "VALOR COMISSÃO"
                    ]):
                        continue
                    
                    # Filtro 2: Ignora datas e contatos
                    if re.match(r"^\d{2}/\d{2}/\d{4}$", linha_compacta) or "(88)" in linha_compacta or "@" in linha_compacta:
                        continue
                        
                    # Filtro 3: Ignora cabeçalho da tabela de vendas
                    if linha_compacta in ["VENDEDOR SUPERVISOR", "VENDEDOR", "SUPERVISOR"]:
                        continue
                        
                    # Captura a vendedora caso a linha não seja uma transação financeira
                    if not padrao_linha.match(linha_limpa):
                        vendedor_atual = re.split(r'\s{2,}', linha_limpa)[0].strip()
                        esperando_vendedor = False
                        print(f"🕵️ Nova Vendedora Identificada: {vendedor_atual}")
                        continue
                
                # Processamento normal das vendas
                match = padrao_linha.match(linha_limpa)
                if match:
                    dados_estruturados.append({
                        "vendedor": vendedor_atual,
                        "cliente": match.group(1).strip(),
                        "tipo_venda": match.group(2).strip(),
                        "data_venda": match.group(3).strip(),
                        "numero_venda": match.group(4).strip(),
                        "valor_total": match.group(5).strip(),
                        "comissao_vendedor": match.group(6).strip(),
                        "comissao_supervisor": match.group(7).strip()
                    })

    df = pd.DataFrame(dados_estruturados)
    print(f"   ✅ {len(df)} linhas estruturadas com sucesso!")

    print("💾 3. Salvando dados estruturados em Parquet...")
    df.to_parquet(caminho_temp_parquet, engine='pyarrow', index=False, compression='snappy')

    print(f"📤 4. Enviando Parquet para a camada Silver no MinIO ({bucket_silver}/{caminho_minio_parquet})...")
    try:
        s3_client.upload_file(caminho_temp_parquet, bucket_silver, caminho_minio_parquet)
        print("🚀 Sucesso absoluto! Pipeline Bronze -> Silver concluído.")
    except Exception as e:
        print(f"❌ Erro ao fazer upload do Parquet: {e}")

    print("🧹 5. Limpando arquivos temporários...")
    if os.path.exists(caminho_temp_pdf):
        os.remove(caminho_temp_pdf)
    if os.path.exists(caminho_temp_parquet):
        os.remove(caminho_temp_parquet)
    print("✨ Processo finalizado com sucesso!")

if __name__ == "__main__":
    processar_bronze_para_silver()