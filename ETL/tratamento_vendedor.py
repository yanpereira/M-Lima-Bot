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
    # Define o nome dos arquivos baseado no dia da rodada
    data_hoje = datetime.now().strftime('%Y-%m-%d')
    nome_arquivo_pdf = f"venda_vendedores_{data_hoje}.pdf"
    nome_arquivo_csv = f"venda_vendedores_{data_hoje}.csv"

    # Define os caminhos nos buckets
    bucket_bronze = 'marialimabronze'
    bucket_silver = 'marialimasilver' # Usando o bucket silver que vi no seu painel
    
    caminho_minio_pdf = f"venda_vendedores/{nome_arquivo_pdf}"
    caminho_minio_csv = f"venda_vendedores/{nome_arquivo_csv}"
    
    # Nomes dos arquivos temporários locais
    caminho_temp_pdf = f"temp_{nome_arquivo_pdf}"
    caminho_temp_csv = f"temp_{nome_arquivo_csv}"

    print(f"📥 1. Baixando o PDF mais recente ({caminho_minio_pdf}) da camada Bronze...")
    try:
        s3_client.download_file(bucket_bronze, caminho_minio_pdf, caminho_temp_pdf)
    except Exception as e:
        print(f"Erro ao baixar PDF do MinIO: {e}")
        return # Para a execução se o PDF não for encontrado

    print("⚙️  2. Extraindo e estruturando os dados do PDF...")
    dados_estruturados = []
    vendedor_atual = "Desconhecido"

    with pdfplumber.open(caminho_temp_pdf) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text(layout=True)
            if not texto: continue
            
            linhas = texto.split('\n')
            for i, linha in enumerate(linhas):
                linha = linha.strip()
                if not linha: continue
                
                # Identifica o Vendedor
                if "Vendedor" in linha and "Supervisor" in linha and "Total" not in linha:
                    try:
                        linha_nomes = linhas[i+1].strip()
                        vendedor_atual = re.split(r'\s{2,}', linha_nomes)[0].strip()
                    except IndexError:
                        pass
                    continue
                
                # Captura os dados da venda usando o Regex
                match = padrao_linha.match(linha)
                if match:
                    dados_estruturados.append({
                        "Vendedor": vendedor_atual,
                        "Cliente": match.group(1).strip(),
                        "Tipo_Venda": match.group(2).strip(),
                        "Data_Venda": match.group(3).strip(),
                        "Numero_Venda": match.group(4).strip(),
                        "Valor_Total": match.group(5).strip(),
                        "Comissao_Vendedor": match.group(6).strip(),
                        "Comissao_Supervisor": match.group(7).strip()
                    })

    df = pd.DataFrame(dados_estruturados)
    print(f"   ✅ {len(df)} linhas estruturadas com sucesso!")

    print("💾 3. Salvando dados estruturados em CSV...")
    # Salva localmente em CSV (sem o índice da linha e formatado em UTF-8)
    df.to_csv(caminho_temp_csv, index=False, encoding='utf-8')

    print(f"📤 4. Enviando CSV para a camada Silver no MinIO ({bucket_silver}/{caminho_minio_csv})...")
    try:
        s3_client.upload_file(caminho_temp_csv, bucket_silver, caminho_minio_csv)
        print("🚀 Sucesso absoluto! Pipeline Bronze -> Silver concluído.")
    except Exception as e:
        print(f"Erro ao fazer upload do CSV: {e}")

    print("🧹 5. Limpando arquivos temporários...")
    if os.path.exists(caminho_temp_pdf):
        os.remove(caminho_temp_pdf)
    if os.path.exists(caminho_temp_csv):
        os.remove(caminho_temp_csv)
    print("✨ Processo finalizado com sucesso!")

if __name__ == "__main__":
    processar_bronze_para_silver()