from playwright.sync_api import sync_playwright
import os
import boto3
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

def extrair_e_salvar_direto():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            storage_state="sessao_gigatech.json", 
            accept_downloads=True
        )
        page = context.new_page()
        page.set_default_navigation_timeout(120000)
        page.set_default_timeout(120000)

        print("Acessando o relatório...")
        page.goto(
            "https://app.mentorasolucoes.com.br/Voti-1.0.7/relatorios_vendas/frm_rel_venda_detalhada_novo.xhtml",
            wait_until="domcontentloaded"
        )

        print("Aguardando o botão de exportar...")
        botao_exportar = page.locator("button:has-text('Exportar Xlsx')")
        botao_exportar.wait_for(state="visible")
        with page.expect_download(timeout=120000) as download_info:
            botao_exportar.click()
        
        # 3. Define o nome do arquivo com a data de hoje (Ex: venda_detalhada_2026-03-06.xlsx)
        data_hoje = datetime.now().strftime('%Y-%m-%d')
        nome_arquivo = f"venda_detalhada_{data_hoje}.xlsx"
        
        # Salva o arquivo temporariamente na mesma pasta do script
        caminho_temporario = os.path.join(os.getcwd(), nome_arquivo)
        download_info.value.save_as(caminho_temporario)
        print("Download do sistema concluído. Iniciando envio para o MinIO...")
        
        # 4. Envia para a pasta venda_detalhado dentro do bucket marialimabronze
        bucket_name = 'marialimabronze'
        caminho_minio = f"venda_detalhado/{nome_arquivo}"
        
        try:
            s3_client.upload_file(caminho_temporario, bucket_name, caminho_minio)
            print(f"Sucesso absoluto! Arquivo salvo direto no MinIO em: {caminho_minio}")
        except Exception as e:
            print(f"Erro ao salvar no MinIO: {e}")
            
        # 5. Apaga o arquivo temporário do computador/VPS para manter tudo limpo
        if os.path.exists(caminho_temporario):
            os.remove(caminho_temporario)
            print("Arquivo temporário local apagado.")

        browser.close()

if __name__ == "__main__":
    extrair_e_salvar_direto()