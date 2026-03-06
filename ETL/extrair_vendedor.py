from playwright.sync_api import sync_playwright
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

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

def extrair_e_salvar_vendedor():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(storage_state="sessao_gigatech.json")
        page = context.new_page()

        pdf_container = []

        # 🕵️ O SUPER ESPIÃO: Sem restrições. Se o servidor disser que é PDF, nós pegamos!
        def capturar_pdf(resposta):
            try:
                tipo_conteudo = resposta.headers.get("content-type", "").lower()
                if "application/pdf" in tipo_conteudo:
                    print(f"👀 Opa! Detectei um PDF trafegando na rede...")
                    corpo = resposta.body()
                    # Verifica a assinatura universal de PDFs
                    if corpo.startswith(b"%PDF"):
                        pdf_container.append(corpo)
                        print(f"✅ Arquivo PDF REAL capturado! (Tamanho: {len(corpo)} bytes)")
            except Exception:
                pass

        # Ativa o espião no contexto inteiro do navegador
        context.on("response", capturar_pdf)

        print("Acessando o relatório...")
        page.goto("https://app.mentorasolucoes.com.br/Voti-1.0.7/relatorios_vendas/frm_rel_vendas_vendedor.xhtml")
        page.wait_for_load_state("load")

        print("Clicando em Imprimir e aguardando o servidor...")
        
        with context.expect_page() as nova_aba_info:
            page.locator("text='Imprimir'").click()
            
        nova_aba = nova_aba_info.value
        
        # Damos tempo suficiente (15s) para o sistema da Mentora gerar e entregar o arquivo
        nova_aba.wait_for_timeout(15000)
        
        if not pdf_container:
            print("❌ ERRO: Nenhum PDF passou pela rede. O botão pode não ter disparado a extração corretamente.")
            browser.close()
            return
            
        # Pega o primeiro PDF válido que o espião capturou
        arquivo_pdf_real = pdf_container[0]
        
        data_hoje = datetime.now().strftime('%Y-%m-%d')
        nome_arquivo = f"venda_vendedores_{data_hoje}.pdf"
        caminho_temporario = os.path.join(os.getcwd(), nome_arquivo)
        
        # Salva o arquivo pesado de verdade
        with open(caminho_temporario, "wb") as arquivo:
            arquivo.write(arquivo_pdf_real)
            
        print("Iniciando envio para a camada Bronze no MinIO...")
        bucket_name = 'marialimabronze'
        caminho_minio = f"venda_vendedores/{nome_arquivo}"
        
        try:
            s3_client.upload_file(caminho_temporario, bucket_name, caminho_minio)
            print(f"🚀 Sucesso absoluto! PDF validado e salvo na nuvem.")
        except Exception as e:
            print(f"Erro ao salvar no MinIO: {e}")
            
        if os.path.exists(caminho_temporario):
            os.remove(caminho_temporario)

        browser.close()

if __name__ == "__main__":
    extrair_e_salvar_vendedor()