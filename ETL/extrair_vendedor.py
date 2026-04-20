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
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state="sessao_gigatech.json")

        pdf_container = []

        # Intercepta todas as requisições antes do browser consumir
        def handle_route(route):
            try:
                response = route.fetch()
                body = response.body()
                content_type = response.headers.get("content-type", "").lower()
                if "application/pdf" in content_type or (body and body[:4] == b"%PDF"):
                    print(f"✅ PDF interceptado! ({len(body)} bytes)")
                    pdf_container.append(body)
                route.fulfill(response=response)
            except Exception as e:
                print(f"⚠️ Erro no route: {e}")
                route.continue_()

        context.route("**/*", handle_route)

        page = context.new_page()
        page.set_default_navigation_timeout(120000)
        page.set_default_timeout(120000)

        print("Acessando o relatório...")
        page.goto(
            "https://app.mentorasolucoes.com.br/Voti-1.0.7/relatorios_vendas/frm_rel_vendas_vendedor.xhtml",
            wait_until="domcontentloaded"
        )
        page.locator("text='Imprimir'").wait_for(state="visible")

        print("Clicando em Imprimir e aguardando o servidor...")
        try:
            with context.expect_page() as nova_aba_info:
                page.locator("text='Imprimir'").click()
            nova_aba = nova_aba_info.value
            nova_aba.wait_for_load_state("networkidle", timeout=60000)
        except Exception as e:
            print(f"⚠️ {e}")

        page.wait_for_timeout(5000)

        if not pdf_container:
            print("❌ ERRO: Nenhum PDF interceptado.")
            browser.close()
            return

        arquivo_pdf_real = pdf_container[0]
        data_hoje = datetime.now().strftime('%Y-%m-%d')
        nome_arquivo = f"venda_vendedores_{data_hoje}.pdf"
        caminho_temporario = os.path.join(os.getcwd(), nome_arquivo)

        with open(caminho_temporario, "wb") as arquivo:
            arquivo.write(arquivo_pdf_real)

        print("Iniciando envio para a camada Bronze no MinIO...")
        bucket_name = 'marialimabronze'
        caminho_minio = f"venda_vendedores/{nome_arquivo}"

        try:
            s3_client.upload_file(caminho_temporario, bucket_name, caminho_minio)
            print(f"🚀 Sucesso absoluto! PDF salvo na nuvem.")
        except Exception as e:
            print(f"Erro ao salvar no MinIO: {e}")

        if os.path.exists(caminho_temporario):
            os.remove(caminho_temporario)

        browser.close()

if __name__ == "__main__":
    extrair_e_salvar_vendedor()