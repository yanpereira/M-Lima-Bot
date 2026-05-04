import argparse
import os
from datetime import datetime

import boto3
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from utils import prefixo_perfil, sessao_path

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


def extrair_e_salvar_direto(perfil: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=sessao_path(perfil),
            accept_downloads=True
        )
        page = context.new_page()
        page.set_default_navigation_timeout(120000)
        page.set_default_timeout(120000)

        print(f"Acessando o relatório ({perfil})...")
        page.goto(
            "https://app.mentorasolucoes.com.br/Voti-1.0.7/relatorios_vendas/frm_rel_venda_detalhada_novo.xhtml",
            wait_until="domcontentloaded"
        )

        print("Aguardando o botão de exportar...")
        botao_exportar = page.locator("button:has-text('Exportar Xlsx')")
        botao_exportar.wait_for(state="visible")

        print("Definindo filtro de data para hoje...")
        data_hoje_fmt = datetime.now().strftime('%d/%m/%Y')
        page.evaluate("""
            (dateStr) => {
                document.querySelectorAll('input[type="text"]').forEach(inp => {
                    if (/^\\d{2}\\/\\d{2}\\/\\d{4}$/.test(inp.value)) {
                        inp.value = dateStr;
                        ['input', 'change', 'blur'].forEach(ev =>
                            inp.dispatchEvent(new Event(ev, {bubbles: true}))
                        );
                    }
                });
            }
        """, data_hoje_fmt)
        page.wait_for_timeout(2000)

        with page.expect_download(timeout=300000) as download_info:
            botao_exportar.click()

        data_hoje = datetime.now().strftime('%Y-%m-%d')
        nome_arquivo = f"venda_detalhada_{data_hoje}.xlsx"

        caminho_temporario = os.path.join(os.getcwd(), nome_arquivo)
        download_info.value.save_as(caminho_temporario)
        print("Download do sistema concluído. Iniciando envio para o MinIO...")

        bucket_name = 'marialimabronze'
        caminho_minio = f"venda_detalhado/{prefixo_perfil(perfil)}{nome_arquivo}"

        try:
            s3_client.upload_file(caminho_temporario, bucket_name, caminho_minio)
            print(f"Sucesso absoluto! Arquivo salvo direto no MinIO em: {caminho_minio}")
        except Exception as e:
            print(f"Erro ao salvar no MinIO: {e}")

        if os.path.exists(caminho_temporario):
            os.remove(caminho_temporario)
            print("Arquivo temporário local apagado.")

        browser.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--perfil", default="default")
    args = parser.parse_args()

    extrair_e_salvar_direto(args.perfil)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())