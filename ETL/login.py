import argparse
import os

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from utils import env_por_perfil, sessao_path

load_dotenv()

URL_LOGIN = (os.getenv("GIGATECH_URL") or "").strip().strip("`\"'")
if URL_LOGIN:
    URL_LOGIN = URL_LOGIN.split(";jsessionid=")[0]


def realizar_login(perfil: str):
    usuario = env_por_perfil("GIGATECH_USER", perfil)
    senha = env_por_perfil("GIGATECH_PASS", perfil)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.set_default_navigation_timeout(120000)
        page.set_default_timeout(120000)

        print(f"Acessando a página de login ({perfil})...")
        page.goto(URL_LOGIN, wait_until="domcontentloaded")

        print("Preenchendo usuário e senha...")
        page.locator("input[type='text']").fill(usuario)
        page.locator("input[type='password']").fill(senha)

        page.locator("button:has-text('Entrar')").click()

        print("Aguardando carregamento da página inicial...")
        page.wait_for_load_state("networkidle")
        print("Login aparentemente realizado com sucesso!")

        caminho_sessao = sessao_path(perfil)
        context.storage_state(path=caminho_sessao)
        print(f"Sessão salva no arquivo {caminho_sessao}!")

        browser.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--perfil", default="default")
    args = parser.parse_args()

    realizar_login(args.perfil)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())