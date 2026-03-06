import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Puxa as informações do .env
URL_LOGIN = os.getenv("GIGATECH_URL")
USUARIO = os.getenv("GIGATECH_USER")
SENHA = os.getenv("GIGATECH_PASS")

def realizar_login():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("Acessando a página de login...")
        page.goto(URL_LOGIN)

        print("Preenchendo usuário e senha...")
        # ATENÇÃO: Talvez seja necessário ajustar os seletores abaixo
        page.locator("input[type='text']").fill(USUARIO)
        page.locator("input[type='password']").fill(SENHA)
        
        # Pode ser que o botão não tenha o texto exato "Entrar" (pode ser "Login", "Acessar")
        page.locator("button:has-text('Entrar')").click()

        print("Aguardando carregamento da página inicial...")
        # Espera a página carregar após o login
        page.wait_for_load_state("networkidle")
        print("Login aparentemente realizado com sucesso!")

        # Salva a sessão
        context.storage_state(path="sessao_gigatech.json")
        print("Sessão salva no arquivo sessao_gigatech.json!")

        browser.close()

if __name__ == "__main__":
    realizar_login()