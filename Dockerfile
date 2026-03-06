# Usa a imagem oficial do Playwright que já tem o Chromium embutido e pronto para uso
FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia a lista de dependências e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do seu projeto para dentro do container
COPY . .

# Comando padrão ao rodar o container (pode ser sobrescrito pelo Kestra depois)
CMD ["python", "extrair_vendedor.py"]