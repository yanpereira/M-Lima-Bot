# 🤖 ETL Bot - Maria Lima

Pipeline automatizado de extração e transformação de dados em Python, utilizando arquitetura Medalhão (Bronze/Silver) com armazenamento em MinIO S3.

## 🏗️ Arquitetura do Projeto
1. **Extração (Scraping):** Navegação automatizada e interceptação de tráfego de rede via `Playwright` para baixar relatórios diretamente do sistema ERP.
2. **Data Lake (Bronze):** Os dados brutos (PDFs, XLSX) são enviados imediatamente para o bucket `marialimabronze` no MinIO utilizando `boto3`.
3. **Transformação (Silver):** Utilização de `pdfplumber`, `Regex` e `Pandas` para estruturar dados não-tabulares e extrair hierarquias complexas.
4. **Data Lake (Silver):** Dados estruturados e limpos são salvos em CSV/Parquet no bucket `marialimasilver`, prontos para consumo por ferramentas de BI.

## 🛠️ Tecnologias Utilizadas
* **Python 3**
* **Playwright** (Web Automation & Network Interception)
* **Boto3** (Integração S3/MinIO)
* **Pandas & PDFPlumber** (Data Wrangling)
* **Docker** (Conteinerização)

## 🔒 Segurança
As credenciais do MinIO e logins do sistema são gerenciados via variáveis de ambiente (`.env`), garantindo que nenhuma informação sensível seja exposta no código-fonte.