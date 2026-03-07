# 🤖 Robô Maria Lima - Pipeline ETL & Data Lake

Este repositório contém o pipeline automatizado de extração, tratamento e consolidação de dados de vendas do sistema GigaTech. O projeto utiliza **Web Scraping** para extração de relatórios e a **Arquitetura Medalhão (Medallion Architecture)** para processamento e armazenamento em um Data Lake (MinIO).

## 🏗️ Arquitetura do Projeto

O fluxo de dados segue o padrão **Bronze ➔ Silver ➔ Gold**, garantindo rastreabilidade, qualidade e performance para o consumo das ferramentas de Business Intelligence (BI).

1. **Camada de Extração (Scraping & Automação):**
   - Utiliza `Playwright` para simular a navegação humana, realizar login com gestão de sessão (cookies) e baixar os relatórios bruto do dia.
2. **Camada Bronze (Raw Data):**
   - Os arquivos brutos (XLSX e PDF) extraídos são enviados diretamente para o bucket `marialimabronze` no MinIO, servindo como histórico imutável da origem.
3. **Camada Silver (Clean & Standardized Data):**
   - Scripts utilizam `Pandas` para ler os arquivos da Bronze, padronizar nomes de colunas (`snake_case`), tipar os dados e salvá-los no formato otimizado **Parquet** (compressão Snappy). Os dados são enviados ao bucket `marialimasilver`.
4. **Camada Gold (Consolidated & Business Level):**
   - Realiza o *Merge* dos dados diários com o **Arquivo Mestre Histórico**.
   - **Idempotência:** Como o robô roda em parciais diárias (ex: 10h, 14h, 18h), a camada Gold identifica e remove as execuções parciais do dia corrente antes de inserir o novo lote, evitando duplicidade de faturamento. Arquivo final salvo no bucket `marialimagold`.

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3
* **Automação Web:** Playwright
* **Processamento de Dados:** Pandas, PyArrow
* **Data Lake / Storage:** MinIO (S3 Compatible) via biblioteca `boto3`
* **Variáveis de Ambiente:** python-dotenv
