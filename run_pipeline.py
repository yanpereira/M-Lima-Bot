import subprocess
import sys

scripts = [
    "ETL/login.py",
    "ETL/extrair_detalhado.py",
    "ETL/extrair_vendedor.py",
    "ETL/tratamento_detalhado.py",
    "ETL/tratamento_vendedor.py",
    "ETL/processar_ouro_detalhado.py",
    "ETL/processar_ouro_vendedor.py"
]

for script in scripts:
    print(f"--- Iniciando: {script} ---")
    result = subprocess.run([sys.executable, script])
    if result.returncode != 0:
        print(f"Erro ao executar {script}. Interrompendo pipeline.")
        break
    print(f"--- Finalizado com sucesso: {script} ---\n")