import argparse
import os
import subprocess
import sys

from dotenv import load_dotenv

load_dotenv()

os.environ.setdefault("PYTHONUTF8", "1")


def _parse_lista(valor: str | None) -> list[str]:
    if not valor:
        return []
    partes = [p.strip() for p in valor.split(",")]
    return [p for p in partes if p]


def _run(cmd: list[str]) -> int:
    print(f"--- Iniciando: {' '.join(cmd[1:])} ---")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"Erro ao executar {' '.join(cmd[1:])}. Interrompendo pipeline.")
    else:
        print(f"--- Finalizado com sucesso: {' '.join(cmd[1:])} ---\n")
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--perfis", default=os.getenv("GIGATECH_PROFILES"))
    args = parser.parse_args()

    perfis = _parse_lista(args.perfis) or ["default"]

    for perfil in perfis:
        print(f"=== Perfil {perfil} ===")

        if _run([sys.executable, "ETL/login.py", "--perfil", perfil]) != 0:
            return 1

        scripts = [
            [sys.executable, "ETL/extrair_detalhado.py", "--perfil", perfil],
            [sys.executable, "ETL/extrair_vendedor.py", "--perfil", perfil],
            [sys.executable, "ETL/tratamento_detalhado.py", "--perfil", perfil],
            [sys.executable, "ETL/tratamento_vendedor.py", "--perfil", perfil],
            [sys.executable, "ETL/processar_ouro_detalhado.py", "--perfil", perfil],
            [sys.executable, "ETL/processar_ouro_vendedor.py", "--perfil", perfil],
        ]

        for cmd in scripts:
            if _run(cmd) != 0:
                return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())