import os


def sanitizar_perfil(perfil: str) -> str:
    if not perfil or perfil == "default":
        return "default"
    return "".join(ch if ch.isalnum() else "_" for ch in perfil.lower())


def sessao_path(perfil: str) -> str:
    s = sanitizar_perfil(perfil)
    if s == "default":
        return "sessao_gigatech.json"
    return f"sessao_gigatech_{s}.json"


def prefixo_perfil(perfil: str) -> str:
    s = sanitizar_perfil(perfil)
    if s == "default":
        return ""
    return f"{s}/"


def env_por_perfil(prefixo: str, perfil: str) -> str:
    s = sanitizar_perfil(perfil)
    nome_especifico = f"{prefixo}_{s.upper()}"
    valor = os.getenv(nome_especifico)
    if valor is not None:
        return valor.strip()
    if s != "default":
        print(f"[AVISO] {nome_especifico} não definido, usando {prefixo} como fallback.")
    return (os.getenv(prefixo) or "").strip()
