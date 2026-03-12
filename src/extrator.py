import requests
import pandas as pd
import os

print("---INICIANDO EXTRAÇÃO DE DADOS DO IBGE (AGRO)---")

# Tabela 5457: Produção Agrícola Municipal (PAM)
# n3/52 = Estado de Goiás (nível 3 = UF, código 52)
# v/214 = Quantidade produzida (toneladas)
# p/last 1 = Último período disponível
# c782/39431,39441 = Culturas: Soja (39431) e Milho (39441)
#
# NOTA: A variável 83 não existe na tabela 5457.
# A variável correta para quantidade produzida é 214.
# Caso queira municípios, use n6/all com n3[52] via API nova.

url_ibge = (
    "https://apisidra.ibge.gov.br/values"
    "/t/5457"        # Tabela PAM
    "/n3/52"         # Estado de Goiás
    "/v/214"         # Quantidade produzida (toneladas)
    "/p/2023"        # Último ano disponível com os dados consolidados. 
    "/c782/39431,39441"  # Soja e Milho
)

# Cabeçalho para evitar bloqueio
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

print(f"Consultando: {url_ibge}")

# Requisição GET com timeout para evitar travamento
try:
    resposta = requests.get(url_ibge, headers=headers, timeout=30)
except requests.exceptions.Timeout:
    print("ERRO: A requisição excedeu o tempo limite (30s). Verifique sua conexão.")
    exit(1)
except requests.exceptions.ConnectionError:
    print("ERRO: Sem conexão com a internet ou a API do IBGE está fora do ar.")
    exit(1)

# Verificação do status da resposta
print(f"Status da resposta: {resposta.status_code}")

if resposta.status_code == 200:
    # Transformação para JSON
    dados_json = resposta.json()

    # A primeira linha é o cabeçalho — pulamos ela
    if len(dados_json) <= 1:
        print("AVISO: A API retornou dados vazios para essa combinação de filtros.")
        exit(0)

    dados_reais = dados_json[1:]

    # Entregando para o pandas
    df = pd.DataFrame(dados_reais)

    # Garante que a pasta raw existe
    os.makedirs('../data/raw', exist_ok=True)

    # Guardando no raw
    caminho_arquivo = '../data/raw/producao_goias_bruto.csv'
    df.to_csv(caminho_arquivo, index=False, sep=';', encoding='utf-8')

    print(f"Extração Concluída! {len(dados_reais)} registros capturados.")
    print(f"Arquivo salvo em: {caminho_arquivo}")
    print(df.head())

else:
    print(f"Erro de conexão. O IBGE respondeu com o código: {resposta.status_code}")
    print(f"Resposta: {resposta.text[:300]}")