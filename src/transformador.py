import pandas as pd
import os 

# Carregar tabela
df_bruto = pd.read_csv('../data/raw/producao_goias_bruto.csv', sep=';')

# Tradução dos dados
dicionario_traducao = {
    'D1N': 'Estado',
    'D3N': 'Ano',
    'D4N': 'Cultura',
    'V': 'Toneladas'
}
df_traduzido = df_bruto.rename(columns=dicionario_traducao)

# Escolher somente as colunas que importam
colunas_boas = ['Estado', 'Ano', 'Cultura', 'Toneladas']
df_limpo = df_traduzido[colunas_boas]

# Converter Toneladas de "Texto" para "Número" (errors='coerce' ignora os traços '-' do IBGE)
df_limpo['Toneladas'] = pd.to_numeric(df_limpo['Toneladas'], errors='coerce')

# Salvar na pasta Processed 
os.makedirs('../data/processed', exist_ok=True)
df_limpo.to_csv('../data/processed/producao_goias_limpo.csv', index=False, sep=';')

print("Limpeza concluída!")
print(df_limpo.head())