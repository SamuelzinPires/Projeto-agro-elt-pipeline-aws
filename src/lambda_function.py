import boto3    
import os 
import pandas as pd

# Inicia o cliente S3 
s3_client = boto3.client('s3')

#---Variáveis de Rota ---
BUCKET_BRONZE = 'projeto-etl-agro-bronze-samuel'
BUCKET_PRATA = 'projeto-etl-agro-prata-samuel'
ARQUIVO_BRONZE = 'dados_brutos/production_value.csv'

# Entrada do lambda
def lambda_handler(event, context):
    print("Iniciando o processamento do Lambda...")
#---Extração Direta da Nuvem---  
    try:
        print("Acessando o cofre Bronze na AWS S3...")
        response = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=ARQUIVO_BRONZE)
    
        print("Carregamento do Pandas...")
        df = pd.read_csv(response['Body'])
        print("Transformação dos dados...")

        # Realizar a limpeza e transformação dos dados
        print(" Iniciando a Limpeza dos Dados (Transformação)...")
        
        # Remover a coluna inútil 
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])

        # Filtrar o tempo e a cultura (2010 em diante, apenas Soja e Milho)
        df = df[df['Year'] >= 2010]
        df = df[df['Grain'].isin(['Soybeans', 'Maize'])]

        # (Wide para Long)
        print(" tratamento da tabela de Wide para Long...")
        df_long = pd.melt(
            df, 
            id_vars=['Grain', 'Year'], 
            var_name='Municipio',      
            value_name='Valor'         
        )

        # Faxina Pesada
        print("Convertendo textos para números absolutos...")
        df_long['Valor'] = pd.to_numeric(df_long['Valor'], errors='coerce')
        df_long = df_long.dropna(subset=['Valor'])
        df_long = df_long[df_long['Valor'] > 0]

        # Filtro (Apenas Goiás)
        df_goias = df_long[df_long['Municipio'].str.contains(r'\(GO\)', regex=True, na=False)].copy()

        # TRADUÇÃO E FORMATAÇÃO 
        print("Traduzindo para Pt-BR...")
        df_goias['Estado'] = 'Goiás'
        # Traduzir os grãos
        df_goias['Cultura'] = df_goias['Grain'].replace({'Soybeans': 'Soja', 'Maize': 'Milho'})
        # Renomear colunas
        df_goias = df_goias.rename(columns={'Year': 'Ano', 'Valor': 'Toneladas'})
        
        # Organizar a ordem final das colunas
        df_goias = df_goias[['Estado', 'Ano', 'Cultura', 'Municipio', 'Toneladas']]

        print("Transformação concluída com sucesso absoluto!")
        print("\n--- Os dos Dados Limpos ---")
        print(f"Linhas x Colunas: {df_goias.shape}")
        print(df_goias.head())

        # Dados para formato Parquet e upload para a camada Prata com salvando na tmp 
        print("\n Empacotando os dados limpos no formato Parquet...")
        
        # Definir o nome do arquivo temporário e o destino na nuvem
        arquivo_temporario = '/tmp/dados_prata_temp.parquet'
        nome_na_nuvem_prata = 'dados_limpos/producao_goias.parquet'

        # O Pandas salva o resultado no  PC temporariamente
        df_goias.to_parquet(arquivo_temporario, index=False)
        
        # boto3 envia esse arquivo novo para o Bucket Prata!
        print(f" Disparando o arquivo para o cofre Prata: {BUCKET_PRATA}...")
        s3_client.upload_file(arquivo_temporario, BUCKET_PRATA, nome_na_nuvem_prata)
        
        # Apaga o arquivo temporário do seu PC para não deixar lixo
        os.remove(arquivo_temporario)

        return {
            'statusCode': 200,
            'body': ' Camada Prata alimentada!'
        }
    except Exception as e:
        print(f" Erro na operação: {e}")
        return {
            'statusCode': 500,
            'body': f'Erro: {e}'
        }