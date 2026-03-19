import os
import boto3
from dotenv import load_dotenv

# Carrega as senhas do .env
load_dotenv()

# Inicia o cliente S3 
s3_client = boto3.client(
    service_name='s3',
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

# Variáveis do Projeto 
NOME_DO_BRONZE = 'projeto-etl-agro-bronze-samuel' 
CAMINHO_LOCAL = '../data/raw/production.csv'
NOME_NA_NUVEM = 'dados_brutos/production_value.csv'

# 4. Envio para a AWS
try:
    print("Iniciando envio do arquivo para a AWS S3...")
    s3_client.upload_file(CAMINHO_LOCAL, NOME_DO_BRONZE, NOME_NA_NUVEM)
    print(" Sucesso! O arquivo enviado na Camada Bronze!")
except Exception as e:
    print(f" Falha: {e}")