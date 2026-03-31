#  Pipeline ETL Agro: Arquitetura Medallion na AWS (Serverless)

<p align="center">
  <img src="https://img.shields.io/badge/AWS-Lambda-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" />
  <img src="https://img.shields.io/badge/AWS-S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" />
  <img src="https://img.shields.io/badge/AWS-Athena-FF4F8B?style=for-the-badge&logo=amazonaws&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" />
  <img src="https://img.shields.io/badge/Looker_Studio-4285F4?style=for-the-badge&logo=looker&logoColor=white" />
</p>

## 📌 Sobre o Projeto
Este repositório contém o código e a documentação de um pipeline de **Engenharia de Dados 100% Serverless** na AWS, focado no processamento de dados do agronegócio (produção de Soja e Milho no estado de Goiás). 

O projeto implementa a **Arquitetura Medallion (Bronze, Silver, Gold)**, automatizando a transformação de dados brutos (CSV) em um formato analítico colunar (Parquet), finalizando com a visualização em um dashboard interativo.

---

##  Arquitetura do Data Lake

O fluxo de dados foi desenhado para ser escalável, de baixo custo e sem necessidade de gerenciamento de servidores:

1. **Camada Bronze (S3):** Armazena os dados brutos de produção agrícola em formato CSV, exatamente como extraídos da fonte original.
2. **Processamento (AWS Lambda + Python/Pandas):** Um script Python roda no Lambda, ativado por eventos. Ele utiliza *Custom Layers* (ARN) do Pandas para contornar as limitações nativas da nuvem. O script realiza:
   * **Melt (Wide para Long):** Reestruturação das colunas de anos para linhas.
   * **Limpeza e Tradução:** Padronização de nomes de colunas e remoção de nulos.
   * **Tipagem de Dados:** Conversão de valores para `Float` (Double) e `Int64` (Bigint).
3. **Camada Prata (S3 em Parquet):** O Lambda salva os dados limpos de volta no S3, agora compactados no formato Parquet (otimizado para Big Data).
4. **Camada Ouro (Amazon Athena):** Criação de tabelas virtuais (`External Tables`) apontando para a camada Prata. Utilização de comandos SQL (CTAS) para agregar os dados (ex: Soma de Toneladas por Estado) e gerar a visão de negócios.
5. **Data Visualization (Looker Studio):** Conexão com a tabela agregada da camada Ouro para geração de gráficos de Business Intelligence.

---

##  Tecnologias e Ferramentas

* **Linguagem:** Python 3.12
* **Bibliotecas:** Pandas, Boto3, io, pyarrow (para Parquet).
* **Cloud Provider (AWS):** * `Amazon S3` (Data Lake Storage)
  * `AWS Lambda` (Computação Serverless)
  * `AWS IAM` (Gestão de Permissões e Roles)
  * `Amazon Athena` (Motor de Consultas SQL)
* **BI:** Google Looker Studio

---

##  Como Funciona o Código (`lambda_function.py`)

O código presente neste repositório é o núcleo da transformação (ETL). Ele contém a função `lambda_handler`, que é o ponto de entrada da AWS. 

**Fluxo interno do script:**
1. Conecta-se ao bucket **Bronze** via `boto3`.
2. Carrega o arquivo `producao_agricola.csv` para a memória (utilizando controle de Timeout e Memória configurados para 512MB e 1min).
3. Aplica as regras de negócio via Pandas (`pd.melt`, `astype`, `rename`).
4. Converte o DataFrame limpo diretamente para o formato `.parquet` utilizando buffer de memória (`io.BytesIO`).
5. Realiza o upload (PutObject) no bucket **Silver**.

---

##  Próximos Passos para aprendizado.
- [ ] Implementar **EventBridge** para disparar o Lambda automaticamente assim que um novo arquivo cair no S3 Bronze.
- [ ] Conectar o Looker Studio diretamente ao AWS Athena (conexão live) ao invés de extração estática.
- [ ] Adicionar testes unitários locais com a biblioteca `moto` (Mock AWS Services).

---
*Desenvolvido por Samuel Pires.*