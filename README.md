# Metodologia Estudo de Caso Pipeline AWS

**Autores:** Diego Luis Oliveira · Gilberto Cury Netto · Rafael Nascimento Prado

---

## Sumário

1. [Configuração do Ambiente e Ingestão (Camada Raw)](#1-configuração-do-ambiente-e-ingestão-camada-raw)
2. [Coleta via API com AWS Lambda](#2-coleta-via-api-com-aws-lambda)
3. [Processamento e Limpeza com AWS Glue (Camada Trusted)](#3-processamento-e-limpeza-com-aws-glue-camada-trusted)
4. [Cruzamento de Dados e Modelagem Dimensional (Camada Refined)](#4-cruzamento-de-dados-e-modelagem-dimensional-camada-refined)
5. [Visualização com AWS QuickSight](#5-visualização-com-aws-quicksight)
---

## 1. Configuração do Ambiente e Ingestão (Camada Raw)

### 1.1 Containerização com Docker

O primeiro passo do pipeline foi criar um ambiente reproduzível para o upload dos arquivos CSV ao S3. Para isso, foi utilizado um contêiner Docker baseado em Python 3.9, responsável por instalar a biblioteca `boto3` e executar o script de upload de forma automatizada. Ao iniciar, o contêiner executa o script diretamente, sem necessidade de intervenção manual.

```dockerfile
FROM python:3.9

WORKDIR /app

VOLUME ["/app/volume"]

COPY script.py /app/volume/script.py
COPY series.csv /app/volume/series.csv
COPY movies.csv /app/volume/movies.csv

RUN pip install boto3

CMD ["python", "/app/volume/script.py"]
```

**Evidência — criação da imagem:**

![prints/sprint 6/01_codigo_criar_img.png](https://github.com/GilbertoCNetto/TG-Estudo-de-Caso-Usabilidade-AWS-para-Engenheiro-de-Dados/blob/bffa374d90d1fabf20fbed1d7e5c5db7e05c9201/prints/sprint%206/01_codigo_criar_img.png)

---

### 1.2 Script de Upload para o S3

O script Python conecta-se ao S3 via `boto3` e realiza o envio dos arquivos CSV para o bucket configurado. O caminho de destino é construído dinamicamente incluindo camada, origem, formato, tipo de dado e data de processamento — convenção que garante rastreabilidade temporal a cada reexecução do pipeline.

```python
import boto3
from datetime import datetime

arquivo_filmes = '/app/movies.csv'
arquivo_series = '/app/series.csv'
bucket = 'data-lake-do-rafael-prado'
camada = 'Raw'
origem = 'Local'
formato = 'CSV'
especificacao_filmes = 'Movies'
especificacao_series = 'Series'
data_processamento = datetime.now().strftime('%Y/%m/%d')

s3 = boto3.client('s3')

def enviar_para_s3(arquivo_local, especificacao, arquivo_nome):
    caminho_s3 = f"{camada}/{origem}/{formato}/{especificacao}/{data_processamento}/{arquivo_nome}"
    s3.upload_file(arquivo_local, bucket, caminho_s3)
    print(f"Arquivo '{arquivo_nome}' enviado para o S3 em: {caminho_s3}")

enviar_para_s3(arquivo_filmes, especificacao_filmes, 'movies.csv')
enviar_para_s3(arquivo_series, especificacao_series, 'series.csv')
```

**Evidências:**
### Executando script
![[execucao_script](./../evidencias/04_codigo_upload_arquivos.png)](https://github.com/GilbertoCNetto/TG-Estudo-de-Caso-Usabilidade-AWS-para-Engenheiro-de-Dados/blob/d39262804b9308958931049acf22cd55cbe02ec7/prints/sprint%206/04_codigo_upload_arquivos.png)
### Bucket
![[criacao_bucket](./../evidencias/05_criacao_bucket.png)](https://github.com/GilbertoCNetto/TG-Estudo-de-Caso-Usabilidade-AWS-para-Engenheiro-de-Dados/blob/d39262804b9308958931049acf22cd55cbe02ec7/prints/sprint%206/05_criacao_bucket.png)
### Csv Filmes e Séries
![upload_csv_movies_e_series](https://github.com/GilbertoCNetto/TG-Estudo-de-Caso-Usabilidade-AWS-para-Engenheiro-de-Dados/blob/d39262804b9308958931049acf22cd55cbe02ec7/prints/sprint%206/06_csv_AWS.png)

---

## 2. Coleta via API com AWS Lambda

### 2.1 Criação da Lambda Layer

Para que as bibliotecas externas `requests` e `boto3` ficassem disponíveis no ambiente Lambda — que não as inclui no runtime padrão —, foi necessário criar uma **Lambda Layer**. O processo consistiu em:

1. Criar um contêiner baseado em **Amazon Linux 2023**, sistema operacional compatível com o ambiente de execução do Lambda;
2. Instalar as dependências via `pip` dentro do contêiner;
3. Empacotar o diretório de instalação em um arquivo ZIP;
4. Fazer o upload do ZIP para o S3;
5. Registrar a Layer na AWS e vinculá-la à função Lambda.

```dockerfile
FROM amazonlinux:2023
RUN yum update -y
RUN yum install -y \
    python3-pip \
    zip
RUN yum -y clean all
```

**Evidências — criação e upload da Layer:**

![criacao_imagem_amazonlinux](./../evidencias/Desafio/01_criacao_da_imagem.png)

![copiando_layer](./../evidencias/Desafio/03_copiando_layer.png)

![criacao_layer](./../evidencias/Desafio/criacao_layer.png)

![upload_layer_s3](./../evidencias/Desafio/upload_layer_s3.png)

---

### 2.2 Função Lambda — Coleta via API REST

A função Lambda realiza a coleta de dados via API REST com paginação automática. A cada execução, percorre múltiplas páginas de resultados, enriquece cada registro buscando seus detalhes individuais em um segundo endpoint e salva os dados em lotes no S3, particionados por data de execução. O caminho de saída segue a mesma convenção hierárquica adotada na camada Raw.

A função é composta por três partes principais:

- `buscar_registros_basicos` — consulta a listagem paginada com os filtros definidos;
- `buscar_detalhes` — consulta o endpoint de detalhes para cada ID retornado;
- `lambda_handler` — orquestra o loop de coleta, controla o limite de registros e persiste os lotes no S3 via `s3.put_object`.

```python
import requests
import json
import boto3
from datetime import datetime

s3_bucket = "data-lake-do-rafael-prado"
s3_folder = "Raw/TMDB/JSON"
s3 = boto3.client("s3")

def lambda_handler(event, context):
    total = 3000
    por_arquivo = 100
    pagina = 1
    coletados = []

    data_atual = datetime.now()
    ano = data_atual.strftime("%Y")
    mes = data_atual.strftime("%m")
    dia = data_atual.strftime("%d")

    while len(coletados) < total and pagina <= 150:
        basicos = buscar_registros_basicos(pagina)
        if not basicos:
            break
        for item in basicos:
            detalhes = buscar_detalhes(item["id"])
            if detalhes:
                coletados.append(detalhes)
        pagina += 1

    coletados = coletados[:total]

    for i in range(0, len(coletados), por_arquivo):
        parte = i // por_arquivo + 1
        nome = f"registros-part-{parte}.json"
        caminho = f"{s3_folder}/{ano}/{mes}/{dia}/{nome}"
        s3.put_object(
            Bucket=s3_bucket,
            Key=caminho,
            Body=json.dumps(coletados[i:i + por_arquivo], ensure_ascii=False, indent=4),
            ContentType="application/json"
        )

    return {
        "statusCode": 200,
        "body": f"{len(coletados)} registros salvos em {s3_folder}/{ano}/{mes}/{dia}/"
    }
```

**Evidências — criação, configuração e execução da função Lambda:**

![criacao_funcao_lambda](./../evidencias/Desafio/06_funcao_lambda_aws.png)

![configuracao_lambda](./../evidencias/Desafio/configuracao_funcao_lambda.png)

![configuracao_lambda2](./../evidencias/Desafio/configuracao_funcao_lambda2.png)

![adicao_layer](./../evidencias/Desafio/08_adicao_layer_lambda.png)

![execucao_lambda](./../evidencias/Desafio/09_execucao_codigo_lambda.png)

![jsons_no_bucket](./../evidencias/Desafio/10_criacao_jsons_bucket_pelo_codigo.png)

---

## 3. Processamento e Limpeza com AWS Glue (Camada Trusted)

Com os dados brutos armazenados na camada Raw, a etapa seguinte consistiu na execução de dois jobs de ETL (*Extract, Transform, Load*) no **AWS Glue** com **PySpark** — um para cada formato de entrada (CSV e JSON). Ambos os jobs leem os dados do S3 Raw, aplicam transformações e gravam o resultado em formato **Parquet** na camada Trusted, particionado por data de execução.

---

### 3.1 Job ETL — CSV para Parquet

O job lê o CSV com delimitador `|`, remove colunas irrelevantes para a análise, elimina linhas com valores nulos e registros duplicados, e salva o resultado em Parquet.

```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime

source_file = "s3://data-lake-do-rafael-prado/Raw/Local/CSV/Movies/2024/11/11/movies.csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("MoviesJob", {})

df = spark.read.option("delimiter", "|").csv(source_file, header=True, inferSchema=True)

df_tratado = df.drop(
    "titulosMaisConhecidos", "profissao", "anoFalecimento",
    "anoNascimento", "nomeArtista", "personagem"
)
df_tratado = df_tratado.dropna().dropDuplicates()

if df_tratado.count() == 0:
    raise ValueError("Nenhum dado disponível para salvar no destino.")

dynamic_frame_tratado = DynamicFrame.fromDF(df_tratado, glueContext)

data_atual = datetime.now()
target_path = f"s3://data-lake-do-rafael-prado/Trusted/CSV/{data_atual.strftime('%Y/%m/%d')}/"

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_tratado,
    connection_type="s3",
    connection_options={"path": target_path},
    format="parquet"
)

job.commit()
```

**Evidências — execução do job e confirmação do Parquet no S3:**

![run_job_csv](./../evidencias/Parquet%20CSV/03_run_job.png)

![parquet_csv_s3](./../evidencias/Parquet%20CSV/pos_execucao_job_csv.png)

---

### 3.2 Job ETL — JSON para Parquet

O job lê os arquivos JSON da camada Raw e aplica as seguintes transformações:

- **Extração de gêneros**: UDF PySpark converte a coluna `genres` de lista de objetos `{id, name}` para lista de strings com apenas os nomes;
- **Conversão de data**: `release_date` convertida de string para `DateType` com `to_date(..., "yyyy-MM-dd")`;
- **Seleção de colunas**: apenas as colunas relevantes são mantidas;
- **Nulos e duplicatas**: `dropna` nos campos obrigatórios e `dropDuplicates` no conjunto de colunas selecionadas;
- **Cast de tipos**: `revenue`, `runtime` e `vote_count` para `int`; `vote_average` para `float`; `revenue = 0` substituído por `null`.

```python
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, udf, to_date, when
from pyspark.sql.types import ArrayType, StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_path = "s3://data-lake-do-rafael-prado/Raw/TMDB/JSON/2024/11/28/"

df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_path]},
    format="json"
).toDF()

def extrair_nomes(genres):
    return [g['name'] for g in genres]

extrair_nomes_udf = udf(extrair_nomes, ArrayType(StringType()))
df = df.withColumn("genres", extrair_nomes_udf(col("genres")))
df = df.withColumn("release_date", to_date(df["release_date"], "yyyy-MM-dd"))

colunas = ["id", "title", "original_title", "release_date", "runtime",
           "vote_count", "vote_average", "revenue", "original_language"]
df = df.select(*colunas)
df = df.dropna(subset=["title", "original_title"]).dropDuplicates(colunas)
df = df.withColumn("revenue", col("revenue.int").cast("int"))
df = df.withColumn("revenue", when(col("revenue") == 0, None).otherwise(col("revenue")))
df = df.withColumn("runtime", col("runtime").cast("int"))
df = df.withColumn("vote_count", col("vote_count").cast("int"))
df = df.withColumn("vote_average", col("vote_average").cast("float"))

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

data_atual = datetime.now()
target_path = f"s3://data-lake-do-rafael-prado/Trusted/JSON/{data_atual.strftime('%Y/%m/%d')}/"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={"path": target_path},
    format="parquet"
)

job.commit()
```

**Evidências — execução do job e confirmação do Parquet no S3:**

![run_job_json](./../evidencias/Parquet%20JSON/03_run_job.png)

![parquet_json_s3](./../evidencias/Parquet%20JSON/04_pos_execucao_json.png)

---

### 3.3 Catalogação com Glue Crawlers e Validação no Athena

Após cada job, **Glue Crawlers** são executados para catalogar automaticamente as tabelas geradas no **Glue Data Catalog**. As tabelas ficam acessíveis via SQL no **AWS Athena**, onde consultas de validação confirmam o schema e a integridade dos dados antes de avançar para a próxima camada.

**Evidências — criação e execução dos Crawlers:**

![database](./../evidencias/Parquet%20JSON/database_usado.png)

![crawler_csv](./../evidencias/Parquet%20CSV/05_crawler_csv.png)

![exec_crawler_csv](./../evidencias/Parquet%20CSV/06_execucao_crawler.png)

![crawler_json](./../evidencias/Parquet%20JSON/05_crawler_json.png)

![exec_crawler_json](./../evidencias/Parquet%20JSON/execucao_crawler_json.png)

![tables](./../evidencias/Parquet%20JSON/tables.png)

**Evidências — queries de validação no Athena:**

![query_csv](./../evidencias/Parquet%20CSV/07_query_athena.png)

![schema_csv](./../evidencias/Parquet%20CSV/08_schema_csv.png)

![query_json](./../evidencias/Parquet%20JSON/06_query_athena.png)

![schema_json](./../evidencias/Parquet%20JSON/schema.png)

---

## 4. Cruzamento de Dados e Modelagem Dimensional (Camada Refined)

### 4.1 Job ETL — Camada Pré-Refined

Este job realiza o cruzamento dos dados das duas fontes já tratados na camada Trusted. O join é feito pelo título do registro, e o campo de gênero artístico proveniente do CSV é incorporado ao dataset JSON como metadado adicional.

Como o CSV pode conter múltiplas linhas para o mesmo registro — uma por categoria de artista —, o campo é agrupado com `collect_set` e depois convertido para string com `concat_ws`, evitando duplicações na tabela resultante.

```python
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

caminho_json = "s3://data-lake-do-rafael-prado/Trusted/JSON/2024/12/13/"
caminho_csv  = "s3://data-lake-do-rafael-prado/Trusted/CSV/2024/12/12/"

dados_json = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", connection_options={"paths": [caminho_json]}, format="parquet"
).toDF()

dados_csv = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", connection_options={"paths": [caminho_csv]}, format="parquet"
).toDF()

dados_csv = dados_csv.withColumnRenamed("generoArtista", "artist_genre")

dados_juntos = dados_json.join(
    dados_csv, dados_json["title"] == dados_csv["tituloPincipal"], "inner"
).select(dados_json["*"], dados_csv["artist_genre"])

dados_agrupados = dados_juntos.groupBy(
    "id", "title", "original_title", "release_date", "runtime",
    "vote_count", "vote_average", "revenue", "original_language",
    "genres", "origin_country", "adult", "budget", "popularity"
).agg(F.collect_set("artist_genre").alias("artist_genre"))

dados_agrupados = dados_agrupados.withColumn(
    "artist_genre", F.concat_ws(", ", F.col("artist_genre"))
)

dyf = DynamicFrame.fromDF(dados_agrupados, glueContext, "dyf")

data_atual = datetime.now()
saida = f"s3://data-lake-do-rafael-prado/pre-refined/{data_atual.strftime('%Y/%m/%d')}/"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={"path": saida},
    format="parquet"
)

job.commit()
```

**Evidências — execução do job e arquivos gerados:**

![run_pre_refined](./../evidencias/pre-refined-staged/03_job_run.png)

![arquivos_pre_refined](./../evidencias/pre-refined-staged/04_arquivos_salvos_bucket.png)

![query_pre_refined](./../evidencias/pre-refined-staged/05_visualizacao_pre_refned_athena.png)

---

### 4.2 Job ETL — Modelagem Dimensional (Camada Refined)

A partir dos dados da camada Pré-Refined, este job implementa um **modelo dimensional em esquema estrela**, composto por três tabelas dimensão e uma tabela fato. Cada tabela é salva em uma subpasta separada no S3.

- **Dim_Filme**: atributos descritivos, utilizando o ID nativo da fonte (não há necessidade de ID sintético);
- **Dim_Tempo**: data de registro decomposta em ano, mês e dia; ID sintético gerado via `row_number()` sobre uma `Window`;
- **Dim_Localização**: cada país de origem é explodido em uma linha individual com `explode(split(...))`, limpo e deduplicado; ID sintético via `row_number()`;
- **Fato**: métricas quantitativas e chaves estrangeiras para as três dimensões, obtidas via join com cada tabela dimensional.

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import explode, split, trim, col, row_number, year, month, dayofmonth, regexp_replace
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path  = "s3://data-lake-do-rafael-prado/pre-refined/2024/12/13/"
output_path = "s3://data-lake-do-rafael-prado/Refined/"

df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [input_path], "recurse": True},
    transformation_ctx="datasource0"
).toDF()

dim_filme = df.select("id", "title", "original_title", "runtime",
                      "original_language", "adult", "revenue", "budget")

dim_tempo = df.select("release_date").distinct() \
    .withColumn("ano", year(col("release_date"))) \
    .withColumn("mes", month(col("release_date"))) \
    .withColumn("dia", dayofmonth(col("release_date"))) \
    .withColumn("id_tempo", row_number().over(Window.orderBy("release_date")))

df = df.withColumn("origin_country", trim(regexp_replace(col("origin_country"), "\\[|\\]", "")))
dim_localizacao = df.select(explode(split(col("origin_country"), ",")).alias("localizacao")).distinct() \
    .withColumn("localizacao", trim(col("localizacao"))) \
    .withColumn("id_localizacao", row_number().over(Window.orderBy("localizacao"))) \
    .dropDuplicates(["localizacao"]) \
    .filter(col("localizacao") != "")

df = df.withColumn("localizacao", explode(split(trim(regexp_replace(col("origin_country"), "\\[|\\]", "")), ",")))
fato = df.select("id", "release_date", "vote_count", "vote_average", "popularity", "localizacao") \
    .join(dim_filme.select("id", col("id").alias("id_filme")), on="id") \
    .join(dim_tempo.select("release_date", "id_tempo"), on="release_date", how="left") \
    .join(dim_localizacao.select("localizacao", "id_localizacao"), on="localizacao", how="left")

for tabela, pasta in [
    (dim_filme,       "dim_filme/"),
    (dim_tempo,       "dim_tempo/"),
    (dim_localizacao, "dim_localizacao/"),
    (fato,            "fato_filme/"),
]:
    glueContext.write_dynamic_frame.from_options(
        DynamicFrame.fromDF(tabela, glueContext, pasta),
        connection_type="s3",
        connection_options={"path": f"{output_path}{pasta}"},
        format="parquet"
    )

job.commit()
```

**Evidências — execução do job, dimensões no S3 e validação no Athena:**

![run_refined](./../evidencias/refined/03_job_run.png)

![dimensoes_refined](./../evidencias/refined/04_dimensoes_buckets.png)

![query_dim_filmes](./../evidencias/refined/05_visualizacao_athena_dim_filmes.png)

![modelo_dimensional](./../Desafio/entregaveis/modelo_dimensional/dimensionamento_sprint_9.png)

---

## 5. Visualização com AWS QuickSight

Com as tabelas dimensionais e de fato catalogadas no Glue Data Catalog e acessíveis via Athena, a etapa final consistiu na criação dos dashboards analíticos no **AWS QuickSight**.

### 5.1 Criação dos Datasets

Os datasets foram criados a partir do Athena como fonte de dados, selecionando individualmente cada tabela do database configurado no Glue Data Catalog.

**Evidências — criação dos datasets:**

![criando_datasets](./../evidencias/01_01_criando_datasets.png)

![selecao_athena](./../evidencias/01_02_selecao_athena.png)

![criacao_dataset_pt3](./../evidencias/01_03_criacao_dataset_pt3.png)

![criacao_dataset_pt4](./../evidencias/01_04_criacao_dataset_pt4.png)

![criacao_dataset_pt5](./../evidencias/01_05_criacao_dataset_pt5.png)

![criacao_dataset_pt6](./../evidencias/01_06_criacao_dataset_pt6.png)

![criacao_dataset_pt7](./../evidencias/01_07_criacao_dataset_pt7.png)

---

### 5.2 Configuração dos Joins

Os joins foram configurados diretamente no editor de dataset do QuickSight. A tabela `fato_filme` foi definida como tabela principal, com **left joins** para cada uma das três dimensões, respeitando o modelo estrela construído na etapa anterior.

**Evidências — configuração dos joins:**

![realizando_join](./../evidencias/02_realizando_join.png)

![lefts_joins_fato](./../evidencias/02_02_lefts_joins_fato.png)

---

### 5.3 Criação da Análise e dos Gráficos

Com o dataset unificado configurado, foi criada uma nova análise no QuickSight. A partir dela, foram construídos os painéis e gráficos utilizados no estudo de caso.

**Evidências — criação da análise:**

![nova_analise](./../evidencias/03_criando_analise.png)

![selecionando_dataset](./../evidencias/04_selecionando_dataset.png)

![usando_em_analise](./../evidencias/05_usando_em_analise.png)

![tela_final](./../evidencias/06_tela_final.png)

---

