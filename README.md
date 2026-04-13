# TG Estudo de Caso - Usabilidade AWS para Engenheiro de Dados
# Alunos:
- Diego Luis Oliveira
- Gilberto Cury Netto
- Rafael Nascimento Prado

# Sprint 1

## Configuração inicial e upload da base de dados utilizada para o estudo de caso:

Iniciamos o estudo de caso configurando um contêiner com Python 3.9 para rodar um script (script.py) que faz o upload dos arquivos series.csv e movies.csv para o S3 da AWS. Ele também instala a biblioteca boto3, que permite a conexão com o S3. Dessa forma, o contêiner cria um ambiente ideal para realizar uploads de dados na nuvem de forma fácil e automatizada.

- Segue abaixo o codigo do dockerfile usado para construção da imagem e em seguida sua explicação:

```Dockerfile 
FROM python:3.9

WORKDIR /app

VOLUME ["/app/volume"]

COPY script.py /app/volume/script.py
COPY series.csv /app/volume/series.csv 
COPY movies.csv /app/volume/movies.csv

RUN pip install boto3

CMD ["python", "/app/volume/script.py"]

```

1. **Imagem base**:
   - A imagem do contêiner é definida como `"python:3.9"`, garantindo que o ambiente de execução use Python 3.9.
2. **Configuração do ambiente de trabalho**:
   - O diretório de trabalho é configurado com `"WORKDIR /app"`, onde os arquivos e comandos subsequentes serão executados.
3. **Criação do volume**:
   - O comando `"VOLUME [\"/app/volume\"]"` cria um volume no contêiner.
4. **Copiando arquivos para o volume**:
   - Os arquivos `script.py`, `series.csv` e `movies.csv` são copiados para o diretório `/app/volume` usando os comandos `"COPY script.py /app/volume/script.py"`, `"COPY series.csv /app/volume/series.csv"` e `"COPY movies.csv /app/volume/movies.csv"`, tornando-os disponíveis para o script durante a execução.
5. **Instalação de dependências**:
   - O comando `"RUN pip install boto3"` instala a biblioteca boto3, necessária para interação com o S3 da AWS.
6. **Comando para execução do script**:
   - O comando `"CMD [\"python\", \"/app/volume/script.py\"]"` define que o script Python será executado quando o contêiner iniciar.

---

### Confirmação da devida criação da imagem no terminal:

![criacao_imagem](./../evidencias/02_criacao_imagem.png)



## ETAPA 2:

Esse código tem como objetivo conectar-se ao S3 da AWS e enviar os arquivos movies.csv e series.csv para um bucket específico (data-lake-do-rafael-prado). Além disso, ele organiza o caminho de armazenamento no S3 com uma estrutura padrão que inclui camada, origem, formato, uma especificação para cada tipo de arquivo (filmes e séries) e a data de processamento. A função `enviar_para_s3` faz o upload dos arquivos seguindo essa estrutura, exibindo uma mensagem de confirmação ao final de cada envio.

- Segue abaixo o codigo em python para o upload dos CSVS para a nuvem:

```Python
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


1. **Conexão com o S3**:
   - Primeiramente, a biblioteca `boto3` é importada para interação com a AWS, especificamente com o S3.
2. **Configuração dos arquivos e parâmetros**:
   - As variáveis `arquivo_filmes` e `arquivo_series` definem o caminho dos arquivos locais a serem enviados ao S3.
   - Variáveis como `bucket`, `camada`, `origem`, `formato`, `especificacao_filmes`, `especificacao_series` e `data_processamento` configuram os detalhes de armazenamento, incluindo a estrutura de pastas e data de processamento.
3. **Função de upload**:
   - A função `enviar_para_s3` recebe o caminho do arquivo local, especificação e nome do arquivo para enviar para o S3.
   - `caminho_s3` constrói o caminho de destino no S3 com a estrutura e data especificadas.
   - O comando `"s3.upload_file"` faz o upload do arquivo para o bucket no S3 no caminho configurado.
4. **Confirmação**:
   - A função exibe uma mensagem confirmando que o arquivo foi enviado para o caminho especificado no S3, indicando o sucesso da operação.
 
 ---
### Evidência da execução do código que sobe os CSV'S:
![execucao_script](./../evidencias/04_codigo_upload_arquivos.png)

## Evidência da criação do bucket:
![criacao_bucket](./../evidencias/05_criacao_bucket.png)

### Evidencia do Upload do CSV Movies:
![upload_csv_movies](./../evidencias/06_csv_AWS_movies.png)

### Evidencia do Upload do CSV Series:
![upload_csv_series](./../evidencias/06_csv_AWS_series.png)

# Sprint 2

## ETAPA 1.1:

O propósito deste código é configurar o contêiner utilizando o Amazon Linux 2023 como base, preparando o ambiente para rodar scripts Python que possam interagir com o S3. Ele inclui a instalação do Python 3 e do pip que servem para executar scripts em Python e gerenciar dependências, como a biblioteca boto3. Além disso, a ferramenta zip é instalada, possibilitando também a compactação de arquivos.

- Segue abaixo o codigo do dockerfile usado para construção da imagem e em seguida sua explicação:

```Dockerfile 
FROM amazonlinux:2023
RUN yum update -y
RUN yum install -y \
    python3-pip \
    zip
RUN yum -y clean all
```

1. **Imagem Base**: 
- A imagem do contêiner é definida como `amazonlinux:2023`, garantindo que o ambiente de execução utilize o Amazon Linux 2023;

2. **Atualização do sistema**:
- O comando `RUN yum update -y` atualiza os pacotes do sistema para as versões mais recentes;

3. **Instalação de ferramentas necessárias**:
- O comando `RUN yum install -y \ python3-pip \ zip` instala o gerenciador de pacotes do Python (pip) e a ferramenta de compactação (zip). O pip é essencial para instalar bibliotecas como boto3, enquanto o zip permite a manipulação e compactação de arquivos.

4. **Limpeza do ambiente**:
- O comando `RUN yum -y clean all` remove os arquivos temporários gerados durante a instalação, reduzindo o tamanho final da imagem e tornando o contêiner mais eficiente.
---

### Confirmação da devida criação da imagem no terminal:

![criacao_imagem](./..//evidencias/Desafio/01_criacao_da%20_imagem.png)


## ETAPA 1.2: 

Nesta etapa foi feita a criação da layer. Para isso, foi necessario **seguir o tutorial da criação de layers do exercício LAB LAMBDA da SPRINT 6**, o passo-a-passo é basicamente o mesmo, e por conta disso decidi não tirar prints do processo de criação, pois o tutorial é fornecido pelo PB e encontra-se no PDF do exercício da sprint 6 na Udemy. A diferença está no momento em que fazemos a `instalação de bibliotecas`, pois na ultima sprint foi feita a instalação da biblioteca `Pandas`, e nesta sprint, foi feita a instalação das bibliotecas `requests` e `boto3` na layer através do comando  `pip install` no terminal no momento da criação, com o `bash`. 

### Segue abaixo a evidência da criação da layer em zip:

![criacao_layer](./..//evidencias/Desafio/03_copiando_layer.png)

![criacao_layer](./..//evidencias/Desafio/criacao_layer.png)



--- 

### E após isso foi feito o Upload da layer no S3:

![criacao_layer](./..//evidencias/Desafio/upload_layer_s3.png)

## ETAPA 2:


O propósito deste código abaixo é coletar informações de filmes da API do The Movie Database (TMDb) através do `Lambda` da AWS e salvar esses dados em um bucket S3 no formato JSON. Ele utiliza as bibliotecas requests e boto3 para, respectivamente, consumir a API e interagir com o serviço S3.

- Segue abaixo o codigo Lambda usado para a consulta ao TMDb e sua devida explicação resumida através dos comentários no código:

```Python
import requests
import json
import boto3
from datetime import datetime

# Chave da API do The Movie Database (TMDb), usada para autenticar as requisições.
api_key = "6a0efdab02802a37b023045239e9c652"

# URL base para buscar listas de filmes.
base_discover_url = "https://api.themoviedb.org/3/discover/movie"

# URL base para buscar detalhes específicos de um filme.
base_detail_url = "https://api.themoviedb.org/3/movie"

# Define o idioma das respostas como português do Brasil.
language = "pt-BR"

# Nome do bucket S3 onde os arquivos JSON serão salvos.
s3_bucket = "data-lake-do-rafael-prado"

# Caminho dentro do bucket para organizar os arquivos salvos.
s3_folder = "Raw/TMDB/JSON"

# Inicializa o cliente boto3 para interagir com o S3.
s3 = boto3.client("s3")

# Função para buscar filmes básicos na API do TMDb, de acordo com os filtros aplicados.
def buscar_filmes_basicos(api_key, pagina):
    # Constrói a URL com filtros: gênero, data de lançamento, nota mínima, e paginação.
    url = f"{base_discover_url}?api_key={api_key}&language={language}&page={pagina}&primary_release_date.gte=1970-01-01&primary_release_date.lte=2020-12-31&vote_average.gte=8&with_genres=28,12"
    resposta = requests.get(url)  # Faz a requisição à API.
    if resposta.status_code == 200:  # Verifica se a requisição foi bem-sucedida.
        return resposta.json().get("results", [])  # Retorna a lista de filmes básicos.
    else:
        print(f"Erro ao buscar filmes básicos: {resposta.status_code}")  # Log de erro.
        return []

# Função para buscar os detalhes de um filme específico pelo ID.
def buscar_detalhes_filme(api_key, movie_id):
    # Constrói a URL com o ID do filme.
    url = f"{base_detail_url}/{movie_id}?api_key={api_key}&language={language}"
    resposta = requests.get(url)  # Faz a requisição à API.
    if resposta.status_code == 200:  # Verifica se a requisição foi bem-sucedida.
        return resposta.json()  # Retorna os detalhes do filme.
    else:
        print(f"Erro ao buscar detalhes do filme {movie_id}: {resposta.status_code}")  # Log de erro.
        return None

# Função principal que será executada, ideal para uso em uma função AWS Lambda.
def lambda_handler(event, context):
    total_filmes = 3000  # Quantidade total de filmes a coletar.
    filmes_por_arquivo = 100  # Quantidade de filmes por arquivo JSON.
    pagina_atual = 1  # Página inicial para buscar filmes na API.
    filmes_coletados = []  # Lista para armazenar todos os filmes coletados.

    # Define a data atual para organizar os arquivos no S3 por ano/mês/dia.
    data_atual = datetime.now()
    ano = data_atual.strftime("%Y")
    mes = data_atual.strftime("%m")
    dia = data_atual.strftime("%d")

    # Loop para buscar filmes enquanto o limite não for atingido.
    while len(filmes_coletados) < total_filmes and pagina_atual <= 150:
        # Busca filmes básicos na página atual.
        filmes_basicos = buscar_filmes_basicos(api_key, pagina_atual)
        if not filmes_basicos:  # Para o loop se não houver mais filmes.
            break
        for filme in filmes_basicos:  # Itera sobre os filmes básicos retornados.
            # Busca os detalhes de cada filme pelo ID.
            detalhes = buscar_detalhes_filme(api_key, filme["id"])
            if detalhes:
                filmes_coletados.append(detalhes)  # Adiciona os detalhes à lista final.
        pagina_atual += 1  # Passa para a próxima página.

    # Limita a lista ao total máximo de filmes permitido.
    filmes_coletados = filmes_coletados[:total_filmes]

    # Divide os filmes em partes e salva cada uma como um arquivo JSON no S3.
    for i in range(0, len(filmes_coletados), filmes_por_arquivo):
        parte = i // filmes_por_arquivo + 1  # Define o número da parte atual.
        nome_arquivo = f"filmes-acao-aventura-1970-2020-part-{parte}.json"  # Nome do arquivo.
        caminho_s3 = f"{s3_folder}/{ano}/{mes}/{dia}/{nome_arquivo}"  # Caminho no S3.

        # Serializa os dados como JSON e os salva no bucket S3.
        json_data = json.dumps(filmes_coletados[i:i + filmes_por_arquivo], ensure_ascii=False, indent=4)
        s3.put_object(
            Bucket=s3_bucket,
            Key=caminho_s3,
            Body=json_data,
            ContentType="application/json"
        )

    # Retorna uma mensagem de sucesso com o número total de filmes coletados e salvos.
    return {
        "statusCode": 200,
        "body": f"{len(filmes_coletados)} filmes salvos em arquivos de no máximo 100 registros no bucket S3 '{s3_bucket}' na pasta '{s3_folder}/{ano}/{mes}/{dia}'."
    }

```

 ## ETAPA 3:

 - Nesta etapa foi feita a `criação e configuração da função Lambda`, a `adição da layer` e por fim a `execução do código acima no proprio lambda` - segue abaixo as evidências.

### Criação da função lambda:
 

![criacao_layer](./..//evidencias/Desafio/06_funcao_lambda%20_aws.png)

### Configuração da função lambda:

![criacao_layer](./..//evidencias/Desafio/configuracao_funcao_lambda.png)

![criacao_layer](./..//evidencias/Desafio/configuracao_funcao_lambda2.png)

### Adição da layer:

![criacao_layer](./..//evidencias/Desafio/08%20adicao_layer_lambda.png)
 

### Execução do código:

![criacao_layer](./..//evidencias/Desafio/09%20execucao_codigo%20_lambda.png)


 ## ETAPA 4:

- O proximo passo foi conferir no S3 se tudo foi salvo corretamente como esperado.

![criacao_layer](./..//evidencias/Desafio/10%20criacao_jsons_bucket_pelo_codigo.png)

# Sprint 8 
## Neste desafio foram feitos 2 scripts para tratar dois diferentes formatos de arquivos, sendo o primeiro em CSV, e o segundo em JSON. Os processos são de certa forma semelhantes, e em virtude disso, serão explicados de forma paralela nas etapas.

# ETAPA 1

O propósito dos códigos abaixo é realizar uma série de tarefas para `processar e limpar dados armazenados em arquivos CSV'S e JSON no S3`, utilizando as ferramentas `AWS Glue` e `PySpark`, e enfim salvando-os em um bucket no formato `Parquet`.

### SCRIPT USADO NO JOB DO CSV:
- Segue abaixo o ``script  usado para a realização do processamento, limpeza, e transformação dos dados do CSV para Parquet`` (`ETL`), com sua devida expicação através dos comentários:

```python

import sys
from pyspark.context import SparkContext 
# Cria um contexto Spark (entrada para toda a funcionalidade do Spark). Coordena a execução de operações de transformação e ação em dados distribuídos;
from awsglue.context import GlueContext
# cria um contexto que permite a interação com o AWS Glu;
from awsglue.dynamicframe import DynamicFrame
# Permite a manipulação de dados de um modo mais flexível;
from pyspark.sql.functions import lit
# Fornece várias funções para manipular dados em DataFrames do Spark;
from awsglue.job import Job
#Importa a classe Job, usada para criar, inicializar e gerenciar Jobs no AWS Glue;
from awsglue.utils import getResolvedOptions
# Usado para obter e resolver opções de execução passadas como argumentos na linha de comando ao iniciar o trabalho do Glue;
from datetime import datetime

#Declarando camino do arquivo CSV a ser tratado
source_file = "s3://data-lake-do-rafael-prado/Raw/Local/CSV/Movies/2024/11/11/movies.csv"

# Inicializando o GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("MoviesJob", {})

# Lendo o arquivo CSV no S3 com delimitador pipe "|"
df = spark.read.option("delimiter", "|").csv(source_file, header=True, inferSchema=True)

# Verificando o esquema do DataFrame
df.printSchema()

# Removendo as colunas desnecessárias
df_tratado = df.drop("titulosMaisConhecidos", "profissao", "anoFalecimento", "anoNascimento", "nomeArtista", "personagem")

# removendo os dados nulos e linhas duplicadas
df_tratado = df_tratado.dropna().dropDuplicates()

if df_tratado.count() == 0:
    raise ValueError("Nenhum dado disponível para salvar no destino.")

# Convertendo o DataFrame de volta para DynamicFrame
dynamic_frame_tratado = DynamicFrame.fromDF(df_tratado, glueContext)

data_atual = datetime.now()
ano = data_atual.strftime('%Y')
mes = data_atual.strftime('%m')
dia = data_atual.strftime('%d')

target_path = f"s3://data-lake-do-rafael-prado/Trusted/CSV/{ano}/{mes}/{dia}/"

# Salvando os dados no S3 em formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_tratado,
    connection_type="s3",
    connection_options={"path": target_path},
    format="parquet"
)

job.commit()


```
--- 

### SCRIPT USADO NO JOB DO JSON:
- Segue abaixo o ``script  usado para a realização do processamento, limpeza, e transformação dos dados do JSON para Parquet``, com sua devida expicação através dos comentários:

```python

import sys
from datetime import datetime
from awsglue.transforms import * 
# Importa todas as transformações disponíveis no AWS Glue para manipulação e transformação de dados;
from awsglue.utils import getResolvedOptions  
# Usado para obter e resolver opções de execução passadas como argumentos na linha de comando ao iniciar o trabalho do Glue;
from pyspark.context import SparkContext
# Importa a classe SparkContext, essencial para criar o contexto Spark, que coordena a execução de operações de transformação e ação em dados distribuídos;
from awsglue.context import GlueContext
# cria um contexto que permite a interação com o AWS Glu;
from awsglue.job import Job
# Importa a classe Job, usada para criar, inicializar e gerenciar Jobs no AWS Glue;
from awsglue.dynamicframe import DynamicFrame
# Permite a manipulação de dados de um modo mais flexível;
from pyspark.sql.functions import col, udf, to_date, when, expr
# Importa várias funções do módulo pyspark.sql.functions, usadas para manipulação de colunas e definição de funções definidas pelo usuário (UDF);
from pyspark.sql.types import ArrayType, StringType
# Importa tipos de dados do PySpark, como ArrayType e StringType, usados para definir esquemas de dados.



# Inicializando o Glue e o job:
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Declarando o caminho do arquivo JSON a ser tratado
source_path = "s3://data-lake-do-rafael-prado/Raw/TMDB/JSON/2024/11/28/"

# Lendo os arquivos JSON em um DynamicFrame:
dynamic_frame_tratado = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_path]},
    format="json"
)

# Transformando o DynamicFrame em um dataframe:
data_frame = dynamic_frame_tratado.toDF()

# Função para extrair apenas os nomes dos gêneros:
def extrair_nomes_dos_generos(genres):
    return [genre['name'] for genre in genres]

# Registrando a função como um UDF (User Defined Function):
extrair_nomes_dos_generos_udf = udf(extrair_nomes_dos_generos, ArrayType(StringType()))

# Aplicando a função UDF à coluna "genres" para extrair apenas os nomes:
data_frame = data_frame.withColumn("genres", extrair_nomes_dos_generos_udf(col("genres")))

# Convertendo a coluna release_date para o formato de data (estava como string)
data_frame = data_frame.withColumn("release_date", to_date(data_frame["release_date"], "yyyy-MM-dd"))

# Colunas que decidi manter nos arquivos:
colunas_manter = ["id", "title", "original_title", "release_date", "runtime", "vote_count", "vote_average", "revenue", "original_language"]
filtered_data_frame = data_frame.select(*colunas_manter)

# Removendo dados nulos e linhas sem title ou original_title:
data_frame_tratado = filtered_data_frame.dropna(subset=["title", "original_title"])
data_frame_tratado = data_frame_tratado.filter(data_frame_tratado.title.isNotNull() & data_frame_tratado.original_title.isNotNull())

# Removendo duplicatas
data_frame_tratado = data_frame_tratado.dropDuplicates(colunas_manter)

# Corrigindo a coluna revenue para conter apenas valores inteiros:
data_frame_tratado = data_frame_tratado.withColumn("revenue", col("revenue.int").cast("int"))
data_frame_tratado = data_frame_tratado.withColumn("revenue", when(col("revenue") == 0, None).otherwise(col("revenue")))

# Convertendo a coluna runtime para valores inteiros:
data_frame_tratado = data_frame_tratado.withColumn("runtime", col("runtime").cast("int"))

# Garantindo que vote_count e vote_average sejam números válidos
data_frame_tratado = data_frame_tratado.withColumn("vote_count", col("vote_count").cast("int"))
data_frame_tratado = data_frame_tratado.withColumn("vote_average", col("vote_average").cast("float"))

# Convertendo o DataFrame limpo de volta para DynamicFrame
dynamic_frame_tratado_transformado = DynamicFrame.fromDF(data_frame_tratado, glueContext, "dynamic_frame_tratado_transformado")

# Definindo o caminho de destino para os arquivos Parquet:
data_atual = datetime.now()
ano = data_atual.strftime('%Y')
mes = data_atual.strftime('%m')
dia = data_atual.strftime('%d')
target_path = f"s3://data-lake-do-rafael-prado/Trusted/JSON/{ano}/{mes}/{dia}/"

# Escrevendo o DynamicFrame transformado no formato Parquet:
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_tratado_transformado,
    connection_type="s3",
    connection_options={"path": target_path},
    format="parquet"
)


job.commit()

```

# ETAPA 2

- Na etapa 2 foi feita a ``execução dos jobs do CSV e do JSON``. Segue a baixo as devidas evidencias de execução corretas:

### Run JOB CSV: 

![x](./..//evidencias/Parquet%20CSV/03_run_job.png)


### Run JOB JSON: 

![x](./..//evidencias/Parquet%20JSON/03_run_job.png)

---

- Em seguida foi feita a confirmação de que os ``CSVs e JSONs foram transformados em Parquets em suas devidas pastas dentro do bucket no AWS S3``:

### Confirmação da transformação do CSV em Parquet:
![x](./..//evidencias/Parquet%20CSV/pos_execucao_job_csv.png)

### Confirmação da transformação do JSON em Parquet:
![x](./..//evidencias/Parquet%20JSON/04_pos_execucao_json.png)

# ETAPA 3

Nesta etapa foi feita a ``criação do Database`` no qual serão armazenadas as tables criadas pelos ``crawlers``. Foi usado o mesmo database tanto para a table do CSV quanto do JSON.

![x](./..//evidencias/Parquet%20JSON/database_usado.png)


# ETAPA 4

- Na etapa 4 foi feita de forma respectiva a ``criação dos Crawlers``, suas ``execuções`` e a ``verificação da criação das tables`` após a execução dos crawlers. segue abaixo as evidências:

### DO CSV:

#### Crawler CSV:
![x](./..//evidencias/Parquet%20CSV/05_crawler_csv.png)

#### Execução do Crawler CSV: 
![x](./..//evidencias/Parquet%20CSV/06_execucao_crawler.png)

#### Criação da table CSV:

![x](./..//evidencias/Parquet%20JSON/tables.png)


### DO JSON:

#### Crawler JSON:
![x](./..//evidencias/Parquet%20JSON/05_crawler_json.png)

#### Execução do Crawler JSON: 
![x](./..//evidencias/Parquet%20JSON/execucao_crawler_json.png)

- obs: foram feitas diversas execuções, até conseguir o resultado esperado.

#### Criação da table CSV:
![x](./..//evidencias/Parquet%20JSON/tables.png)

# ETAPA 5

- Aqui foram realizadas as ``queries para consulta das tables no AWS Athena`` com objetivo de verificar tudo que foi feito no codigo foi efetivamente aplicado. Foram executadas diversas queries, mas para o propósito do desafio será mostrada apenas uma query mais básica.

### Query do Parquet do CSV:

![x](./..//evidencias/Parquet%20CSV/07_query_athena.png)

#### Schema CSV:

![x](./..//evidencias/Parquet%20CSV/08_schema_csv.png)

---


### Query do Parquet do JSON:

![x](./..//evidencias/Parquet%20JSON/06_query_athena.png)

#### Schema JSON:

![x](./..//evidencias/Parquet%20JSON/schema.png)

# Sprint 9


---
### Neste desafio foram feitos 2 scripts para tratar dois arquivos , um referente a uma camada pré-refined, onde foram salvos os arquivos em formato parquet após o cruzamento com o arquivo JSON para filtrar os filmes em comum; e outro script foi feito para fazer o dimensionamento a parte, salvando as dimensões em uma pasta a parte. Os processos são de certa forma semelhantes, e em virtude disso, serão explicados de forma paralela nas etapas.

#

## Etapa 1:

O propósito dos códigos abaixo é realizar uma série de tarefas para preparar o codigo para o estagio final. Os codigos estarão devidamente explicados através dos comentarios nos proprios codigos. Segue abaixo:

### Script dos arquivos da camada Pré-refined:

```python 
import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

# Inicializando contextos Glue e Spark
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Declarando caminhos de entrada
caminho_s3_entrada1 = "s3://data-lake-do-rafael-prado/Trusted/JSON/2024/12/13/"
caminho_s3_entrada2 = "s3://data-lake-do-rafael-prado/Trusted/CSV/2024/12/12/"

# Lendo dados da Trusted em DynamicFrame e convertendo em Dataframe
dados_json = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {"paths": [caminho_s3_entrada1]},
    format = "parquet"
).toDF()

# Lendo dados do CSV em DynamicFrame e convertendo em Dataframe
dados_csv = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {"paths": [caminho_s3_entrada2]},
    format = "parquet"
).toDF()

# Renomeando a coluna "generoArtista" para inglês
dados_csv = dados_csv.withColumnRenamed("generoArtista", "artist_genre")

# Realizando o cruzamento dos arquivos, mantendo os dados do JSON e incluindo algumas colunas do CSV
dados_juntos = dados_json.join(dados_csv, dados_json["title"] == dados_csv["tituloPincipal"], "inner") \
                         .select(dados_json["*"], dados_csv["artist_genre"])

# Agrupando colunas com a coluna "artist_genre" em forma de lista, pois havia um ID's repetidos para os mesmos filmes, caso houvessem Ator e Atriz.
dados_agrupados = dados_juntos.groupBy(
    "id", "title", "original_title", "release_date", "runtime",
    "vote_count", "vote_average", "revenue", "original_language",
    "genres", 'origin_country', 'adult', 'budget', 'popularity'
).agg(
    F.collect_set("artist_genre").alias("artist_genre")
)

# Convertendo 'artist_genre' de array para string para tornar a visualização mais flexível
dados_agrupados = dados_agrupados.withColumn("artist_genre", F.concat_ws(", ", F.col("artist_genre")))

# Convertendo df de volta para DynamicFrame
dynamic_frame_agrupado = DynamicFrame.fromDF(dados_agrupados, glueContext, "dynamic_frame_agrupado")

data_atual = datetime.now()
ano = data_atual.strftime("%Y")
mes = data_atual.strftime("%m")
dia = data_atual.strftime("%d")
caminho_s3_saida = f"s3://data-lake-do-rafael-prado/pre-refined/{ano}/{mes}/{dia}/"

# Salvando dados na pré-refined (staged)
glueContext.write_dynamic_frame.from_options(
    frame = dynamic_frame_agrupado,
    connection_type = "s3",
    connection_options = {"path": caminho_s3_saida},
    format = "parquet"
)

job.commit()
```

### Script das dimensões da camada Refined:

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import explode, split, trim, col, row_number, year, month, dayofmonth, regexp_replace
from pyspark.sql.window import Window

# Inicializando o contexto Glue e Spark
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos de entrada e saída
input_path = "s3://data-lake-do-rafael-prado/pre-refined/2024/12/13/"
output_path = "s3://data-lake-do-rafael-prado/Refined/"

# Lendo o arquivo Parquet em um DynamicFrame
datasource0 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [input_path], "recurse": True},
    transformation_ctx="datasource0"
)
 
# Conversão para DataFrame para manipulação
df = datasource0.toDF()

# Criando Dimensão Filme (Não foi preciso criar ID's para a Dim_Filmes, pois os filmes ja haviam ID proprio, que foram aproveitados.)
dim_filme = df.select('id', 'title', 'original_title', 'runtime', 'original_language', 'adult', 'revenue', 'budget')

# Criando Dimensão Tempo
dim_tempo = df.select('release_date').distinct()
dim_tempo = dim_tempo.withColumn("ano", year(col("release_date"))) \
    .withColumn("mes", month(col("release_date"))) \
    .withColumn("dia", dayofmonth(col("release_date")))
windowSpecTempo = Window.orderBy("release_date") #Usando windowSpec para criar ID's da Dim_Tempo
dim_tempo = dim_tempo.withColumn("id_tempo", row_number().over(windowSpecTempo))

# removendo colchetes da coluna origin_country para que ficasse melhor de visualizar.
df = df.withColumn("origin_country", trim(regexp_replace(col("origin_country"), "\\[|\\]", "")))

# Criando Dimensão Localização
dim_localizacao = df.select(explode(split(col("origin_country"), ",")).alias("localizacao")).distinct()
dim_localizacao = dim_localizacao.withColumn("localizacao", trim(col("localizacao")))
windowSpecLocalizacao = Window.orderBy("localizacao") #Usando windowSpec para criar ID's da Dim_Localizacao
dim_localizacao = dim_localizacao.withColumn("id_localizacao", row_number().over(windowSpecLocalizacao))
dim_localizacao = dim_localizacao.dropDuplicates(["localizacao"])
dim_localizacao = dim_localizacao.filter(col("localizacao") != "")

# Separando siglas da coluna localizacao para ter uma filtragem mais flexivel no AWS QuickSight.
df = df.withColumn("localizacao", explode(split(trim(regexp_replace(col("origin_country"), "\\[|\\]", "")), ",")))

# Criação da Tabela Fato Filme com ID's e alguns dados numéricos.
fato_filme = df.select('id', 'release_date', 'vote_count', 'vote_average', 'popularity', 'localizacao')
fato_filme = fato_filme \
    .join(dim_filme.select('id', col('id').alias('id_filme')), on='id') \
    .join(dim_tempo.select('release_date', 'id_tempo'), on='release_date', how='left') \
    .join(dim_localizacao.select('localizacao', 'id_localizacao'), on='localizacao', how='left')

# Conversão de DataFrame para DynamicFrame para salvar no Glue
dim_filme_dyf = DynamicFrame.fromDF(dim_filme, glueContext, "dim_filme_dyf")
dim_tempo_dyf = DynamicFrame.fromDF(dim_tempo, glueContext, "dim_tempo_dyf")
dim_localizacao_dyf = DynamicFrame.fromDF(dim_localizacao, glueContext, "dim_localizacao_dyf")
fato_filme_dyf = DynamicFrame.fromDF(fato_filme, glueContext, "fato_filme_dyf")

# Salvando as tabelas dimensionais e de fatos no S3
glueContext.write_dynamic_frame.from_options(
    dim_filme_dyf, 
    connection_type="s3", 
    connection_options={"path": f"{output_path}dim_filme/"}, 
    format="parquet"
)
glueContext.write_dynamic_frame.from_options(
    dim_tempo_dyf, 
    connection_type="s3", 
    connection_options={"path": f"{output_path}dim_tempo/"}, 
    format="parquet"
)
glueContext.write_dynamic_frame.from_options(
    dim_localizacao_dyf, 
    connection_type="s3", 
    connection_options={"path": f"{output_path}dim_localizacao/"}, 
    format="parquet"
)
glueContext.write_dynamic_frame.from_options(
    fato_filme_dyf, 
    connection_type="s3", 
    connection_options={"path": f"{output_path}fato_filme/"}, 
    format="parquet"
)

job.commit()

```

### Etapa 2:

- Na etapa 2 foi feita a ``execução dos jobs da Pré-Refined e da Refined``. Segue a baixo as devidas evidencias de execução corretas:

### Run Job camada Pré-Refined:

![x](./..//evidencias/pre-refined-staged/03_job_run.png)

### Run Job camada Refined:

![x](./..//evidencias/refined/03_job_run.png)

- Em seguida foi feita a confirmação de que as pastas da ``Pré-Refined `` e `` Refined`` foram devidamente criadas, estando a ``Pré-Refined`` com os Parquets do cruzamento dos arquivos, e a ``Refined`` com as dimensões presentes na pasta. Segue abaixo:

### Arquivos Pré-Refined:

![x](./..//evidencias/pre-refined-staged/04_arquivos_salvos_bucket.png)

### Dimensões Refined:

![x](./..//evidencias/refined/04_dimensoes_buckets.png)

- Abaixo encontram-se Queries feitas no serviço AWS Athena para confirmar que os arquivos foram criados devidamente, após a criação e execução de seus respectivos crawlers.

### Visualização Pré-Refined:

![x](./..//evidencias/pre-refined-staged/05_visualizacao_pre_refned_athena.png)


### Visualização Dim_Filmes da camada Refined:

![x](./..//evidencias/refined/05_visualizacao_athena_dim_filmes.png)

--- 

- Por fim, foi feito o modelo dimensional no site ``DbDiagram.io``, ja usado anteriormente na Sprint 2. Segue abaixo:

![x](./..//Desafio/entregaveis/modelo_dimensional/dimensionamento_sprint_9.png)

--- 

# Sprint 10
### Passo 1:

O primeiro no serviço AWS QuickSight foi criar criar os datasets. Segue abaixo as evidências:

![x](./..//evidencias/01_01_criando_datasets.png)

Após clicar em new dataset, deve-se selecionar a opção ``"Athena"``:

![x](./..//evidencias/01_02_selecao_athena.png)

Em seguida, dar um nome ao Data Source e clicar em ``Create data source``:

![x](./..//evidencias/01_03_criacao_dataset_pt3.png)

Logo após, deve-se selecionar o database (Job-Trusted):

![x](./..//evidencias/01_04_criacao_dataset_pt4.png)

Após isso, deve-se selecionar as tables, uma a uma (ou seja, todas as dimensões):

![x](./..//evidencias/01_05_criacao_dataset_pt5.png)
![x](./..//evidencias/01_06_criacao_dataset_pt6.png)

E por fim, todos os datasets estão criados.

![x](./..//evidencias/01_07_criacao_dataset_pt7.png)

### Passo 2:

O passo 2 é realizar os joins. Para isso, clica-se primeiro na tabela principal (fato_filme) e após isso em "Edit Dataset"

![x](./..//evidencias/02_realizando_join.png)

Depois disso, fiz os lefts joins à todas as outras tabelas adjacentes;

![x](./..//evidencias/02_02_lefts_joins_fato.png)

### Passo 3

No passo 3 foi criada a análise, clicando no botão "New Analysis"

![x](./..//evidencias/03_criando_analise.png)

Em seguida, deve-se selecionar a fato:

![x](./..//evidencias/04_selecionando_dataset.png)

E depois clicar em "Use in Analisys":

![x](./..//evidencias/05_usando_em_analise.png)

E por fim, esta é a tela onde os graficos foram criados:

![x](./..//evidencias/06_tela_final.png)



## Gráficos e Historytelling:

Era dos Heróis: A Jornada dos Filmes de Ação e Aventura (1970-2020)

Nas décadas de 1970 a 2020, a indústria cinematográfica dos EUA viu uma transformação incrível nos gêneros de ação e aventura. Este período foi marcado pelo nascimento de ícones, evolução de técnicas cinematográficas e a conquista dos corações de espectadores em todo o mundo. 


--- 

### Gráfico 1: Numero de filmes lançados pelos US vs Outros países

A seguir, a pesquisa foi sobre o país dos EUA, tendo a hipotese inicial de que seria uma país destaque por se tratar de uma superpotência, além de ser o país que domina a lingua mundial.

![x](./..//evidencias/graficos/02_lancamento_filmes_eua_vs_outros.png)

- Nota-se que o EUA domina a produção de filmes de Ação e Aventura, produzindo apenas ele mais filmes que todos os filmes dos outros países analisados em questão somados.

### Gráfico 2: Lançamento de Filmes de Ação e Aventura nos EUA

O gráfico mostra a evolução do número de filmes de Ação e Aventura entre 1970 e 2020 nos US. No eixo horizontal (x) estão os anos, e no eixo vertical (y) está a quantidade de filmes lançados por ano.

![x](./..//evidencias/graficos//01_lancamento_filmes_tempo.png)

Segue abaixo alguns insigts que podem ser retirados do gráfico:

- Quanta a tendencia Geral de Crescimento: Há uma tendência geral de crescimento no número de filmes de Ação e Aventura ao longo dos anos. Nos anos 70, o número de filmes era baixo, com alguns picos e vales, mas a partir dos anos 80, há um aumento mais consistente. Existem alguns picos notáveis no número de filmes lançados. Por exemplo, em 1990, houve um pico de 15 filmes, e em 2016, o número chegou a 35 filmes, o maior valor registrado no gráfico.

- Quedas Significativas e Recuperações: Após alguns picos, há quedas significativas. Por exemplo, após o pico de 2009, o número de filmes caiu para 20 em 2010 e 11 em 201, numero que se recuperou após 2 anos para 30 anos.

- Estabilidade nos Anos 90: Durante os anos 90, o número de filmes de Ação e Aventura se manteve relativamente estável, variando entre 7 e 13 filmes por ano.


### Gráfico 3: Lucro vs Orçamento dos filmes lançados pelos US

---

- Após notar a grande significância dos EUA na industria cinematográfica global nos filmes de Ação e Aventura, foi feita a pergunta: Quão grande foi a evolução do lucro em relação ao orçamento nos EUA ao longo de 5 décadas?

![x](./..//evidencias/graficos/04_relacao_lucro_orcamento.png)


- Conclusões: Nota-se que existe uma tendência geral de crescimento tanto no investimento quanto no lucro dos filmes de Ação e Aventura ao longo dos anos. Nos anos 70, os investimentos e lucros eram relativamente baixos, mas a partir dos anos 90, há um aumento mais consistente. Além disso, a partir dos anos 90, notamos uma explosão de rentabilidade. O lucro saltou significativamente em comparação com as décadas anteriores. Por exemplo, o lucro nos anos 90 foi de 1.94 bilhões, muito acima do orçamento de 0.87 bilhões.

- De 2010 a 2020, apesar de na decada anterior ter tido um investimento maior, a decada seguinte lucrou ainda mais, com menos investimento, o que prova o crescimento da industria dos filmes de Ação e Aventura.

- A enorme diminuição de lucro e orçamento na década de 2020 se da porque os dados vão até 01/2020.


### Gráfico 4: Relação da popularidade vs Orçamento nos filmes de Ação e Aventura lançados pelos EUA

Após a analise do lucro vs orçamento, foi decidido fazer uma analise em relação a popularidade dos filmes de Ação & Aventura e a quantidade investida no filme. A hipótese base para a Análise foi: Será mesmo que o orçamento é diretamente proporcional a popularidade? ou seja, quanto mais se investe no filme, mais popular ele é?

![x](./..//evidencias/graficos/05_relacao_popularidade_investimento.png)

- A hipotese acima foi confirmada através da analise da media da popularidade dos filmes e orçamento agrupada por decadas em 5 decadas consecutivas. Nota se que quando há um orçamento menor, a média da popularidade também cai, tomando como exemplo a decada de 2010.

### Gráficos 5 e 6: Comparação entre a relação da evolução da duraçao dos filmes ao longo das decadas e popularidade.

Nos graficos abaixo a intenção foi ainda investigar a relação da popularidade com outros fatores (em filmes de Ação & Aventura nos US), no caso em questão, a duração dos filmes. Para isso foi feita uma análise da evolução da duração dos filmes ao longo das decadas, e em paralelo, a evolução da popularidade dos filmes ao longo dos decadas.


![x](./..//evidencias/graficos/06_grafico_06.png)


Conclusões:
- Através da Analise dos dados, infere-se que não necessariamente há uma correlação, mas é evidente que alguns picos e vales dos graficos merecem atenção. 
- Observa-se que nas decadas de 80 e 90 a média de duração dos filmes não variou consideravelmente, porém, a popularidade cresceu, mesmo com uma pequena mudança nesse fator. De 1990 a 2000, a média de duração caiu, enquanto a popularidade aumentou.
- Na proxima decada a duração dos filmes caiu ainda mais, e de 2000 a 2010 a popularidade caiu imensamente, porém, um fator que influenciou nisso foi o investimento nos filmes, que nesse periodo também diminuiu.
- E por fim, nota-se que entre 2010 e 2020 a duração dos filmes aumentou, enquanto e a popularidade também. Porém, de 2010 a 2020 o orçamento tambem cresceu consideravelmente, ou seja o orçamento influencia muito mais na popularidade do que a duração do filme em si.

### Gráficos 7 e 8: Comparação entre a nota média e popularidade dos 10 filmes mais populares dos US, com seus respectivos orçamentos e lucros.

Abaixo, continuamos nossa analise do fator popularidade. A intenção dos graficos abaixo foi comparar a popularidade dos 10 filmes mais populares desde a decada de 70 até 2020 lançados pelos EUA, com seus lucros e orçamentos. 

![x](./..//evidencias/graficos/08_graficos_08_e_09.png)

Através da análise dos graficos, conclui-se que:

- Não necessariamente a nota de avaliação do filme tem relação com sua popularidade, pois por exemplo o filme "Kung Fu Panda" embora tenha a nota de avaliação menor que o filme "Deadpool", ainda sim tem sua popularidade maior. Analisando os dois mesmos filmes no grafico "Lucro x Orçamento", nota-se que foi investido mais no filme Kung Fu Panda que no filme Deadpool, apesar de o ultimo ter tido maior lucrabilidade. Ou seja, o como foi sugerido antes, o orçamento é o fator que mais influencia na popularidade dos filmes.
- Nota-se tambem que o filme Aquaman, apesar de encontrar-se na oitava posição entre os filmes mais populares dos EUA, é o segundo filme que mais teve lucro, estando em primeiro lugar com a maior bilheteria e popularidade da historia, o filme Avatar - tendo sido seu lucro de 2,92 bi.

--- 
