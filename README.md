# TG Estudo de Caso - Usabilidade AWS para Engenheiro de Dados
# Alunos:
- Diego Luis Oliveira
- Gilberto Cury Netto
- Rafael Nascimento Prado

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
