# airflow-brewery
airflow-brewery


apontar os test_cases fora da pipeline principal
tentei adicionar os testes na pipeline principal, mas isso não é considerada uma
boa prática


# airflow-brewery

Este documento tem como objetivo guiar o study case "Adicionar Study Case" em `Airflow`.

Este projeto foi testado no `Windows 11` com Python `3.9`.

## Estrutura do Projeto

**Item**                                  |**Descrição**
|-----                                    |-----
.storage/                                 |Pasta para armazenar os arquivos de dados persistentes.
dags/                                     |Pasta contendo as dags/fluxos de processamento de dados do `Airflow`.
plugins/                                  |Pasta de plugins e bibliotecas auxiliares.
.env-sample                               |Arquivo de variáveis de ambiente de exemplo.
airflow.cfg                               |Arquivo de configuração do `Airflow` para execução em ambiente local.
docker-compose-swarm                      |Arquivo para execução do container de Produção em Swarm
docker-compose.yml                        |Arquivo para execução dos containers do `Airflow`.
Dockerfile                                |Arquivo de imagem `docker` para criação do containers `Airflow`.
requirements.txt                          |Arquivo contando bibliotecas necessárias para execução do projeto.
webserver_config.py                       |Arquivo de configurações do webserver do Airflow

## Requisitos

* [Python 3.9](https://www.python.org/ftp/python/3.9.0/python-3.9.0-amd64.exe)
* [Python pip](https://www.geeksforgeeks.org/how-to-install-pip-on-windows/)
* [Docker](https://docs.docker.com/get-docker/)
* [Visual Studio Code](https://code.visualstudio.com/download)
    * [Docker Extension for VSCode](https://github.com/microsoft/vscode-docker)
        * _Pode ser instaldo via `VSCode` no painel de `Extensões`.

## Detalhamento das especificações do Study Case

Primeiramente, duplique o arquivo `.env-sample` para `.env`.

Em seguida, crie o pacote de distribuição da biblioteca auxiliar.

```shell
cd plugins/gentileza-framework-dataflow
python3 setup.py bdist_wheel

cd ..
cd ..

cd plugins/gentileza-framework-tools
python3 setup.py bdist_wheel
```

Em seguida, crie a imagem do `Airflow`:

```shell
cd ..
cd ..
docker build -t airflow:developer .
```

Em seguida, suba o `docker-compose` para iniciar os containers do `Airflow`:

```shell
docker-compose up -d
```

Acesse o endereço [http://localhost:8080](http://localhost:8080) no browser e verifique se a interface do `Airflow` é exibida.

Entre com o usuário `airflow` e senha padrão `airflow`.

Verifique se a `DAG - Sample ETC` é exibida.

### Executando a DAG c/ Docker

Para executar e debugar o projeto com `Docker` é necessário abrir o projeto em modo `Dev Container`.

Dentro do `VSCode`, aperte `CTRL+SHIT+P > Dev Containers: Rebuild and Reopen in Container`. 

A janela do `VSCode` será re-aberta com o projeto executando dentro do container de desenvolvimento. Aguarde até que o toda o processo de `build` de container seja concluído (você pode clicar na caixa informativa suspensa do lado inferior direito para acompanhar o processo).

Em seguida, clique no painel esquerdo `Executar e Depurar` do `VSCode`, selecione a opção `Pytest - Tag: _DEV` na caixa suspensa e clique no botão `Iniciar depuração (F5)`. Verifique se a tarefa é executada com sucesso no `console`.

## Configução do ambiente local

Caso queria debugar o projeto localmente, crie um novo ambiente virtual:

```shell
$ python3 -m virtualenv --python=python3.9 .venv
```

Em seguida, instale o `airflow` via `pip`:

```shell
$ pip install 'apache-airflow==2.4.3' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.4.3/constraints-3.9.txt"
 ```

Configure a variável de ambiente do `airflow`:

```shell
export AIRFLOW_HOME=<project_path>
```

Para tornar a variável de ambiente persistente, adicione o comando acima ao arquivo `.bash_profile`.

Ajuste os caminhos no arquivo de configuração `airflow.cfg`:

```
[core]

dags_folder = /<project_path>/dags
plugins_folder = /<project_path>/plugins
sql_alchemy_conn = sqlite:////<project_path>/airflow.db

...

[logging]

base_log_folder = /<project_path>/.logs
dag_processor_manager_log_location = /<project_path>/.logs/dag_processor_manager/dag_processor_manager.log
child_process_log_directory = /<project_path>/.logs/scheduler
```

Caso queira carregar as `dags` de exemplo ajuste a configuração abaixo para `true`:

```
[core]
load_examples = True
```

Após inicialize o banco de dados do `airflow`:

```shell
$ <project_path>/.venv/bin/python <project_path>/.venv/lib/python3.9/site-packages/airflow db init
```

Ou caso esteja utilizando o `VSCode` execute a configuração `Airflow Init DB` na seção `Executar e Depurar (Degug)`.

Após crie o usuário `Admin` do `airflow`:

```shell
<project_path>/.venv/bin/python <project_path>/.venv/lib/python3.9/site-packages/airflow \
    users create \
    --username airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@airflow.org
```

Ou caso esteja utilizando o `VSCode` execute a configuração `Airflow Init Admin User` na seção `Executar e Depurar (Degug)`.

Para que sua `dag` não falhe durante o processo de `debug`, ajuste o arquivo de configuração `airflow.cfg`:

```
# How long before timing out a python file import
dagbag_import_timeout = 0
```

Por fim, instale os pacotes adicionais do projeto:

```shell
pip install -r requeriments.txt
```

### Iniciando o Webserver em ambiente local

Para inicializar a interface gráfica do `airflow` execute:

```shell
$ <project_path>/.venv/bin/python <project_path>/.venv/lib/python3.9/site-packages/airflow webserver
```

Ou caso esteja utilizando o `VSCode` execute a configuração `Airflow Webserver` na seção `Executar e Depurar (Degug)`.

### Executando a DAG em ambiente local

Após inicialize o banco de dados do `airflow`:

```shell
$ <project_path>/.venv/bin/python <project_path>/.venv/lib/python3.9/site-packages/airflow dags test sample_etl YYYY-mm-dd
```

Ou caso esteja utilizando o `VSCode` execute a configuração `Airflow Test Dag` na seção `Executar e Depurar (Degug)`.


> *Esse bloco deve ser removido !<br>
> Blocos em italico servem como guia e devem ser substituidos pelas descrições especificas.<br>
> Valores envolvidos por chaves devem ser substituidos pelos valores especificos da DAG*<br>
## DAG:
*Nome padrão da DAG seguindo o formato padrão com os seguintes componentes (pode variar de acordo com os componentes e necessidades do painel, mas deve-se direcionar pelo padrão apresentado):<br>
{**IDENTIFICACORPROJETO**}\_{**NOMESETOR**}\_{**NOMEPAINEL**}\_{**FONTE**}\_{**NOMERELATORIO**}<br>
Ex: god_dags_controladoria_ciclofinanceiro_gera_pme<br>
Ex: rpa_dags_troca_de_finalizadora*

## Status implantação
![Pipeline Status](https://github.com/gentilnegocios/{**NOME_REPOSITORIO**}/actions/workflows/main-pull.yml/badge.svg)
<br>
![Pipeline Status](https://github.com/gentilnegocios/{**NOME_REPOSITORIO**}/actions/workflows/staging-pull.yml/badge.svg)

## Fontes de dados
Fontes de dados utilizadas na DAG
- {**FONTE 1**}
- {**FONTE 2**}

## O que é
A DAG {**NOME DAG**} é a pipeline responsável pela extração, transformação e persistência nas bases da Gentil Negócios, realizando consultas na {**FONTE DE DADOS**}.

## Observações
*Observações gerais sobre o processo realizado na DAG, informando os nomes das tabelas ou relatórios utilizados e seus respectivos sistemas de origem.
Adicionando uma listagem com conteudos obrigatorios contendo:*<br>
- *O link para a documentação completa da DAG.*<br>
- *A camada final onde a DAG grava os dados processados.*<br>
- *A estrutura de gravação de dados processados na camada final.*<br>

*Exemplo:
A DAG realiza as extrações das tabelas Tabela1 e Tabela2 do sistema A. Posteriormente unifica os dados do Relatório2 extraidos do sistema B.*
- Documentação: {**LINK**}
- Camada final: {**CAMADA FINAL**}
- Diretório: *pasta_raiz/subpasta/ano={ano}/mes={mes}/dia={dia}*

## Visualização
*Prints da DAG da aba graph no Airflow que representem o fluxo completo:<br>
![dag](resources/dag_view_1.png)<br>
![dag](resources/dag_view_2.png)<br>
![dag](resources/dag_view_3.png)<br>
![dag](resources/dag_view_4.png)*