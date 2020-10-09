# TCC - Business Intelligence e Analytics 2020 (PUC Minas)

- Trabalho de Conclusão de Curso apresentado ao Curso de Especialização em Business Intelligence e Analytics como requisito parcial à obtenção do título de especialista.
- Aluno: **Alessandro Cezar Lorençone**
- Data: **Outubro/2020**

***
![](https://raw.githubusercontent.com/alorenco/TCC-PUC-Minas-2020/master/diagrama_fluxo_etl.png?token=ADTFTXA7ELOUYOVBEQOSESC7QCYQI)
***

## Organização do Código Fonte

Este repositório contém os códigos fonte do projeto, organizados da seguinte forma:

1. Pasta: **[scripts](https://github.com/alorenco/TCC-PUC-Minas-2020/tree/master/scripts)**
Contém o script python que fará a **extração** das tabelas de dados selecionadas do ERP Totvs RM que estão no MS SQL Server. O destino será o banco de dados *Data Warehouse* no PostgreSQL.

2. Pasta: **[dbt](https://github.com/alorenco/TCC-PUC-Minas-2020/tree/master/dbt)**
Contém a estrutura do projeto responsável pelas etapas de **transformação** e **carga** das tabelas Fato e Dimensão. Para esta etapa foi utilizado o software **[dbt](https://github.com/fishtown-analytics/dbt)** (data build tool).

3. Pasta: **[machine_learning](https://github.com/alorenco/TCC-PUC-Minas-2020/tree/master/machine_learning)**
Contém o script python responsável por obter os dados de clientes do ERP Totvs RM, aplicar o algorítmo K-Means para classificação e salvar o resultado no banco de dados. Também foram disponibilizados os dados e o arquivo Jupyter Notebook utilizados no estudo.

4. Arquivo: **[docker-compose.yml](https://github.com/alorenco/TCC-PUC-Minas-2020/blob/master/docker-compose.yml)**
Arquivo com as configurações necessárias para a instância docker que executará o software **[Metabase](https://github.com/metabase/metabase)** utilizado para **apresentação** dos dashboards.


# Preparação do Servidor de Homologação

Abaixo seguem as instruções para instalação e execução deste projeto.

## Pré-requisitos:

- Servidor Linux **Ubuntu Server 20.04**.
- Ambiente Docker já instalado e configurado no servidor.
- Acesso ao banco de dados do Totvs RM em MS SQL Server.

## Configurar as Variáveis de Ambiente

```bash
$ sudo nano /etc/environment
````
- Incluir as configurações:

```bash
export POSTGRES_USER=usuario
export POSTGRES_PASS=senha
export DB_RM_URL='mssql+pyodbc://usuario:senha@servidor:1433/TOTVSRM?driver=ODBC+Driver+17+for+SQL+Server'
export DB_DW_URL='postgresql+psycopg2://usuario:senha@servidor/dw'
```

## Instalação do PostgreSQL

```bash
$ sudo apt install postgresql postgresql-contrib
````

- Configurar a senha do usuário *postgres*

```bash
$ sudo su - postgres
$ psql -c "alter user postgres with password 'postgres'"
$ sudo systemctl restart postgresql
````

- Criar os bancos de dados do projeto.

```bash
$ psql -c "create database metabase"
$ psql -c "create database dw"
$ exit
````

## Instalação do DBT

```bash
$ sudo apt-get install git libpq-dev python-dev python3-pip
$ sudo pip3 install dbt
$ dbt --version
````

- Configurar o arquivo de perfil para acesso aos dados.

```bash
$ mkdir ~/.dbt/
$ cp ~/TCC-PUC-Minas-2020/dbt/profiles.yml ~/.dbt/
````

## Instalação do Driver para conexão do MS SQL Server:

```bash
$ sudo su
$ curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
$ curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
$ apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev
$ apt-get update 
$ apt-get install -y locales 
$ echo "en_US.UTF-8 UTF-8" > /etc/locale.gen
````

## Clonar o repositório do projeto e instalar requisitos

```bash
$ git clone https://github.com/alorenco/TCC-PUC-Minas-2020.git
$ cd TCC-PUC-Minas-2020
$ sudo pip3 install -r requirements.txt
````

## Iniciar e acessar o servidor Metabase

```bash
$ cd TCC-PUC-Minas-2020
$ docker-compose up -d
````

## Executar o processo de ETL

- Os comandos abaixo realizam o processo completo de ETL, e podem ser agendados.

```bash
$ cd ~/TCC-PUC-Minas-2020/scripts/
$ python3 extract_totvs_rm.py
$ cd ~/TCC-PUC-Minas-2020/machine_learning/
$ python3 cluster_clientes.py
$ cd ~/TCC-PUC-Minas-2020/dbt/
$ dbt seed
$ dbt run
````

## Agendamento do processo de ETL

- Para o agendamento é possível usar o CRON no ambiente linux:

```bash
$ sudo crontab -e
````

Cole o conteúdo abaixo para agendar os scripts:

```bash
# m h  dom mon dow   command
01 23 * * * /usr/bin/python3 /home/<usuario>/TCC-PUC-Minas-2020/scripts/extract_totvs_rm.py >> /var/log/extract_totvs.log 2>&1
15 23 * * * /usr/bin/python3 /home/<usuario>/TCC-PUC-Minas-2020/machine_learning/cluster_clientes.py >> /var/log/cluster_clientes.log 2>&1
17 23 * * * cd /home/<usuario>/TCC-PUC-Minas-2020/dbt && /usr/local/bin/dbt run --profiles-dir /home/<usuario>/.dbt >> /var/log/dbt.log 2>&1
````


