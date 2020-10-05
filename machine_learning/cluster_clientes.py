import numpy as np
import pandas as pd
from sklearn.cluster import KMeans 
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text, func
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric
from datetime import datetime, timezone
from pytz import timezone
import sys, getopt
import os

# Prepara a conexão as bases de dados SQL
db_orig_url = os.environ['DB_RM_URL']
db_dest_url = os.environ['DB_DW_URL']

# Banco de Dados Origem - (Sql Server - TOTVSRM)
source_engine = create_engine(db_orig_url, echo=False)

# Banco de Dados Destino - (PostgreSQL - SBIDW )
dest_engine = create_engine(db_dest_url, echo=False)
DestSession = sessionmaker(dest_engine)
destSession = DestSession()

horaIni = datetime.now(timezone('America/Campo_Grande'))

print("")
print("--------------------------------------------------------------------")
print(" Extração de Dados e processamento K-Means para classificação       ")
print("--------------------------------------------------------------------")
print("Início em: " + horaIni.strftime("%d/%m/%Y - %H:%M:%S"))
print("")

# Carrega dados base de para análise

sql = '''
    select 
        a.codcfo,
        t01.valorm2

    from
        fcfo a (nolock)
        
    inner join (
        select 
            x.codcolcfo,
            x.codcfo,
            round(sum(z.area),2) area_comprada,
            round(sum(x.valor_venda), 2) valor_compra,
            round(sum(x.valor_venda) / sum(z.area), 2) as valorm2,
            datediff(dd, max(x.dat_venda), getdate()) dias_ult_compra
        from
            xvenda x (nolock)
        inner join
            xitemvenda y (nolock) on
                x.num_venda = y.num_venda
        inner join
            xsubunidade z (nolock) on
                y.cod_pess_empr = z.cod_pess_empr and
                y.num_unid = z.num_unid and
                y.num_sub_unid = z.num_sub_unid
        inner join
            xempreendimento e (nolock) on
                x.cod_pess_empr = e.cod_pess_empr
        where
            x.valor_venda > 5000 and
            z.area > 0 and
            x.cod_pess_empr != 17
        group by 
            x.codcolcfo,
            x.codcfo
            
        ) t01 on
            a.codcoligada = t01.codcolcfo and
            a.codcfo = t01.codcfo
            
    where
        a.codcfo not in (
            '00004711', '00004245', '00005324', '00005491', '00000503', '00002075', '00003077', 
            '00015029', '00006002', '00022574', '00008201', '00013577', '00019459', '00001099',
            '00007969', '00000581', '00000434') and 
        exists (
            select 
                1 
            from
                xvenda x01 (nolock)
            where
                x01.codcolcfo = a.codcoligada and
                x01.codcfo = a.codcfo  
        )
'''

# Carregando a base de dados no dataframe:
df = pd.read_sql(
    sql,
    con=source_engine
)

# Selecionando o número de clusters através do método Elbow (Soma das distâncias quadráticas intra clusters):
X2 = df[['valorm2']].iloc[: , :].values

# Inicializando e Computando o KMeans com o valor de 4 clusters:
print('- Calculando KMeans.')
algorithm = (KMeans(n_clusters = 3))
algorithm.fit(X2)

KMeans(algorithm='auto', copy_x=True, init='k-means++', max_iter=300,
       n_clusters=3, n_init=10, n_jobs=None, precompute_distances='auto',
       random_state=None, tol=0.0001, verbose=0)

df["clusters"] = algorithm.labels_

print('\n- Calculo finalizado. Head do dataframe:')
print(df.head())

# Limpa dados existentes na tabela
destSession.execute('''truncate table staging.clientes_clustering''')
destSession.commit()

# Salvando resultado no banco de dados destino
print('\n- Salvando resultado no banco de dados.')
df.to_sql(
    'clientes_clustering',
    dest_engine,
    schema='staging',
    if_exists='append',
    index=False,
    chunksize=1000,
    dtype={
        "codcfo": String(25),
        "valorm2": Numeric(15,4),
        "clusters": Integer
    }
)

# Estatística descritiva dos grupos:
print('\n- Estatísticas dos clusters criados:')
df_group = df.drop(['codcfo',],axis=1).groupby("clusters")
print(df_group.describe())

horaFim = datetime.now(timezone('America/Campo_Grande'))
duracao = horaFim - horaIni

print("\nTérmino em:  " + horaFim.strftime("%d/%m/%Y - %H:%M:%S"))
print("Tempo Total: " + str(duracao))
print("")


