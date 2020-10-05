"""
Autor: Alessandro Lorençone
Data:  08/09/2020

Script para extração de dados base do SQL Server que hospeda o banco 
do Totvs RM para o Postgresql local
"""

from models_totvs_rm import Coligada, ClienteFornecedor, ClienteDadosAdicionais
from models_totvs_rm import ConsistenciaCampos, Municipio, Empreendimento, Quadra
from models_totvs_rm import Lote, SituacaoVenda, Venda, VendaRegraComponente
from models_totvs_rm import ParcelaVenda, ItemVenda, CompradorVenda, CorretorVenda
from models_totvs_rm import Lancamento, LancamentoIntegracao, LancamentoBaixa, ExtratoCaixa
from models_totvs_rm import ModalidadeVenda, Distrato, MotivoDistrato, TabelaPrecoLote
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text, func
from datetime import datetime, timezone
from pytz import timezone
from faker import Faker
import sys, getopt
import os

fake = Faker('pt_BR')

Base = declarative_base(metadata=MetaData(schema='staging'))


##################################################
# GCOLIGADA
##################################################
def migra_coligada(sourceSession, destSession):

    tabela = "GCOLIGADA"

    sql = '''
        select 
            codcoligada, 
            nome,
            nomefantasia,
            cidade,
            estado,
            cep
        from
            gcoligada (nolock)
        where 
            codcoligada in (1, 4, 5, 6, 8, 9, 10, 11, 12, 16)  and
            {}
    '''

    query = sourceSession.query(Coligada).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.gcoligada (
            codcoligada, 
            nome,
            nomefantasia,
            cidade,
            estado,
            cep
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            f'\'{fake.company()}\'',
            f'\'{fake.company()}\'',
            f'\'{rowd["cidade"]}\'' if rowd["cidade"] else 'Null',
            f'\'{rowd["estado"]}\'' if rowd["estado"] else 'Null',
            f'\'{rowd["cep"]}\'' if rowd["cep"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# FCFO
##################################################
def migra_cliente_fornecedor(sourceSession, destSession):

    tabela = "FCFO"

    sql = '''
        select
            fcfo.codcoligada,
            fcfo.codcfo,
            nome,
            nomefantasia,
            fcfo.bairro,
            fcfo.cidade,
            fcfo.codetd,
            fcfo.cep,
            fcfo.codmunicipio,
            fcfo.dtnascimento,
            fcfocompl.grpven,
            fcfo.pessoafisoujur
        from 
            fcfo (nolock)
            left join fcfocompl (nolock) 
              on fcfo.codcoligada = fcfocompl.codcoligada and fcfo.codcfo = fcfocompl.codcfo
        where
            {}
    '''

    query = sourceSession.query(ClienteFornecedor).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.fcfo (
            codcoligada,
            codcfo,
            nome,
            nomefantasia,
            bairro,
            cidade,
            codetd,
            cep,
            codmunicipio,
            dtnascimento,
            grpven
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            f'\'{rowd["codcfo"]}\'',
            f'\'{sanitize_str(fake.company())}\'' if rowd["pessoafisoujur"] == 'J' else f'\'{sanitize_str(fake.name())}\'',
            f'\'{sanitize_str(fake.company())}\'' if rowd["pessoafisoujur"] == 'J' else f'\'{sanitize_str(fake.name())}\'',
            f'\'{sanitize_str(rowd["bairro"])}\'' if rowd["bairro"] else 'Null',
            f'\'{sanitize_str(rowd["cidade"])}\'' if rowd["cidade"] else 'Null',
            f'\'{rowd["codetd"]}\'' if rowd["codetd"] else 'Null',
            f'\'{rowd["cep"]}\'' if rowd["cep"] else 'Null',
            f'\'{rowd["codmunicipio"]}\'' if rowd["codmunicipio"] else 'Null',
            f'\'{rowd["dtnascimento"]}\'' if rowd["dtnascimento"] else 'Null',
            f'\'{sanitize_str(rowd["grpven"])}\'' if rowd["grpven"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XCLIENTEPESSOAFISICA
##################################################
def migra_cliente_dados_adicionais(sourceSession, destSession):

    tabela = "XCLIENTEPESSOAFISICA"

    sql = '''
        select 
            xclientepessoafisica.codcoligada,
            xclientepessoafisica.codcfo,
            xprofissao.nom_prof,
            xestadocivil.dsc_est_civ,
            xclientepessoafisica.cod_sexo,
            xclientepessoafisica.nat_cfo,
            xclientepessoafisica.pg_aluguel,
            xclientepessoafisica.casa_propria,
            isnull(xclientepessoafisica.empr_renda, 0) as empr_renda
            
        from xclientepessoafisica (nolock)
        left join xprofissao (nolock) on xclientepessoafisica.cod_prof = xprofissao.cod_prof
        left join xestadocivil (nolock) on xclientepessoafisica.cod_est_civ = xestadocivil.cod_est_civ

        where
            xclientepessoafisica.codcfo <> '00000000' and 
            {}
    '''

    query = sourceSession.query(ClienteDadosAdicionais).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xclientepessoafisica (
            codcoligada,
            codcfo,
            nom_prof,
            dsc_est_civ,
            cod_sexo,
            nat_cfo,
            pg_aluguel,
            casa_propria,
            empr_renda
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            f'\'{rowd["codcfo"]}\'',
            f'\'{sanitize_str(rowd["nom_prof"])}\'' if rowd["nom_prof"] else 'Null',
            f'\'{sanitize_str(rowd["dsc_est_civ"])}\'' if rowd["dsc_est_civ"] else 'Null',
            f'\'{sanitize_str(rowd["cod_sexo"])}\'' if rowd["cod_sexo"] else 'Null',
            f'\'{sanitize_str(rowd["nat_cfo"])}\'' if rowd["nat_cfo"] else 'Null',
            f'\'{sanitize_str(rowd["pg_aluguel"])}\'' if rowd["pg_aluguel"] else 'Null',
            f'\'{sanitize_str(rowd["casa_propria"])}\'' if rowd["casa_propria"] else 'Null',
            rowd["empr_renda"] 
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# GCONSIST
##################################################
def migra_consistencia_campos(sourceSession, destSession):

    tabela = "GCONSIST"

    sql = '''
        select
            codcoligada,
            aplicacao,
            codtabela,
            codcliente,
            codinterno,
            descricao
        from 
            gconsist (nolock)
        where
            {}
    '''

    query = sourceSession.query(ConsistenciaCampos).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.gconsist (
            codcoligada,
            aplicacao,
            codtabela,
            codcliente,
            codinterno,
            descricao
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            f'\'{sanitize_str(rowd["aplicacao"])}\'' if rowd["aplicacao"] else 'Null',
            f'\'{sanitize_str(rowd["codtabela"])}\'' if rowd["codtabela"] else 'Null',
            f'\'{sanitize_str(rowd["codcliente"])}\'' if rowd["codcliente"] else 'Null',
            f'\'{sanitize_str(rowd["codinterno"])}\'' if rowd["codinterno"] else 'Null',
            f'\'{sanitize_str(rowd["descricao"])}\'' if rowd["descricao"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# GMUNICIPIO
##################################################
def migra_municipio(sourceSession, destSession):

    tabela = "GMUNICIPIO"

    sql = '''
        select 
            gmunicipio.codmunicipio,
            gmunicipio.codetdmunicipio,
            gmunicipio.nomemunicipio,
            dcodificacaomunicipio.codigo AS codibge
            
        from gmunicipio (nolock)

        inner join dcodificacaomunicipio (nolock) 
          on gmunicipio.codmunicipio = dcodificacaomunicipio.codmunicipio 
         and gmunicipio.codetdmunicipio = dcodificacaomunicipio.codetdmunicipio
         and dcodificacaomunicipio.idclassifmunicipio = 1
    '''

    query = sourceSession.query(Municipio).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.gmunicipio (
            codmunicipio,
            codetdmunicipio,
            nomemunicipio,
            codibge
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {})".format(
            f'\'{rowd["codmunicipio"]}\'',
            f'\'{rowd["codetdmunicipio"]}\'',
            f'\'{sanitize_str(rowd["nomemunicipio"])}\'' if rowd["nomemunicipio"] else 'Null',
            f'\'{sanitize_str(rowd["codibge"])}\'' if rowd["codibge"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XEMPREENDIMENTO
##################################################
def migra_empreendimento(sourceSession, destSession):

    tabela = "XEMPREENDIMENTO"

    sql = '''
        select
            cod_pess_empr,
            codcoligada,
            case cod_pess_empr
                when 1   then 'Parque da Flores I'
                when 2   then 'Parque Ecológico I '
                when 3   then 'Vila Alegre I'
                when 4   then 'Vila Alegre II'
                when 5   then 'Parque Verde I'
                when 6   then 'Jardim Oasis I'
                when 7   then 'Jardim Oasis II'
                when 8   then 'Parque Ecológico II'
                when 9   then 'Parque Ecológico III'
                when 10  then 'Parque Ecológico IV'
                when 11  then 'Jardim das Tulherias'
                when 12  then 'All Park Residence'
                when 13  then 'Parque dos Ventos I'
                when 14  then 'Parque Verde I'
                when 15  then 'Jardim dos Ipês I'
                when 16  then 'Parque das Rosa I'
                when 17  then 'Parque Goias I'
                when 18  then 'Jardim das Paineiras I'
                when 19  then 'Residencial Veneza I'
                when 20  then 'Jardim Alcântara I'
                when 21  then 'Jardim das Acácias I'
                when 22  then 'Residencial Detroit'
                when 23  then 'Parque dos Ventos II'
                when 24  then 'Vila Cecília I'
                when 25  then 'Parque Imperial'
                when 26  then 'Parque Verde II'
                when 27  then 'Jardim dos Ipês II'
                when 28  then 'Jardim Bueno I'
                when 29  then 'Residencial Neo Home'
                when 30  then 'Eco Parque Logoa'
                when 31  then 'Jardim das Paineiras II'
                when 32  then 'Residencial Boa Vista'
                when 33  then 'Vila Cecília II'
                when 34  then 'Jardim Alcântara II'
                else ''
            end as nome,
            cidade,
            uf,
            cep,
            codmunicipio
        from 
            xempreendimento (nolock)
        where
            cod_pess_empr <> 0 and
            {}
    '''

    query = sourceSession.query(Empreendimento).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xempreendimento (
            cod_pess_empr,
            codcoligada,
            nome,
            nomefantasia,
            cidade,
            uf,
            cep,
            codmunicipio
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {}, {}, {})".format(
            rowd["cod_pess_empr"], 
            rowd["codcoligada"], 
            f'\'{sanitize_str(rowd["nome"])}\'' if rowd["nome"] else 'Null',
            f'\'{sanitize_str(rowd["nome"])}\'' if rowd["nome"] else 'Null',
            f'\'{sanitize_str(rowd["cidade"])}\'' if rowd["cidade"] else 'Null',
            f'\'{rowd["uf"]}\'' if rowd["uf"] else 'Null',
            f'\'{rowd["cep"]}\'' if rowd["cep"] else 'Null',
            f'\'{rowd["codmunicipio"]}\'' if rowd["codmunicipio"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XUNIDADE
##################################################
def migra_quadra(sourceSession, destSession):

    tabela = "XUNIDADE"

    sql = '''
        select
            cod_pess_empr,
            num_unid,
            dsc_local,
            isnull(qtd_sub_unid, 0) as qtd_sub_unid
        from
            xunidade (nolock)
        where
            cod_pess_empr <> 0 and
            {}
    '''

    query = sourceSession.query(Quadra).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xunidade (
            cod_pess_empr,
            num_unid,
            dsc_local,
            qtd_sub_unid
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {})".format(
            rowd["cod_pess_empr"], 
            f'\'{rowd["num_unid"]}\'',
            f'\'{sanitize_str(rowd["dsc_local"])}\'' if rowd["dsc_local"] else 'Null',
            rowd["qtd_sub_unid"]
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XSUBUNIDADE
##################################################
def migra_lote(sourceSession, destSession):

    tabela = "XSUBUNIDADE"

    sql = '''
        select
            a.cod_pess_empr,
            a.num_unid,
            a.num_sub_unid,
            substring(a.descricao, 1, 254) as descricao,
            isnull(a.area, 0) as area,
            a.cod_sit_sub_unid,
            b.dsc_sit_sub_unid,
            cod_prefeitura,
            isnull(codcoltabprecosubunid, 0) as codcoltabprecosubunid,
            isnull(numtabprecosubunid, 0) as numtabprecosubunid          
        from
            xsubunidade a (nolock)
            left join xsituacaosubunidade b (nolock) on a.cod_sit_sub_unid = b.cod_sit_sub_unid
        where
            cod_pess_empr <> 0 and
            {}
    '''

    query = sourceSession.query(Lote).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xsubunidade (
            cod_pess_empr,
            num_unid,
            num_sub_unid,
            descricao,
            area,
            cod_sit_sub_unid,
            dsc_sit_sub_unid,
            cod_prefeitura,
            codcoltabprecosubunid,
            numtabprecosubunid            
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            rowd["cod_pess_empr"], 
            f'\'{rowd["num_unid"]}\'',
            f'\'{rowd["num_sub_unid"]}\'',
            f'\'{sanitize_str(rowd["descricao"])}\'' if rowd["descricao"] else 'Null',
            rowd["area"],
            rowd["cod_sit_sub_unid"],
            f'\'{sanitize_str(rowd["dsc_sit_sub_unid"])}\'' if rowd["dsc_sit_sub_unid"] else 'Null',
            f'\'{sanitize_str(rowd["cod_prefeitura"])}\'' if rowd["cod_prefeitura"] else 'Null',
            rowd["codcoltabprecosubunid"],
            rowd["numtabprecosubunid"]
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XTABPRECOSUBUNIDADE
##################################################
def migra_tabela_preco_lote(sourceSession, destSession):

    tabela = "XTABPRECOSUBUNIDADE"

    sql = '''
        select 
            codcoltabpreco,
            numtabpreco,
            codpessempr,
            numunid,
            numsubunid,
            precosubunid
        from 
            xtabprecosubunidade (nolock)
        where
            {}
    '''

    query = sourceSession.query(TabelaPrecoLote).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xtabprecosubunidade (
            codcoltabpreco,
            numtabpreco,
            codpessempr,
            numunid,
            numsubunid,
            precosubunid
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {})".format(
            rowd["codcoltabpreco"], 
            rowd["numtabpreco"], 
            rowd["codpessempr"], 
            f'\'{rowd["numunid"]}\'',
            f'\'{rowd["numsubunid"]}\'',
            rowd["precosubunid"]
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XSITUACAOVENDA
##################################################
def migra_situacao_venda(sourceSession, destSession):

    tabela = "XSITUACAOVENDA"

    sql = '''
        select 
            cod_sit_venda,
            dsc_sit_venda
        from 
            xsituacaovenda (nolock)
        where 
            {}
    '''

    query = sourceSession.query(SituacaoVenda).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xsituacaovenda (
            cod_sit_venda,
            dsc_sit_venda
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {})".format(
            rowd["cod_sit_venda"], 
            f'\'{rowd["dsc_sit_venda"]}\''
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XVENDA
##################################################
def migra_venda(sourceSession, destSession):

    tabela = "XVENDA"

    sql = '''
        select 
            xvenda.num_venda,
            xvenda.codcoligada,
            xvenda.num_cont,
            xvenda.cod_pess_empr,
            xvenda.dat_venda,
            xvenda.dat_cancela,
            xvenda.dat_quit,
            xvenda.codcfo,
            xvenda.cod_sit_venda,
            xvenda.cod_mod_venda,	
            xvendacompl.tipven

        from xvenda (nolock)

        left join xvendacompl (nolock) 
          on xvenda.codcoligada = xvendacompl.codcoligada 
         and xvenda.num_venda = xvendacompl.numvenda

        where 
            {}
    '''

    query = sourceSession.query(Venda).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xvenda (
            num_venda,
            codcoligada,
            num_cont,
            cod_pess_empr,
            dat_venda,
            dat_cancela,
            dat_quit,
            codcfo,
            cod_sit_venda,	
            cod_mod_venda,
            tipven
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            rowd["num_venda"], 
            rowd["codcoligada"], 
            rowd["num_cont"] if rowd["num_cont"] else 'Null', 
            rowd["cod_pess_empr"] if rowd["cod_pess_empr"] else 'Null', 
            f'\'{rowd["dat_venda"]}\'' if rowd["dat_venda"] else 'Null',
            f'\'{rowd["dat_cancela"]}\'' if rowd["dat_cancela"] else 'Null',
            f'\'{rowd["dat_quit"]}\'' if rowd["dat_quit"] else 'Null',
            f'\'{sanitize_str(rowd["codcfo"])}\'' if rowd["codcfo"] else 'Null',
            rowd["cod_sit_venda"] if rowd["cod_sit_venda"] else 'Null', 
            rowd["cod_mod_venda"] if rowd["cod_mod_venda"] else 'Null', 
            f'\'{sanitize_str(rowd["tipven"])}\'' if rowd["tipven"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XREGRACOMPONENTEVENDA
##################################################
def migra_venda_regra_compn(sourceSession, destSession):

    tabela = "XREGRACOMPONENTEVENDA"

    sql = '''
        select 
            num_venda,
            cod_compn,
            cod_grupo,
            isnull(vr_tx_per, 0) vr_tx_per,
            isnull(data_base, '1900-01-01') data_base,
            isnull(dat_reaj_ind, '1900-01-01') dat_reaj_ind,
            isnull(cod_grupo_ref, 0) cod_grupo_ref
        from 
            xregracomponentevenda (nolock)
        where
            {}
    '''

    query = sourceSession.query(VendaRegraComponente).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xregracomponentevenda (
            num_venda,
            cod_compn,
            cod_grupo,
            vr_tx_per,
            data_base,
            dat_reaj_ind,
            cod_grupo_ref
        ) 
        VALUES {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, '{}', '{}', {})".format(
            rowd["num_venda"], 
            rowd["cod_compn"], 
            rowd["cod_grupo"], 
            rowd["vr_tx_per"], 
            rowd["data_base"], 
            rowd["dat_reaj_ind"], 
            rowd["cod_grupo_ref"]
        )

        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XVENDAPARCELA
##################################################
def migra_parcela_venda(sourceSession, destSession):

    tabela = "XVENDAPARCELA"

    sql = '''
        select 
            xvendaparcela.codcoligada,
            xvendaparcela.numvenda,
            xvendaparcela.codgrupo,
            xvendaparcela.numparc,
            xvendaparcela.codtipoparc,
            xvendaparcela.codcollan,
            xvendaparcela.idlan, 
            isnull(xvendaparcela.numnotapromissoria, '') numnotapromissoria
        from 
            xvendaparcela (nolock)

        inner join flan (nolock) 
            on xvendaparcela.codcollan = flan.codcoligada 
           and xvendaparcela.idlan = flan.idlan
           and flan.statuslan != 2 

        where
            {}
    '''

    query = sourceSession.query(ParcelaVenda).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xvendaparcela (
            codcoligada, 
            numvenda, 
            codgrupo, 
            numparc, 
            codtipoparc, 
            codcollan, 
            idlan, 
            numnotapromissoria
        ) 
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {}, {}, '{}')".format(
            rowd["codcoligada"], 
            rowd["numvenda"], 
            rowd["codgrupo"], 
            rowd["numparc"], 
            rowd["codtipoparc"], 
            rowd["codcollan"], 
            rowd["idlan"], 
            rowd["numnotapromissoria"]
        )

        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XITEMVENDA
##################################################
def migra_item_venda(sourceSession, destSession):

    tabela = "XITEMVENDA"

    sql = '''
        select 
            num_venda,
            cod_pess_empr,
            num_unid,
            num_sub_unid,
            isnull(vr_item, 0) as vr_item,
            isnull(vr_desc, 0) as vr_desc,
            isnull(vr_acrescimo, 0) as vr_acrescimo
        from
            xitemvenda (nolock)
        where
            {}
    '''

    query = sourceSession.query(ItemVenda).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xitemvenda (
            num_venda,
            cod_pess_empr,
            num_unid,
            num_sub_unid,
            vr_item,
            vr_desc,
            vr_acrescimo
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {}, {})".format(
            rowd["num_venda"], 
            rowd["cod_pess_empr"] if rowd["cod_pess_empr"] else 'Null', 
            f'\'{rowd["num_unid"]}\'' if rowd["num_unid"] else 'Null',
            f'\'{rowd["num_sub_unid"]}\'' if rowd["num_sub_unid"] else 'Null',
            rowd["vr_item"], 
            rowd["vr_desc"], 
            rowd["vr_acrescimo"]
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XCOMPRADOR
##################################################
def migra_comprador_venda(sourceSession, destSession):

    tabela = "XCOMPRADOR"

    sql = '''
        select
            codcoligada,
            num_venda,
            idcomprador,
            codcolcfo,
            codcfo,
            principal,
            participacao,
            dataentradaplano,
            datasaidaplano
        from 
            xcomprador (nolock)
        where 
            {}
    '''

    query = sourceSession.query(CompradorVenda).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xcomprador (
            codcoligada,
            num_venda,
            idcomprador,
            codcolcfo,
            codcfo,
            principal,
            participacao,
            dataentradaplano,
            datasaidaplano
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            rowd["num_venda"], 
            rowd["idcomprador"], 
            rowd["codcolcfo"], 
            f'\'{rowd["codcfo"]}\'',
            rowd["principal"] if rowd["principal"] else 'Null', 
            rowd["participacao"] if rowd["participacao"] else 'Null', 
            f'\'{rowd["dataentradaplano"]}\'' if rowd["dataentradaplano"] else 'Null',
            f'\'{rowd["datasaidaplano"]}\'' if rowd["datasaidaplano"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XVENDEDOR
##################################################
def migra_corretor_venda(sourceSession, destSession):

    tabela = "XVENDEDOR"

    sql = '''
        select 
            codcoligada,
            num_venda,
            idvendedor,
            isnull(codcolcfo, 0) as codcolcfo,
            codcfo
        from
            xvendedor (nolock)
    '''

    query = sourceSession.query(CorretorVenda).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xvendedor (
            codcoligada,
            num_venda,
            idvendedor,
            codcolcfo,
            codcfo
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            rowd["num_venda"], 
            rowd["idvendedor"], 
            rowd["codcolcfo"], 
            f'\'{rowd["codcfo"]}\'' if rowd["codcfo"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XMODALIDADEVENDA
##################################################
def migra_modalidade_venda(sourceSession, destSession):

    tabela = "XMODALIDADEVENDA"

    sql = '''
        select 
            cod_mod_venda,
            dsc_mod_venda
        from 
            xmodalidadevenda (nolock)
        where
            {}
    '''

    query = sourceSession.query(ModalidadeVenda).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xmodalidadevenda (
            cod_mod_venda,
            dsc_mod_venda
        ) 
        VALUES {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {})".format(
            rowd["cod_mod_venda"], 
            f'\'{rowd["dsc_mod_venda"]}\'' if rowd["dsc_mod_venda"] else 'Null'
        )

        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XDISTRATO
##################################################
def migra_distrato(sourceSession, destSession):

    tabela = "XDISTRATO"

    sql = '''
        select
            numvenda,
            databasedistrato,
            codtipodistrato,
            codmotivodistrato
        from 
            xdistrato (nolock)
        where 
            {}
    '''

    query = sourceSession.query(Distrato).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xdistrato (
            numvenda,
            databasedistrato,
            codtipodistrato,
            codmotivodistrato
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {})".format(
            rowd["numvenda"], 
            f'\'{rowd["databasedistrato"]}\'' if rowd["databasedistrato"] else 'Null',
            rowd["codtipodistrato"] if rowd["codtipodistrato"] else 'Null', 
            rowd["codmotivodistrato"] if rowd["codmotivodistrato"] else 'Null' 
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# XMOTIVODISTRATO
##################################################
def migra_motivo_distrato(sourceSession, destSession):

    tabela = "XMOTIVODISTRATO"

    sql = '''
        select
            codmotivodistrato,
            dscmotivodistrato
        from 
            xmotivodistrato (nolock)
        where 
            {}
    '''

    query = sourceSession.query(MotivoDistrato).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.xmotivodistrato (
            codmotivodistrato,
            dscmotivodistrato
        )
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {})".format(
            rowd["codmotivodistrato"], 
            f'\'{rowd["dscmotivodistrato"]}\'' if rowd["dscmotivodistrato"] else 'Null'
        )
        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# FLAN
##################################################
def migra_lancamento(sourceSession, destSession):

    tabela = "FLAN"

    sql = '''
        select 
            codcoligada,
            idlan,
            pagrec,
            statuslan,
            codaplicacao,
            codtdo,
            isnull(datavencimento, '1900-01-01') datavencimento,
            valororiginal,
            valorjuros,
            valordesconto,
            valoracrescimoacordo,
            valorjurosacordo,
            valorop1,
            valorop3,
            isnull(baixapendente, 0) baixapendente
        from 
            flan (nolock)
        where
            statuslan != 2 and 
            {}
    '''

    query = sourceSession.query(Lancamento).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.flan (
            codcoligada,
            idlan,
            pagrec,
            statuslan,
            codaplicacao,
            codtdo,
            datavencimento,
            valororiginal,
            valorjuros,
            valordesconto,
            valoracrescimoacordo,
            valorjurosacordo,
            valorop1,
            valorop3,
            baixapendente
        ) 
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            rowd["idlan"], 
            rowd["pagrec"], 
            rowd["statuslan"], 
            rowd["codaplicacao"], 
            rowd["codtdo"], 
            rowd["datavencimento"],
            rowd["valororiginal"], 
            rowd["valorjuros"],
            rowd["valordesconto"],
            rowd["valoracrescimoacordo"],
            rowd["valorjurosacordo"],
            rowd["valorop1"],
            rowd["valorop3"],
            rowd["baixapendente"]
        )

        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# FLANINTEGRACAO
##################################################
def migra_lancamento_integracao(sourceSession, destSession):

    tabela = "FLANINTEGRACAO"

    sql = '''
        select
            flanintegracao.codcoligada,
            flanintegracao.idcampo,
            flanintegracao.idlan,
            flanintegracao.valor,
            xcomponentevrintegracao.codcompn
        from 
            flanintegracao (nolock)

        inner join flan (nolock) 
            on flanintegracao.codcoligada = flan.codcoligada 
           and flanintegracao.idlan = flan.idlan
           and flan.statuslan != 2 
        
        left join xcomponentevrintegracao (nolock)
            on flanintegracao.codcoligada = xcomponentevrintegracao.codcoligada
           and flanintegracao.idcampo = xcomponentevrintegracao.idcampo

        where
            flanintegracao.valor > 0 and 
            {}
    '''

    query = sourceSession.query(LancamentoIntegracao).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.flanintegracao (
            codcoligada,
            idcampo,
            idlan,
            valor,
            codcompn
        ) 
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            rowd["idcampo"], 
            rowd["idlan"], 
            rowd["valor"],
            rowd["codcompn"]
        )

        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# FLANBAIXA
##################################################
def migra_lancamento_baixa(sourceSession, destSession):

    tabela = "FLANBAIXA"

    sql = '''
        select
            codcoligada,
            idlan,
            idbaixa,
            isnull(databaixa, '1900-01-01') databaixa,
            isnull(codcolxcx, 0) codcolxcx,
            isnull(idxcx, 0) idxcx,
            status,
            valorbaixa,
            valormulta,
            valorjuros,
            valordesconto,
            valorvinculado,
            valornotacreditoadiantamento,
            valordevolucao,
            valornotacredito,
            valorperdafinanceira
        from 
            flanbaixa (nolock)
        where
            status != 1 and 
            {}
    '''

    query = sourceSession.query(LancamentoBaixa).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.flanbaixa (
            codcoligada,
            idlan,
            idbaixa,
            databaixa,
            codcolxcx,
            idxcx,
            status,
            valorbaixa,
            valormulta,
            valorjuros,
            valordesconto,
            valorvinculado,
            valornotacreditoadiantamento,
            valordevolucao,
            valornotacredito,
            valorperdafinanceira
        ) 
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            rowd["codcoligada"], 
            rowd["idlan"], 
            rowd["idbaixa"], 
            rowd["databaixa"], 
            rowd["codcolxcx"], 
            rowd["idxcx"], 
            rowd["status"], 
            rowd["valorbaixa"],
            rowd["valormulta"],
            rowd["valorjuros"],
            rowd["valordesconto"],
            rowd["valorvinculado"],
            rowd["valornotacreditoadiantamento"],
            rowd["valordevolucao"],
            rowd["valornotacredito"],
            rowd["valorperdafinanceira"]
        )

        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# FXCX
##################################################
def migra_extrato_caixa(sourceSession, destSession):

    tabela = "FXCX"

    sql = '''
        select 
			codcoligada,
			idxcx,
			codcolcxa,
			codcxa,		
			tipo,
			compensado,
			reconciliado,
			tipocustodia,
			valor,
			isnull(data, '1900-01-01') data,
			isnull(datacompensacao, '1900-01-01') datacompensacao,
			isnull(datavencimento, '1900-01-01') datavencimento
        from 
            fxcx (nolock)
        where
            compensado = 1 and 
            {}
    '''

    query = sourceSession.query(ExtratoCaixa).from_statement(text(sql.format("0 = 0"))).yield_per(10000)
    destSession.execute('''truncate table staging.{0}'''.format(tabela.lower()))

    comando_insert = '''
        insert into staging.fxcx (
			codcoligada,
			idxcx,
			codcolcxa,
			codcxa,		
			tipo,
			compensado,
			reconciliado,
			tipocustodia,
			valor,
			data,
			datacompensacao,
			datavencimento
        ) 
        values {}
    '''

    buffer = []
    lines = 0
    for row in query:
        if len(buffer) > 10000:
            destSession.execute(comando_insert.format(",".join(buffer)))
            buffer.clear()
        rowd = row.__dict__
        val_str = "({}, {}, {}, '{}', {}, {}, {}, {}, {}, '{}', '{}', '{}')".format(
            rowd["codcoligada"], 
            rowd["idxcx"], 
            rowd["codcolcxa"], 
            rowd["codcxa"], 
            rowd["tipo"], 
            rowd["compensado"], 
            rowd["reconciliado"],    
            rowd["tipocustodia"],
            rowd["valor"],
            rowd["data"],
            rowd["datacompensacao"],
            rowd["datavencimento"]
        )

        buffer.append(val_str)
        lines += 1
    if buffer:
        destSession.execute(comando_insert.format(",".join(buffer)))

    print(f'- Tabela {tabela} - registros transferidos: {lines:,}')


##################################################
# LIMPEZA DE CARACTERES INVALIDOS
##################################################
def sanitize_str(string: str) -> str:
    return string.replace("'", " ")


##################################################
# MAIN
##################################################
def main(argv):
    db_orig_url = os.environ['DB_RM_URL']
    db_dest_url = os.environ['DB_DW_URL']

    # Banco de Dados Origem - (Sql Server - TOTVSRM)
    source_engine = create_engine(db_orig_url, echo=False)
    SourceSession = sessionmaker(source_engine)

    # Banco de Dados Destino - (PostgreSQL - SBIDW )
    dest_engine = create_engine(db_dest_url, echo=False)
    DestSession = sessionmaker(dest_engine)

    # Cria o Schema STAGING se não existir
    destSession = DestSession()
    destSession.execute('''create schema if not exists staging''')
    destSession.commit()

    # Cria as classes e as tabelas no banco destino
    Coligada.metadata.create_all(dest_engine)
    ClienteFornecedor.metadata.create_all(dest_engine)
    ClienteDadosAdicionais.metadata.create_all(dest_engine)
    ConsistenciaCampos.metadata.create_all(dest_engine)
    Municipio.metadata.create_all(dest_engine)
    Empreendimento.metadata.create_all(dest_engine)
    Quadra.metadata.create_all(dest_engine)
    Lote.metadata.create_all(dest_engine)
    SituacaoVenda.metadata.create_all(dest_engine)
    Venda.metadata.create_all(dest_engine)
    VendaRegraComponente.metadata.create_all(dest_engine)
    ParcelaVenda.metadata.create_all(dest_engine)
    ItemVenda.metadata.create_all(dest_engine)
    CompradorVenda.metadata.create_all(dest_engine)
    CorretorVenda.metadata.create_all(dest_engine)
    Lancamento.metadata.create_all(dest_engine)
    LancamentoIntegracao.metadata.create_all(dest_engine)
    LancamentoBaixa.metadata.create_all(dest_engine)
    ExtratoCaixa.metadata.create_all(dest_engine)
    ModalidadeVenda.metadata.create_all(dest_engine)
    Distrato.metadata.create_all(dest_engine)
    MotivoDistrato.metadata.create_all(dest_engine)
    TabelaPrecoLote.metadata.create_all(dest_engine)


    # Sessão para leitura da base origem
    sourceSession = SourceSession()

    horaIni = datetime.now(timezone('America/Campo_Grande'))

    print("")
    print("--------------------------------------------------------------------")
    print(" Processo de Extração de Dados - Origem TOTVS RM (SQL Server)       ")
    print("--------------------------------------------------------------------")
    print("Início em: " + horaIni.strftime("%d/%m/%Y - %H:%M:%S"))
    print("")

    try:
        migra_coligada(sourceSession, destSession)
        migra_cliente_fornecedor(sourceSession, destSession)
        migra_cliente_dados_adicionais(sourceSession, destSession)
        migra_consistencia_campos(sourceSession, destSession)
        migra_municipio(sourceSession, destSession)
        migra_empreendimento(sourceSession, destSession)
        migra_quadra(sourceSession, destSession)
        migra_lote(sourceSession, destSession)
        migra_tabela_preco_lote(sourceSession, destSession)
        migra_situacao_venda(sourceSession, destSession)
        migra_venda(sourceSession, destSession)
        migra_venda_regra_compn(sourceSession, destSession)
        migra_modalidade_venda(sourceSession, destSession)
        migra_distrato(sourceSession, destSession)
        migra_motivo_distrato(sourceSession, destSession)
        migra_parcela_venda(sourceSession, destSession)
        migra_item_venda(sourceSession, destSession)
        migra_comprador_venda(sourceSession, destSession)
        migra_corretor_venda(sourceSession, destSession)
        migra_lancamento(sourceSession, destSession)
        migra_lancamento_integracao(sourceSession, destSession)
        migra_lancamento_baixa(sourceSession, destSession)
        migra_extrato_caixa(sourceSession, destSession)

        destSession.commit()
    except:
        destSession.rollback()
        raise
    finally:
        destSession.close()

    horaFim = datetime.now(timezone('America/Campo_Grande'))
    duracao = horaFim - horaIni

    print("")
    print("Término em:  " + horaFim.strftime("%d/%m/%Y - %H:%M:%S"))
    print("Tempo Total: " + str(duracao))
    print("")


if __name__ == '__main__':
    main(sys.argv)
