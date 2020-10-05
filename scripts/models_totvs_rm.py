from sqlalchemy import Boolean, Column, Integer, String, Date, DateTime, ForeignKey, Numeric, Table
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone

Base = declarative_base(metadata=MetaData(schema='staging'))

# ----------------------------------
# TOTVS RM - Classes para extração
# ----------------------------------

class Coligada(Base):
    __tablename__ = 'gcoligada'
    codcoligada = Column(Integer, primary_key=True)
    nome = Column(String(255))
    nomefantasia = Column(String(255))
    cidade = Column(String(32))
    estado = Column(String(2))
    cep = Column(String(9))

class ClienteFornecedor(Base):
    __tablename__ = 'fcfo'
    codcoligada = Column(Integer, primary_key=True)
    codcfo = Column(String(25), primary_key=True)
    nome = Column(String(100))
    nomefantasia = Column(String(100))
    bairro = Column(String(80))
    cidade = Column(String(32))
    codetd = Column(String(2))
    cep = Column(String(9))
    codmunicipio = Column(String(20))
    dtnascimento = Column(Date)
    grpven = Column(String(10))
    pessoafisoujur = Column(String(1))

class ClienteDadosAdicionais(Base):
    __tablename__ = 'xclientepessoafisica'
    codcoligada = Column(Integer, primary_key=True)
    codcfo = Column(String(25), primary_key=True)
    nom_prof = Column(String(30))
    dsc_est_civ = Column(String(30))
    cod_sexo = Column(String(1))
    nat_cfo = Column(String(30))
    pg_aluguel = Column(String(1))
    casa_propria = Column(String(1))
    empr_renda = Column(Numeric(15,4))

class ConsistenciaCampos(Base):
    __tablename__ = 'gconsist'
    codcoligada = Column(Integer, primary_key=True)
    aplicacao = Column(String(1), primary_key=True)
    codtabela = Column(String(10), primary_key=True)
    codcliente = Column(String(10), primary_key=True)
    codinterno = Column(String(10))
    descricao = Column(String(70))    

class Municipio(Base):
    __tablename__ = 'gmunicipio'
    codmunicipio = Column(String(20), primary_key=True)
    codetdmunicipio = Column(String(2), primary_key=True)
    nomemunicipio = Column(String(32))
    codibge = Column(String(20))

class Empreendimento(Base):
    __tablename__ = 'xempreendimento'
    cod_pess_empr = Column(Integer, primary_key=True)
    codcoligada = Column(Integer)
    nome = Column(String(45))
    nomefantasia = Column(String(45))
    cidade = Column(String(32))
    uf = Column(String(2))
    cep = Column(String(9))
    codmunicipio = Column(String(20))

class Quadra(Base):
    __tablename__ = 'xunidade'
    cod_pess_empr = Column(Integer, primary_key=True)
    num_unid = Column(String(6), primary_key=True)
    dsc_local = Column(String(100))
    qtd_sub_unid = Column(Integer)

class Lote(Base):
    __tablename__ = 'xsubunidade'
    cod_pess_empr = Column(Integer, primary_key=True)
    num_unid = Column(String(6), primary_key=True)
    num_sub_unid = Column(String(6), primary_key=True)
    descricao = Column(String(255))
    area = Column(Numeric(15,4))
    cod_sit_sub_unid = Column(Integer)
    dsc_sit_sub_unid = Column(String(30))
    cod_prefeitura = Column(String(30))
    codcoltabprecosubunid = Column(Integer)
    numtabprecosubunid = Column(Integer)

class SituacaoVenda(Base):
    __tablename__ = 'xsituacaovenda'
    cod_sit_venda = Column(Integer, primary_key=True)
    dsc_sit_venda = Column(String(30))

class Venda(Base):
    __tablename__ = 'xvenda'
    num_venda = Column(Integer, primary_key=True)
    codcoligada = Column(Integer)
    num_cont = Column(Integer)
    cod_pess_empr = Column(Integer)
    dat_venda = Column(Date)
    dat_cancela = Column(Date)
    dat_quit = Column(Date)
    codcfo = Column(String(25))
    cod_sit_venda = Column(Integer)
    tipven = Column(String(10))
    cod_mod_venda = Column(Integer)

class VendaRegraComponente(Base):
    __tablename__ = 'xregracomponentevenda'
    num_venda = Column(Integer, primary_key=True)
    cod_compn = Column(Integer, primary_key=True)
    cod_grupo = Column(Integer, primary_key=True)
    vr_tx_per = Column(Numeric(15,4))
    data_base = Column(Date)
    dat_reaj_ind = Column(Date)
    cod_grupo_ref = Column(Integer)

class ParcelaVenda(Base):
    __tablename__ = 'xvendaparcela'
    codcoligada = Column(Integer, primary_key=True)
    numvenda = Column(Integer, primary_key=True)
    codgrupo = Column(Integer, primary_key=True)
    numparc = Column(Integer, primary_key=True)
    codtipoparc = Column(Integer, primary_key=True)
    codcollan = Column(Integer)
    idlan = Column(Integer)
    numnotapromissoria = Column(String(25))

class ItemVenda(Base):
    __tablename__ = 'xitemvenda'
    num_venda = Column(Integer, primary_key=True)
    cod_pess_empr = Column(Integer, primary_key=True)
    num_unid = Column(String(6), primary_key=True)
    num_sub_unid = Column(String(6), primary_key=True)
    vr_item = Column(Numeric(15,4))
    vr_desc = Column(Numeric(15,4))
    vr_acrescimo = Column(Numeric(15,4))

class CompradorVenda(Base):
    __tablename__ = 'xcomprador'
    codcoligada = Column(Integer, primary_key=True)
    num_venda = Column(Integer, primary_key=True)
    idcomprador = Column(Integer, primary_key=True)
    codcolcfo = Column(Integer)
    codcfo = Column(String(25))
    principal = Column(Integer)
    participacao = Column(Numeric(15,4))
    dataentradaplano = Column(Date)
    datasaidaplano = Column(Date)

class CorretorVenda(Base):
    __tablename__ = 'xvendedor'
    codcoligada = Column(Integer, primary_key=True)
    num_venda = Column(Integer, primary_key=True)
    idvendedor = Column(Integer, primary_key=True)
    codcolcfo = Column(Integer)
    codcfo = Column(String(25))

class ModalidadeVenda(Base):
    __tablename__ = 'xmodalidadevenda'
    cod_mod_venda = Column(Integer, primary_key=True)
    dsc_mod_venda = Column(String(45))

class Distrato(Base):
    __tablename__ = 'xdistrato'
    numvenda = Column(Integer, primary_key=True)
    databasedistrato = Column(Date)
    codtipodistrato = Column(Integer)
    codmotivodistrato = Column(Integer)

class MotivoDistrato(Base):
    __tablename__ = 'xmotivodistrato'
    codmotivodistrato = Column(Integer, primary_key=True)
    dscmotivodistrato = Column(String(50))

class TabelaPrecoLote(Base):
    __tablename__ = 'xtabprecosubunidade'
    codcoltabpreco = Column(Integer, primary_key=True)
    numtabpreco = Column(Integer, primary_key=True)
    codpessempr = Column(Integer, primary_key=True)
    numunid = Column(String(6), primary_key=True)
    numsubunid = Column(String(6), primary_key=True)
    precosubunid = Column(Numeric(15,4))   

class Lancamento(Base):
    __tablename__ = 'flan'
    codcoligada = Column(Integer, primary_key=True)
    idlan = Column(Integer, primary_key=True)
    pagrec = Column(Integer)
    statuslan = Column(Integer)
    codaplicacao = Column(String(2))
    codtdo = Column(String(10))
    datavencimento = Column(Date)
    valororiginal = Column(Numeric(15,4))
    valorjuros = Column(Numeric(15,4))
    valordesconto = Column(Numeric(15,4))
    valoracrescimoacordo = Column(Numeric(15,4))
    valorjurosacordo = Column(Numeric(15,4))
    valorop1 = Column(Numeric(15,4))
    valorop3 = Column(Numeric(15,4))
    baixapendente = Column(Integer)

class LancamentoIntegracao(Base):
    __tablename__ = 'flanintegracao'
    codcoligada = Column(Integer, primary_key=True)
    idcampo = Column(Integer, primary_key=True)
    idlan = Column(Integer, primary_key=True)
    valor = Column(Numeric(15,4))
    codcompn = Column(Integer, primary_key=True)

class LancamentoBaixa(Base):
    __tablename__ = 'flanbaixa'
    codcoligada = Column(Integer, primary_key=True)
    idlan = Column(Integer, primary_key=True)
    idbaixa = Column(Integer, primary_key=True)
    databaixa = Column(Date)
    codcolxcx = Column(Integer)
    idxcx = Column(Integer)
    status = Column(Integer)
    valorbaixa = Column(Numeric(15,4))
    valormulta = Column(Numeric(15,4))
    valorjuros = Column(Numeric(15,4))
    valordesconto = Column(Numeric(15,4))
    valorvinculado = Column(Numeric(15,4))
    valornotacreditoadiantamento = Column(Numeric(15,4))    
    valordevolucao = Column(Numeric(15,4))
    valornotacredito = Column(Numeric(15,4))
    valorperdafinanceira = Column(Numeric(15,4))

class ExtratoCaixa(Base):
    __tablename__ = 'fxcx'
    codcoligada = Column(Integer, primary_key=True)
    idxcx = Column(Integer, primary_key=True)
    codcolcxa = Column(Integer)
    codcxa = Column(String(10))
    tipo = Column(Integer)
    compensado = Column(Integer)
    reconciliado = Column(Integer)
    tipocustodia = Column(Integer)
    valor = Column(Numeric(15,4))
    data = Column(Date)
    datacompensacao = Column(Date)
    datavencimento = Column(Date)
