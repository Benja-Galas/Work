from sqlalchemy import create_engine
from sqlalchemy import or_
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from collections import Counter
import numpy as np
import pandas as pd

pd.set_option('display.width', 1500)
pd.set_option('max_columns', 20)
pd.set_option('max_colwidth', 100)

#Conection

DB_LH = {
         'user':'root',
         'pasword':'',
         'host':'localhost',
         'port':'3306',
         'database':'inventario_test'}

def to_sqlalch_cred(credential, db_type='mysql+mysqlconnector'):
    """

    Args:
        credential (dic): Dictonary with credencial
    """
    cred_str = ''
    cred_str+=db_type+'://'
    cred_str+=credential['user']+':'+credential['pasword']+'@'
    cred_str+=credential['host']+':'+credential['port']+'/'
    cred_str+=credential['database']
    return cred_str

cred_str = to_sqlalch_cred(credential=DB_LH)
engine = create_engine(cred_str,echo=False)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

class Invt(Base):
    __tablename__ = 'inventory'

    id = Column(Integer, primary_key=True, autoincrement=True)
    Sitio = Column(String(200), nullable=False)
    xBBP =Column(String(200), nullable=False)
    xMPT = Column(String(200),nullable=False)
    RRU = Column(String(200), nullable=False)


    def __repr__(self):
        return "<Invt(Sitio=%s', Gabinete=%s', Nodo=%s', RRU=%s', xBBP=%s', xMPT=%s')>"%(self.Sitio,
                                                                                 self.Gabinete,
                                                                                 self.Nodo,
                                                                                 self.RRU,
                                                                                 self.xBBP,
                                                                                 self.xMPT
                                                                                 )


class Cabinet(Base):
    __tablename__ = 'gabinetes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    Sitio = Column(String(200), nullable=False)
    Gabinete = Column(String(200), nullable=False)

    def __repr__(self):
        return "<Cabinet(Sitio=%s', Gabinete=%s')>"%(self.Sitio,
                                                              self.Gabinete)


class Subtrack(Base):
    __tablename__ = 'nodos'
    id = Column(Integer, primary_key=True, autoincrement=True)
    Sitio = Column(String(200), nullable=False)
    Nodo = Column(String(200), nullable=False)

    def __repr__(self):
        return "<Subtrack(Sitio=%s', Nodo=%s')>"%(self.Sitio,
                                                       self.Nodo)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


# manejo de datos

data_board = pd.read_csv('Inventory_Board_20211108_105248.csv')
data_board = data_board.rename(columns={'NEName':'Sitio', 'Board Type':'xBBP',
                                        'Manufacturer Data':'RRU_datos'})
column_board = data_board[['Sitio','xBBP']]
column_rep = data_board[['xBBP']]
column_rep = column_rep.rename(columns={'xBBP':'xMPT'})
column_RRU = data_board[['RRU_datos']]
column_RRU = column_RRU['RRU_datos'].str.split(',', expand=True)
column_RRU = pd.DataFrame({'RRU': column_RRU[0]})



data_board = pd.concat([column_board, column_rep, column_RRU], axis=1)

data_cabinet = pd.read_csv('Inventory_Cabinet_20211108_105248.csv')
data_cabinet = data_cabinet.rename(columns={'NEName':'Sitio', 'Manufacturer Data':'Gabinete'})
data_cabinet = data_cabinet[['Sitio','Gabinete']]



data_subtrack = pd.read_csv('Inventory_Subrack_20211108_105248.csv')
data_subtrack = data_subtrack.rename(columns={'NEName':'Sitio','Frame Type':'Nodo'})
data_subtrack = data_subtrack[['Sitio','Nodo']]



session.bulk_insert_mappings(Invt, data_board.to_dict('records'))
session.bulk_insert_mappings(Cabinet, data_cabinet.to_dict('records'))
session.bulk_insert_mappings(Subtrack, data_subtrack.to_dict('records'))
session.commit()

def aggregate_data(dataframe: pd.DataFrame(),
                   column_to_group: str,
                   column_to_agregate:str
                   ):
    _agg_function = {column_to_agregate:','.join}
    _data_agg = dataframe.groupby(column_to_group).agg(_agg_function)
    _data_agg.reset_index(inplace=True)
    return _data_agg


query_cabinet = session.query(Cabinet.Sitio, Cabinet.Gabinete).filter(Cabinet.Gabinete.like('%Out%'))
data_cabinet = pd.read_sql(sql=query_cabinet.statement, con=session.bind)

column_cabinet = data_cabinet[['Gabinete']]
column_cabinet = column_cabinet['Gabinete'].str.split(',', expand=True)
column_cabinet = pd.DataFrame({'Gabinete': column_cabinet[0]})
data_cabinet['Gabinete'] = column_cabinet
data_cabinet = aggregate_data(dataframe=data_cabinet,column_to_group='Sitio',column_to_agregate='Gabinete')

query_subtrack = session.query(Subtrack.Sitio, Subtrack.Nodo).filter(Subtrack.Nodo.like('%BBU%'))
data_subtrack = pd.read_sql(sql=query_subtrack.statement, con=session.bind)
data_subtrack = aggregate_data(dataframe=data_subtrack,
                          column_to_group='Sitio',
                          column_to_agregate='Nodo'
                          )

# print(data_subtrack)


query_BBP = session.query(Invt.id, Invt.Sitio, Invt.xBBP).filter(Invt.xBBP.like('%BBP%'))
data_BBP = pd.read_sql(sql=query_BBP.statement, con=session.bind)
data_BBP['xBBP'] = 'x' + data_BBP.xBBP.str[4:]
data_BBP = aggregate_data(dataframe=data_BBP,
                          column_to_group='Sitio',
                          column_to_agregate='xBBP'
                          )
data_BBP['xBBP'] = data_BBP.apply( lambda x: dict(Counter(x.xBBP.split(','))), axis=1)
data_BBP['xBBP'] = data_BBP.xBBP.apply(str)
data_BBP['xBBP'] = data_BBP.xBBP.str.strip('{}')
data_BBP['xBBP'] = data_BBP['xBBP'].str.replace(r"[\"\']", '')
# print(data_BBP)

query_RRU = session.query(Invt.id, Invt.Sitio, Invt.RRU).filter(Invt.RRU.like('%RRU%'))
data_RRU = pd.read_sql(sql=query_RRU.statement, con=session.bind)
data_RRU = aggregate_data(dataframe=data_RRU,
                          column_to_group='Sitio',
                          column_to_agregate='RRU')
data_RRU['RRU'] = data_RRU.apply( lambda x: dict(Counter(x.RRU.split(','))), axis=1)
data_RRU['RRU'] = data_RRU.RRU.apply(str)
data_RRU['RRU'] = data_RRU.RRU.str.strip('{}')
data_RRU['RRU'] = data_RRU['RRU'].str.replace(r"[\"\']", '')

query_MPT = session.query(Invt.id, Invt.Sitio, Invt.xMPT).filter(Invt.xMPT.like('%MPT%'))
data_MPT = pd.read_sql(sql=query_MPT.statement, con=session.bind)
data_MPT['xMPT'] = 'x' + data_MPT.xMPT.str[4:]
data_MPT = aggregate_data(dataframe= data_MPT,
                          column_to_group='Sitio',
                          column_to_agregate='xMPT')
data_MPT['xMPT'] = data_MPT.apply( lambda x: dict(Counter(x.xMPT.split(','))), axis=1)
data_MPT['xMPT'] = data_MPT.xMPT.apply(str)
data_MPT['xMPT'] = data_MPT.xMPT.str.strip('{}')
data_MPT['xMPT'] = data_MPT['xMPT'].str.replace(r"[\"\']", '')
# print(data_MPT)

def merge_multiple_data(dataframe_A: pd.DataFrame(),
              dataframe_B:pd.DataFrame(),
              dataframe_C:pd.DataFrame(),
              column_merge: str):
    """Merge 3 coluns by a determinated column

    Args:
        dataframe_A (pd.DataFrame()): Description
        dataframe_B (pd.DataFrame()): Description
        dataframe_C (pd.DataFrame()): Description
        column_merge (str): Description

    Returns:
        TYPE: dataframe merged
    """
    _data_merge = dataframe_A.merge(dataframe_B, how='inner',on=column_merge)
    _data_merge = _data_merge.merge(dataframe_C, how='inner', on=column_merge)
    return _data_merge

cabinet_subtrack = data_cabinet.merge(data_subtrack, how='right', on='Sitio')

board = merge_multiple_data(dataframe_A= data_BBP,
                 dataframe_B= data_MPT,
                 dataframe_C= data_RRU,
                 column_merge= 'Sitio')


data = cabinet_subtrack.merge(board, how='left', on='Sitio')
# print(data)
data.to_excel('invt_final.xlsx')


