import pandas as pd
from sqlalchemy import create_engine
from function.connections import to_sqlalch_cred
from conection_sheets.conn_google_sheets import upload_data_to_sheets
import sqlalchemy



def getData_antena():
    DB_LH2 = {
         'user':'vmranread',
         'pasword':'XwoteMck7',
         'host':'10.120.145.103',
         'port':'3306',
         'database':'db'}


    cred_str = to_sqlalch_cred(credential=DB_LH2)
    engine = create_engine(cred_str,echo=False)
    query = ("""SELECT lcellreference.DateId,

    lcellreference.SITE,

    lcellreference.CELLNAME,

    lcellreference.SECTOR,

    lcellreference.SECTOR_FISICO,

    lcellreference.LAT,

    lcellreference.LON,

    lcellreference.COMUNA,

    lcellreference.REGION,

    lcellreference.FREQBAND,

    lcellreference.RRU_MODEL,

    lcellreference.TXRXMODE,

    lcellreference.ANTENNA_MODEL,

    lcellreference.CELLADMINSTATE,

    lcellreference.CELLACTIVESTATE FROM Lcellreference""")#ORDER BY REGION ASC en caso de querer ser ordenadas
    df_antena = pd.read_sql(query, engine)
    return df_antena

df_antena = getData_antena()
print(df_antena)
#upload_data_to_sheets(data=df_antena, name_sheets='Antena')
#df_antena.to_excel('data_antena_test.xlsx')
#print(df_antena)




