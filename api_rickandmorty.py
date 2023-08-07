
import pandas as pd
import requests
import datetime

link = ["https://rickandmortyapi.com/api/character","https://rickandmortyapi.com/api/character?page=2","https://rickandmortyapi.com/api/character?page=3"]

#extraemos la informacion
def extr(link):
    df_core = []
    #leemos las paginas del api
    for url in link:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data['results'])
            df_core.append(df)
        else:
            print('Código de estado:',response.status_code)
    final = pd.concat(df_core)
    return final

#limpieza de datos
def limp(df):
    
    #rellenar informacion vacia
    df = df.replace(r'^\s*$', 'N/A', regex=True)
    
    #acondicionamos datos
    df['episode'] = df['episode'].apply(lambda row: len(row))
    df['created'] = df['created'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d/%m/%Y"))
    df['origin'] = df['origin'].apply(lambda x: x['name'])
    df['location'] = df['location'].apply(lambda x: x['name'])
    #quitamos columnas 
    df.drop('url', axis=1,inplace=True)
    
    return df

extr_resul = extr(link)
#generamos tabla
limp_res = limp(extr_resul)
nom_archivo = 'ETL_rickandmorty.xlsx'
limp_res.to_excel(nom_archivo, index=False)
    


