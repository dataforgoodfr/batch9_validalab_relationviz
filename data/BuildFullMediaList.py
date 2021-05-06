# %% init

import pandas as pd
import neo4j # just for testing
from neo4j import GraphDatabase, basic_auth # for data loader
from py2neo import Graph, Node, Relationship

from urllib import parse


graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

# %% md
what I want:
- list of all websites already in list D0, D1, D2...
- Be sure that list includes all files that link to RS...
- Add all new media list :SPIIL, CPPAP

For D0, can try to include twitter, facebook or




# %% Load data
SPIIL_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\SPIIL\\SPIIL_list_edit.csv"
columns = pd.read_csv(SPIIL_path, sep =";", header=None, nrows=1)
columns
columns.values.tolist()[0]
SPIIL_df = pd.read_csv(SPIIL_path, sep =";", header=0, index_col=False,usecols = columns)
SPIIL_df

CPPAP_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\CPPAP\\liste-des-services-de-presse-en-ligne-reconnus.csv"
CPPAP_df = pd.read_csv(CPPAP_path, sep =";", header=0, index_col=False)
CPPAP_df

Mediaslocaux_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\MediasLocaux\\MediaLocaux.csv"
ML_df = pd.read_csv(Mediaslocaux_path, sep =",", header=0, index_col=False)
ML_df
len(ML_df[['Média','Typedemédia','Siteweb']])
len(ML_df[['Média','Typedemédia','Siteweb']].drop_duplicates())
len(ML_df[['Média','Typedemédia','Siteweb']].drop_duplicates(subset=['Siteweb']))
ML_df[['Média','Typedemédia','Siteweb']].drop_duplicates()

OriginalList_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\202007WebsitesRS_D0\\20200709_medialistForD0.txt"
Orig_df = pd.read_csv(OriginalList_path, header = None, index_col=False)
Orig_df

Decodex_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\Decodex\\DonnéesDécodeurs_Médiamétrie_Juillet2020-Catégories.csv"
Deco_df = pd.read_csv(Decodex_path, sep=",", index_col=False)
Deco_df.columns=['actu', 'douteux', 'peufiable']
Deco_df

# %% Create one big list
len(SPIIL_df['SiteWeb']) + len(CPPAP_df['Url']) + len(ML_df['Siteweb']) + len(Orig_df)
biglist_df = SPIIL_df['SiteWeb'].append(CPPAP_df['Url'], ignore_index = True)
biglist_df = biglist_df.append(ML_df['Siteweb'], ignore_index = True)
biglist_df = biglist_df.append(Orig_df, ignore_index = True)
len(biglist_df)

biglist_df['orig_url']=biglist_df[0]

len(biglist_df.dropna(subset=['orig_url']))
biglist_df['site_name'] = None

for index, row in biglist_df.dropna(subset=['orig_url']).iterrows():
    try:
        biglist_df.loc[index, 'site_name']= parse.urlsplit(row['orig_url']).netloc.replace("www.","").lower()
    except:
        print(row['orig_url'])


len(biglist_df.dropna(subset=['site_name']))
len(biglist_df.drop_duplicates(subset=['site_name']))

biglist_df.drop_duplicates(subset=['site_name']).to_csv('C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\202008Websites\\BigList.csv')

# %% big list without local media
len(SPIIL_df['SiteWeb']) + len(CPPAP_df['Url']) + len(Orig_df)
biglist2_df = SPIIL_df['SiteWeb'].append(CPPAP_df['Url'], ignore_index = True)
biglist2_df = biglist2_df.append(Orig_df, ignore_index = True)
len(biglist2_df)

biglist2_df['orig_url']=biglist2_df[0]

len(biglist2_df.dropna(subset=['orig_url']))
biglist2_df['site_name'] = None

for index, row in biglist2_df.dropna(subset=['orig_url']).iterrows():
    try:
        biglist2_df.loc[index, 'site_name']= parse.urlsplit(row['orig_url']).netloc.replace("www.","").lower()
    except:
        print(row['orig_url'])


len(biglist2_df.dropna(subset=['site_name']))
len(biglist2_df.drop_duplicates(subset=['site_name']))

biglist2_df.drop_duplicates(subset=['site_name']).to_csv('C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\202008Websites\\BigList2.csv')


# %% Check what data is not in base
i=0
for index, row in SPIIL_df.iterrows():
    try:
        name = row['SiteWeb'].replace("https://","").replace("http://","").replace("www.","").replace("/","")
        node = graph.nodes.match(name=name).first()
        if node == None:
            i=i+1
            print(name)
    except:
        print("error with:", row['SiteWeb'])


print(i)

# This is row with NaN values
SPIIL_df[SPIIL_df.isnull().any(axis=1)]


# %% CPPAP
i=0
for index, row in CPPAP_df.iterrows():
    try:
        name = row['Service'].replace("https://","").replace("http://","").replace("www.","").replace("/","")
        node = graph.nodes.match(name=name).first()
        if node == None:
            i=i+1
            print(name)
    except:
        print("error with:", row['Service'])

# %% Decodex
Deco_df.columns

i=j=0
for index, row in Deco_df.iterrows():
    name = row['peufiable']
    node = graph.nodes.match("Website", site_name=name).first()
    if node ==None:
        i=i+1
        print(name)
    else:
        node['decodex']=2
        graph.push(node)
        print(node['site_name'])

i=j=0
for index, row in Deco_df[0:120].iterrows():
    name = row['douteux']
    node = graph.nodes.match("Website", site_name=name).first()
    if node ==None:
        i=i+1
        print(name)
    else:
        node['decodex']=1
        graph.push(node)
        print(node['site_name'])
        j=j+1

Deco_df['douteux'][0:120]
DecolistActu =["lefigaro.fr","francetvinfo.fr","bfmtv.com","ouest-france.fr","leparisien.fr","20minutes.fr","lemonde.fr","linternaute.com","actu.fr","lci.fr",
"francebleu.fr","huffingtonpost.fr","rtl.fr","lexpress.fr","nouvelobs.com","lepoint.fr","ladepeche.fr","lavoixdunord.fr","planet.fr","cnews.fr" ]

i=j=0
for name in DecolistActu:
    node = graph.nodes.match("Website", site_name=name).first()
    if node ==None:
        i=i+1
        #print(name)
    else:
        node['decodex']=0
        graph.push(node)
        print(node['site_name'])
        j=j+1
