# %% init

import pandas as pd
import neo4j # just for testing
from neo4j import GraphDatabase, basic_auth # for data loader
from py2neo import Graph, Node, Relationship

from urllib import parse


graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

# %% Load data
# SPIIL
SPIIL_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\SPIIL\\SPIIL_list_edit.csv"
columns = pd.read_csv(SPIIL_path, sep =";", header=None, nrows=1)
columns.values.tolist()[0]
SPIIL_df = pd.read_csv(SPIIL_path, sep =";", header=0, index_col=False,usecols = columns)

# CPPAP
CPPAP_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\CPPAP\\liste-des-services-de-presse-en-ligne-reconnus.csv"
CPPAP_df = pd.read_csv(CPPAP_path, sep =";", header=0, index_col=False)
CPPAP_df.columns =['Editeur','FormeJuridique','Service','Url','Qualification', 'ServiceReconnuJusquau','N_CPPAP']

# MediasLocaux
Mediaslocaux_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\MediasLocaux\\MediaLocaux.csv"
ML_df = pd.read_csv(Mediaslocaux_path, sep =",", header=0, index_col=False)

# create site_name
ML_df['site_name']=None
for index, row in ML_df.iterrows():
    try:
        ML_df.loc[index, 'site_name']= parse.urlsplit(row['Siteweb']).netloc.replace("www.","").lower()
    except:
        print(row['Siteweb'])

# group by site_name and get list of other columns
MLgrouped_df = ML_df.groupby('site_name')['COMMUNE'].apply(list).reset_index(name='communelist')
MLgrouped_df['medialist']=ML_df.groupby('site_name')['Média'].apply(list).reset_index(name='medialist')['medialist']
MLgrouped_df['typemedialist']=ML_df.groupby('site_name')['Typedemédia'].apply(list).reset_index(name='typemedialist')['typemedialist']
MLgrouped_df['siteweblist']==ML_df.groupby('site_name')['Siteweb'].apply(list).reset_index(name='siteweblist')['siteweblist']

# remove doublons in lists and create nice df
MLgrouped_df['communes']=None
MLgrouped_df['medias']=None
MLgrouped_df['typesmedia']=None
for index, row in MLgrouped_df.iterrows():
    MLgrouped_df.loc[index, 'communes'] = list(set(row['communelist']))
    MLgrouped_df.loc[index, 'medias'] = list(set(row['medialist']))
    MLgrouped_df.loc[index, 'typesmedia'] = list(set(row['typemedialist']))
MLnice_df = MLgrouped_df[['site_name','communes','medias','typesmedia']]

# Decodex
Decodex_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\Decodex\\DonnéesDécodeurs_Médiamétrie_Juillet2020-Catégories.csv"
Deco_df = pd.read_csv(Decodex_path, sep=",", index_col=False)
Deco_df.columns=['actu', 'douteux', 'peufiable']

# DigitalAdTrust
DAT_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\DigitalAdTrust\\DAT_edit.csv"
DAT_df = pd.read_csv(DAT_path, sep=";", index_col=False)

# %% import DECODEX in DB
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
# %% import CPPAP in base
i=j=k=0
for index, row in CPPAP_df.iterrows():
    try:
        name = row['Service'].replace("https://","").replace("http://","").replace("www.","").replace("/","")
        node = graph.nodes.match("Website",name=name).first()
        if node == None:
            i=i+1
        else:
            for col in CPPAP_df.columns:
                node['CPPAP_'+col] = row[col]
            graph.push(node)
            j=j+1
    except:
        k=k+1
        #print("error with:", row['Service'])
        pass
print("CPPAP:",j,"nodes found and updated, ",i,"not in base and",k,"errors")

# %% import SPIIL in base
i=j=k=0
for index, row in SPIIL_df.iterrows():
    try:
        name = row['SiteWeb'].replace("https://","").replace("http://","").replace("www.","").split("/")[0]
        node = graph.nodes.match("Website",name=name).first()
        if node == None:
            i=i+1
            #print(name)
        else:
            for col in SPIIL_df.columns:
                node['SPIIL_'+col] = row[col]
            graph.push(node)
            j=j+1
    except:
        k=k+1
        #print("error with:", row['SiteWeb'])

print("SPIIL:",j,"nodes found and updated, ",i,"not in base and",k,"errors")

# %% import Mediaslocaux in base
i=j=k=0
for index, row in MLnice_df.iterrows():
    try:
        name = row['site_name']
        node = graph.nodes.match("Website",name=name).first()
        if node == None:
            i=i+1
            #print(name)
        else:
            for col in MLnice_df.columns:
                node['ML_'+col] = row[col]
            graph.push(node)
            j=j+1
    except:
        k=k+1
        #print("error with:", row['SiteWeb'])

print("MediasLocaux:",j,"nodes found and updated, ",i,"not in base and",k,"errors")

# %% Import DigitalAdTrust in base
i=j=k=0
for index, row in DAT_df.iterrows():
    try:
        name = row['DomainesLabellisés'].lower().replace(" ","").split("/")[0].replace("  ","")
        node = graph.nodes.match("Website",name=name).first()
        if node == None:
            i=i+1
            #print(name)
        else:
            for col in DAT_df.columns:
                node['DAT_'+col] = row[col]
            graph.push(node)
            j=j+1
    except:
        k=k+1
        #print("error with:", row)

print("Digital ad Trust:",j,"nodes found and updated, ",i,"not in base and",k,"errors")
