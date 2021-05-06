# %% Import new full new DB


import networkx as nx
from py2neo import Graph, Node, Relationship
import pandas as pd
import random
from neo4j import GraphDatabase, basic_auth
import matplotlib


graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()



import sys
dataPath = 'C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data'
if dataPath not in sys.path:
    sys.path.insert(0, dataPath)

from cleanImportProcedures import importGexfNodesAndRel
from cleanImportProcedures import importMondeDiploFiles
from cleanImportProcedures import importEntityWebsiteRelForDiplo, importACPM_SiteGP, importACPM_SitePro

# Import Hyphe D0 DISCO
gexfD0DISCO="C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_DISCO.gexf"
importGexfNodesAndRel(gexfD0DISCO, 0)

# Import Hyphe D1 DISCO
gexfD1DISCO="C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D1_DISCO.gexf"
importGexfNodesAndRel(gexfD1DISCO, 1)

# Import Mondediplo
medias_path = 'C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/mondediplo/medias_francais.tsv'
relations_medias_path = 'C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/mondediplo/relations_medias_francais.tsv'
importMondeDiploFiles(medias_path, relations_medias_path)

medias_francais_relDB_path = 'C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/mondediplo/medias_francais_relDB.tsv'
importEntityWebsiteRelForDiplo(medias_francais_relDB_path)

#ACPM:
ACPM_siteGrandsPublics_path = 'C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/ACPM/ACPM_list_classement-unifie_20200708_GrandPub.csv'
importACPM_SiteGP(ACPM_siteGrandsPublics_path)
ACPM_sitePro_path = 'C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/ACPM/ACPM_list_classement-unifie_20200708_site_pro.csv'
importACPM_SitePro(ACPM_sitePro_path)


# %% Import D2 test2
gexfD2DISCO="C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005_Websites01_D2_DISCO.gexf"
importGexfNodesAndRel(gexfD2DISCO, 2)

# Stop before importing relationships for DISCO2


# %% sandbox
#This works but not in automated procedure...


gexffilepath = gexfD1DISCO
depth = 1
G= nx.read_gexf(gexffilepath, node_type=None, relabel=False, version='1.1draft')
data = nx.json_graph.node_link_data(G)
totnblinks=len(data['links'])
print(totnblinks," links found in gexf")
j=0
for link in data['links']:
    results = db.run(
        "MATCH (n1) WHERE $source_id IN n1.D"+str(depth)+"_id "
        "MATCH (n2) WHERE $target_id IN n2.D"+str(depth)+"_id "
        "CREATE (n1)-[:LINKS_TO_D"+str(depth)+" {count: $count}]->(n2) "
        ,{"source_id":link['source'], "target_id":link['target'], "count": link['count']}
    )
    if j%100 == 0:
        print(".", end=" ")
    if j%1000 ==0:
        print(j ,"/",totnblinks)
    j=j+1
print(j," links imported")
print(len(graph.relationships.match()), "links in db after import")







def importMondeDiploFiles(path_to_medias_francais, path_to_relations_medias_francais):
    ''' import or mondediplo files in Neo4j database as "Entity" nodes and "ONED_BY"
    relationships. Create all properties as Diplo_propname.
    No relationship cretaed to other nodes
    '''
    medias_df = pd.read_csv(path_to_medias_francais, sep='	')
    # nodes
    i=0
    totnbnodes = len(medias_df)-1
    print(totnbnodes, "nodes found in file")
    for index, row in medias_df.iterrows():
        i=1+1
        nodematch = graph.nodes.match(entity_name = row["nom"]).first()
        if nodematch == None:
            try:
                nodematch = Node('Entity', entity_name = row["nom"])
                nodematch.__primarylabel__ = 'Entity'
                nodematch.__primarykey__ = 'entity_name'
                graph.merge(nodematch)
            except:
                print("could not import ", row["nom"])
                nodematch = None
        if nodematch != None:
            for key in row.keys():
                nodematch["Diplo_" + key] = row[key]
            graph.push(nodematch)
        if i%100 == 0:
            print(".", end=" ")
        if i%1000 ==0:
            print(i,"/",totnbnodes)
    print(len(graph.nodes.match("Entity")), "Entities in db after import")

    # links
    relations_df = pd.read_csv(path_to_relations_medias_francais, sep='	')
    totnblinks = len(relations_df)-1
    print(totnblinks, "links found in file")
    j = 0
    for index, row in relations_df.iterrows():
        j=j+1
        results = db.run(
            "MATCH (n1:Entity) WHERE n1.Diplo_nom = $origine "
            "MATCH (n2:Entity) WHERE n2.Diplo_nom = $cible "
            "MERGE (n2)-[:OWNED_BY {valeur:$v, source:$s, datePubli:$dp, dateConsult:$dc}] ->(n1) "
            ,{"origine":row['origine'], "cible":row['cible'],"v":row['valeur'], "s":row['source'], "dp":row['datePublication'], "dc":row['dateConsultation'] }
        )
        if j%100 == 0:
            print(".", end=" ")
        if j%1000 ==0:
            print(j ,"/",totnblinks)
    relfin = len(graph.relationships.match((),"OWNED_BY"))
    print(relfin, " OWNED_BY relationships in db after import (or not : there's a bug)")

def importEntityWebsiteRelForDiplo(medias_francais_relDB_path):
    '''create a ONED_BY relationship for each couple Diplo-nom - website-sitename
    in the file. Look only at rows with valid sitename
    '''
    rel_diplo_db_df  = pd.read_csv(medias_francais_relDB_path, sep=',')
    for index, row in rel_diplo_db_df[ rel_diplo_db_df['db_sitename'].notnull()].iterrows():
        results = db.run(
            "MATCH (e:Entity) WHERE e.Diplo_nom = $nom "
            "MATCH (w:Website) WHERE w.site_name = $site "
            "MERGE (w)-[:OWNED_BY ] ->(e) "
            ,{"nom":row['nom'], "site":row['db_sitename']}
        )
        print(row['nom'], row['db_sitename'])


def importACPM_SiteGP(ACPM_siteGrandsPublics_path):
    '''Import file from export https://www.acpm.fr/site/Les-chiffres/Frequentation-internet/Sites-Grand-Public/Classement-unifie
    as ACPM_SiteGP_Prop in Websites nodes
    '''
    df = pd.read_csv(ACPM_siteGrandsPublics_path, sep=';', encoding='cp1252')
    df['db_site_name']=""

    ACPM_key = {"L'Aisnenouvelle.fr" : "aisnenouvelle.fr", "L'Equipe.fr":"lequipe.fr",
        "L'Obs.com":"nouvelobs.com", "Lathierache.fr":"la-thierache.fr", "LHumanité.fr":"humanite.fr",
        "Diabétologie-pratique.com":"diabetologie-pratique.com", "Gynécologie-pratique.com":"gynecologie-pratique.com" }

    for index, row in df.iterrows():
        if row['Sites'] in ACPM_key.keys():
            match = graph.nodes.match(site_name = ACPM_key[row['Sites']].lower()).first()
            df.loc[df['Sites'] == row['Sites'], ['db_site_name']] = ACPM_key[row['Sites']]
        else:
            match = graph.nodes.match(site_name = row['Sites'].lower()).first()
        if match != None:
            df.loc[df['Sites'] == row['Sites'], ['db_site_name']] = match['site_name']

            for key in row.keys():
                if match["ACPM_SiteGP_" + key] == None:
                    match["ACPM_SiteGP_" + key] = row[key]
            graph.push(match)
    print("no node match for these Site names, did not import:")
    print(df[df.db_site_name == ""]['Sites'])

def importACPM_SitePro(ACPM_sitePro_path):
    '''Import file from export https://www.acpm.fr/site/Les-chiffres/Frequentation-internet/Sites-Grand-Public/Classement-unifie
    as ACPM_SitePro__Prop in Websites nodes
    '''
    df = pd.read_csv(ACPM_sitePro_path, sep=';', encoding='cp1252')
    df['db_site_name']=""

    ACPM_key = {"L'Aisnenouvelle.fr" : "aisnenouvelle.fr", "L'Equipe.fr":"lequipe.fr",
        "L'Obs.com":"nouvelobs.com", "Lathierache.fr":"la-thierache.fr", "LHumanité.fr":"humanite.fr",
        "Diabétologie-pratique.com":"diabetologie-pratique.com", "Gynécologie-pratique.com":"gynecologie-pratique.com" }

    for index, row in df.iterrows():
        if row['Sites'] in ACPM_key.keys():
            match = graph.nodes.match(site_name = ACPM_key[row['Sites']].lower()).first()
            df.loc[df['Sites'] == row['Sites'], ['db_site_name']] = ACPM_key[row['Sites']]
        else:
            match = graph.nodes.match(site_name = row['Sites'].lower()).first()
        if match != None:
            df.loc[df['Sites'] == row['Sites'], ['db_site_name']] = match['site_name']

            for key in row.keys():
                if match["ACPM_SitePro_" + key] == None:
                    match["ACPM_SitePro_" + key] = row[key]
            graph.push(match)
    print("no node match for these Site names, did not import:")
    print(df[df.db_site_name == ""]['Sites'])
