import pandas as pd
import numpy as np
from neo4j import GraphDatabase, basic_auth
from py2neo import Graph, Node, Relationship

graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

medias_df = pd.read_csv('C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/mondediplo/medias_francais.tsv', sep='	')
relations_df = pd.read_csv('C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/mondediplo/relations_medias_francais.tsv', sep='	')
medias_df_relDB  = pd.read_csv('C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/mondediplo/medias_francais_relDB.tsv', sep=',')

medias_df.head()
medias_df.typeLibelle.unique()
relations_df[relations_df.cible == "Groupe Perdriel"]


*********************************************************************************
'''
ça ça importe bien les 2 fichiers monde diplo
creer procedure à partir de ça
Puis avec medias_df_relDB relier aux Websites
'''

def importMondeDiploFiles(path_to_medias_francais, path_to_relations_medias_francais):
    ''' import or mondediplo files in Neo4j database as "Entity" nodes and "ONED_BY"
    relationships. Create all properties as Diplo_propname.
    No relationship cretaed to other nodes
    '''
    medias_df = pd.read_csv(medias_path, sep='	')
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






medias_path = 'C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/mondediplo/medias_francais.tsv'
relations_medias_path = 'C:/Users/Jo/Documents/Tech/Atom_prj/MyMedia-FillDB/data/mondediplo/relations_medias_francais.tsv'
importMondeDiploFiles(medias_path, relations_medias_path)
