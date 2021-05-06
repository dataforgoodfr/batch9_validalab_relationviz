import networkx as nx
from py2neo import Graph, Node, Relationship
import pandas as pd
import random


graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))

def importGexfLinks(gexffilepath, depth = 0):
    # imports or update all nodes / relationships in gexf file from hyphe

    G= nx.read_gexf(gexffilepath, node_type=None, relabel=False, version='1.1draft')
    data = nx.json_graph.node_link_data(G)
    totnblinks=len(data['links'])
    print(totnblinks," links found in gexf")

    j=1
    for link in data['links']:
        source_n = graph.nodes.match("Website", D2_id = link['source']).first()
        target_n = graph.nodes.match("Website", D2_id = link['target']).first()
        relmatch = graph.relationships.match((source_n,target_n),r_type="LINKS_TO").first()

        try:
            if relmatch == None:
                rel = Relationship(source_n, "LINKS_TO", target_n)
                rel["count_D" + str(depth)]=link['count']
                graph.merge(rel)
            else:
                relmatch["count_D" + str(depth)]=link['count']
                graph.push(relmatch)
            if j%100 == 0:
                print(".", end=" ", flush=True)
            if j%5000 ==0:
                print(j*5000,"/",totnblinks)
            j=j+1
        except:
            print("error")

    print(j," links imported")
    print(len(graph.relationships.match()), "links in db after import")

pathD2Disco = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005_Websites01_D2_DISCO.gexf"


print("yeah")
print("yop")
importGexfLinks(pathD2Disco, 2)
