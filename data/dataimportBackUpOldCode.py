import networkx as nx
from py2neo import Graph, Node, Relationship
import pandas as pd
import random


graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))





# TODO:
- ça à l'air de marcher pour les nodes, check que les rel D0 sont pas effacées quand rel D1 importé
- voir comment réimporter les D0 quand commence par D1
- faire pareil avec les discord
- Integrer les D2
- et faire requete pour display dans app


**********************************************************************************
# This imports Import Hyphe_D0.gexf
gexfPath = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_IN.gexf"

G = nx.read_gexf(gexfPath, node_type=None, relabel=False, version='1.1draft')
data_D0 = nx.json_graph.node_link_data(G)

for node in data_D0['nodes']:
    a = Node("Website",
        site_name = node['label'],
        D0_status = node['status'],
        D0_pages_total = node['pages_total'],
        D0_undirected_degree = node['undirected_degree'],
        D0_crawling_status = node['crawling_status'],
        D0_indegree = node['indegree'],
        D0_outdegree = node['outdegree'],
        D0_creation_date = node['creation_date'],
        D0_indexing_status = node['indexing_status'],
        D0_crawled = node['crawled'],
        D0_last_modification_date = node[ 'last_modification_date'],
        D0_pages_crawled = node['pages_crawled'],
        D0_name = node['name'],
        D0_label = node['label'],
        D0_id = node['id']
        )
    if len(graph.nodes.match(site_name = node['label'])) !=0:
        print(node)
        a["site_name"]=node['label'] + "X" + str(random.randint(1000,9999))
    a.__primarylabel__ = 'Website'
    a.__primarykey__ = 'site_name'
    graph.merge(a)

for link in data_D0['links']:
    try:
        tx = graph.begin()
        source_n = graph.nodes.match(D0_id = link['source']).first()
        target_n = graph.nodes.match(D0_id = link['target']).first()
        rel = Relationship(source_n, "LINKS_TO", target_n)
        rel["count_D0"]=link['count']
        tx.merge(rel)
        tx.commit()
    except:
        pass
    # regarde "label"
    # si label déjà dans base


len(data_D0['nodes'])
len(data_D0['links'])

len(graph.nodes.match("Website"))
len(graph.relationships.match())


********************************************************************************

# This imports in db data from 202005Websites01_D0.csv
WebD0_df=pd.read_csv("C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0.csv")

for index, row in WebD0_df.iterrows():
    node = graph.nodes.match(D0_id = str(row['ID'])).first()
    try:
        node['D0_home_page'] = row['HOME PAGE']
        node['D0_start_pages'] = row['START PAGES']
        graph.push(node)
    except:
        print(row['ID'])

# This imports crawl data :
Crawl_D0_df = pd.read_csv("C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_crawls.csv")

for index, row in Crawl_D0_df.iterrows():
    node = graph.nodes.match(D0_id = str(row['webentity_id'])).first()
    try:
        node['D0_max_depth']=row['max_depth']
        node['D0_nb_pages']=row['nb_pages']
        node['D0_nb_crawled_pages']=row['nb_crawled_pages']
        node['D0_nb_pages_indexed']=row['nb_pages_indexed']
        node['D0_nb_unindexed_pages']=row['nb_unindexed_pages']
        node['D0_nb_links']=row['nb_links']
        graph.push(node)
    except:
        print(row['webentity_id'])


*******************************************************************************
# TODO :
# - changer les labels des reseaux sociaux / trouver les usernames ou chaines
# - checker avec les RrRrRr si 2 noeuds ont même username. Dans ce cas faire merge.



*************************************************************************************
///////////////
# TUTO
#TUTO : This creates a Node, add a property and update db
a = Node("Website",site_name ="a")
graph.merge(a, 'Website', 'site_name' )

aa = list(graph.nodes.match(site_name = "a"))[0]
aa
aa['drop']="drop"
aa
graph.push(aa)
graph.nodes.match(site_name = "a").first()


# TUTO : This creates a Website node, change it to Twitter node
b = Node("Website",site_name ="b")
graph.merge(b, 'Website', 'site_name' )
bb = graph.nodes.match(site_name = "b").first()
bb

bb.labels
bb.add_label("Twitter")
bb.labels
bb.remove_label("Website")
bb.labels
graph.push(bb)
graph.nodes.match(site_name = "b").first()


# TUTO: add a property to a relationship
a = Node("Website",site_name ="a")
b = Node("Website",site_name ="b")
graph.merge(a, 'Website', 'site_name' )
graph.merge(b, 'Website', 'site_name' )
rel = Relationship(a, "LINKS_TO", b)
rel["count_D0"]="1"
graph.merge(rel)

rel["count_D1"]="2"
graph.push(rel)


source_n = graph.nodes.match("Website", site_name ="a").first()
target_n = graph.nodes.match("Website", site_name ="b").first()
rel2 = graph.relationships.match((a,b),r_type="LINKS_TO").first()
rel2["test"]="test"
graph.push(rel2)
////////////
************************************************************************************














# Import Hyphe_D1 : des erreurs !!
gexfPathD1 = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D1_IN.gexf"

GD1 = nx.read_gexf(gexfPathD1, node_type=None, relabel=False, version='1.1draft')
dataD1 = nx.json_graph.node_link_data(GD1)


for node in dataD1['nodes']:
    nodematch = graph.nodes.match(site_name =node['label']).first()
    if nodematch == None:
        try:
            nodematch = Node('Website',
            site_name = node['label'])
            nodematch.__primarylabel__ = 'Website'
            nodematch.__primarykey__ = 'site_name'
            graph.merge(nodematch)
        except:
            print(a)
    else:
        if "D1_status" in dict(nodematch):
            print(node['label'],"already in D1 base")
    for key in node.keys():
        nodematch["D1_"+key] = node[key]
        graph.push(nodematch)




for link in dataD1['links']:
    try:
        source_n = graph.nodes.match("Website", D1_id = link['source']).first()
        target_n = graph.nodes.match("Website", D1_id = link['target']).first()
        rel = Relationship(source_n, "LINKS_TO", target_n)

        rel["count_D1"]=link['count']
        tx.merge(rel)
        tx.commit()
    except:
        print(link)


********************************************************************************
#QC:
# QC
for node in dataD1['nodes']:
    if graph.nodes.match("Website", D1_id = node['id']).first()==None:
        print(node)
# Ce sont les nodes non importés dans base



len(dataD1['links'])
len(dataD1['nodes'])

len(graph.nodes.match("Website"))
len(graph.relationships.match())
