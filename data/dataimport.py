import networkx as nx
from py2neo import Graph, Node, Relationship
import pandas as pd
import random
from neo4j import GraphDatabase, basic_auth
import matplotlib


graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
********************************************************************************
# functions to import here


def importGexf(gexffilepath, depth = 0):
    '''
    Reads gexf network file from hyphe, update or create all nodes and relationships in neo4j database
    Print . for each 100 nodes/links imported, 1000 for each 1000
    "depth" is used to prefix new properties on node and rel. Value can be 0, 1 or 2
    '''

    # imports or update all nodes / relationships in gexf file from hyphe

    G= nx.read_gexf(gexffilepath, node_type=None, relabel=False, version='1.1draft')
    data = nx.json_graph.node_link_data(G)
    totnbnodes=len(data['nodes'])
    print(totnbnodes," nodes found in gexf")
    i=1

    for node in data['nodes']:
        i=i+1
        nodematch = graph.nodes.match(site_name =node['label']).first()
        if nodematch == None:
            try:
                nodematch = Node('Website', site_name = node['label'])
                nodematch.__primarylabel__ = 'Website'
                nodematch.__primarykey__ = 'site_name'
                graph.merge(nodematch)
            except:
                print("could not import ", node['label'])

        for key in node.keys():
            nodematch["D" + str(depth) + "_" + key] = node[key]
            graph.push(nodematch)
        if i%100 == 0:
            print(".", end=" ")
        if i%1000 ==0:
            print(i,"/",totnbnodes)

    print(i," nodes imported")
    print(len(graph.nodes.match("Website")), "nodes in db after import")

    totnblinks=len(data['links'])
    print(totnblinks," links found in gexf")

    j=0
    for link in data['links']:

        if depth ==0:
            source_n = graph.nodes.match("Website", D0_id = link['source']).first()
            target_n = graph.nodes.match("Website", D0_id = link['target']).first()
        if depth == 1:
            source_n = graph.nodes.match("Website", D1_id = link['source']).first()
            target_n = graph.nodes.match("Website", D1_id = link['target']).first()
        if depth == 2:
            source_n = graph.nodes.match("Website", D2_id = link['source']).first()
            target_n = graph.nodes.match("Website", D2_id = link['target']).first()
        if depth == 3:
            source_n = graph.nodes.match("Website", D3_id = link['source']).first()
            target_n = graph.nodes.match("Website", D3_id = link['target']).first()
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
                print(".", end=" ")
            if j%1000 ==0:
                print(j, "/", totnblinks)
            j=j+1
        except:
            pass
    print(j," links imported")
    print(len(graph.relationships.match()), "links in db after import")

********************************************************************************
# Modifying this to import twitter nodes
def importGexfWithLabels(gexffilepath, depth = 0):
    '''
    Reads gexf network file from hyphe, update or create all nodes and relationships in neo4j database
    Print . for each 100 nodes/links imported, 1000 for each 1000
    "depth" is used to prefix new properties on node and rel. Value can be 0, 1 or 2
    '''

    # imports or update all nodes / relationships in gexf file from hyphe

    G= nx.read_gexf(gexffilepath, node_type=None, relabel=False, version='1.1draft')
    data = nx.json_graph.node_link_data(G)
    totnbnodes=len(data['nodes'])
    print(totnbnodes," nodes found in gexf")
    i=1

    for node in data['nodes']:
        i=i+1
        nodematch = graph.nodes.match(site_name =node['label']).first()
        if nodematch == None:
            try:
                nodematch = Node('Website', site_name = node['label'])
                nodematch.__primarylabel__ = 'Website'
                nodematch.__primarykey__ = 'site_name'
                graph.merge(nodematch)
            except:
                print("could not import ", node['label'])

        for key in node.keys():
            nodematch["D" + str(depth) + "_" + key] = node[key]
            graph.push(nodematch)
        if i%100 == 0:
            print(".", end=" ")
        if i%1000 ==0:
            print(i,"/",totnbnodes)

    print(i," nodes imported")
    print(len(graph.nodes.match("Website")), "nodes in db after import")

    totnblinks=len(data['links'])
    print(totnblinks," links found in gexf")

    j=0
    for link in data['links']:

        if depth ==0:
            source_n = graph.nodes.match("Website", D0_id = link['source']).first()
            target_n = graph.nodes.match("Website", D0_id = link['target']).first()
        if depth == 1:
            source_n = graph.nodes.match("Website", D1_id = link['source']).first()
            target_n = graph.nodes.match("Website", D1_id = link['target']).first()
        if depth == 2:
            source_n = graph.nodes.match("Website", D2_id = link['source']).first()
            target_n = graph.nodes.match("Website", D2_id = link['target']).first()
        if depth == 3:
            source_n = graph.nodes.match("Website", D3_id = link['source']).first()
            target_n = graph.nodes.match("Website", D3_id = link['target']).first()
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
                print(".", end=" ")
            if j%1000 ==0:
                print(j, "/", totnblinks)
            j=j+1
        except:
            pass
    print(j," links imported")
    print(len(graph.relationships.match()), "links in db after import")


pathD0IN = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_IN.gexf"
importGexf(pathD0IN, 0)







def importGexfLinks(gexffilepath, depth = 0):
    '''
    Reads gexf network file from hyphe, update or create relationships in neo4j database
    Print . for each 100 links imported, 1000 for each 1000
    "depth" is used to prefix new properties on rel. Value can be 0, 1 or 2
    '''

    G= nx.read_gexf(gexffilepath, node_type=None, relabel=False, version='1.1draft')
    data = nx.json_graph.node_link_data(G)
    totnblinks=len(data['links'])
    print(totnblinks," links found in gexf")

    j=1
    for link in data['links']:

        if depth ==0:
            source_n = graph.nodes.match("Website", D0_id = link['source']).first()
            target_n = graph.nodes.match("Website", D0_id = link['target']).first()
        if depth == 1:
            source_n = graph.nodes.match("Website", D1_id = link['source']).first()
            target_n = graph.nodes.match("Website", D1_id = link['target']).first()
        if depth == 2:
            source_n = graph.nodes.match("Website", D2_id = link['source']).first()
            target_n = graph.nodes.match("Website", D2_id = link['target']).first()
        if depth == 3:
            source_n = graph.nodes.match("Website", D3_id = link['source']).first()
            target_n = graph.nodes.match("Website", D3_id = link['target']).first()
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
                print(".", end=" ")
            if j%1000 ==0:
                print(j ,"/",totnblinks)
            j=j+1
        except:
            pass
    print(j," links imported")
    print(len(graph.relationships.match()), "links in db after import")


# functions to import - END
********************************************************************************

# This imports all gexf files (takes time)

pathD0IN = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_IN.gexf"
importGexf(pathD0IN, 0)

pathD1IN = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D1_IN.gexf"
importGexf(pathD1IN, 1)

# This has not been done entirely
pathD2IN = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005_Websites01_D2_IN.gexf"
importGexf(pathD2IN, 2)

pathD0Disco = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_DISCO.gexf"
importGexf(pathD0Disco, 0)




********************************************************************************
# Compute Data Science Algo on graph

db = driver.session()

def createGraphInDGDSCatalog(name = "mygraph"):
    results = db.run(
        "CALL gds.graph.create($graph_name ,'Website','LINKS_TO',"
        "{relationshipProperties:'count_D0'})"
        "YIELD graphName, nodeCount, relationshipCount, createMillis", {"graph_name":name}
        )
    for record in results:
        print("graph created in GDS catalog with name:",record['graphName'],",",record['nodeCount'],"nodes,", record['relationshipCount'],"relationships in", record['createMillis'], "milliseconds")
    return(name)

def createUndirectedGraphInGDSCatalog(name = "myundirectedgraph" ):
    results = db.run(
        "CALL gds.graph.create($graph_name, 'Website',{LINKS_TO: {orientation: 'UNDIRECTED'}})"
        "YIELD graphName, nodeCount, relationshipCount, createMillis", {"graph_name":name}
        )
    for record in results:
        print("undirected graph created in GDS catalog with name:",record['graphName'],",",record['nodeCount'],"nodes,", record['relationshipCount'],"relationships in", record['createMillis'], "milliseconds")
    return(name)

def isGraphInDGSCatalog(name):
    results = db.run(
        "CALL gds.graph.exists($graph_name) YIELD exists;", {"graph_name":name}
        )
    for record in results:
        r=record
    return(r["exists"])

def removeGraphFromDGSCatalog(name):
    results = db.run(
        "CALL gds.graph.drop($graph_name) YIELD graphName;", {"graph_name":name}
        )
    for record in results:
        print("graph ",record["graphName"], "removed")

def listGraphInDGSCatalog():
    result = db.run(
        "CALL gds.graph.list()"
        "YIELD graphName, nodeProjection, relationshipProjection, nodeQuery, relationshipQuery,"
        "nodeCount, relationshipCount, degreeDistribution, creationTime, modificationTime;"
    )
    for record in result:
        print(record)

def computePageRank(graph_name):
    result = db.run(
        "CALL gds.pageRank.write($graph_name, {"
        "maxIterations: 20,"
        "dampingFactor: 0.85,"
        "writeProperty: 'PageRank'})"
        "YIELD nodePropertiesWritten, ranIterations, computeMillis, didConverge"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("Unweighted PageRank run in", record["ranIterations"],"iterations and", record["computeMillis"], "ms on", record["nodePropertiesWritten"], "nodes and converged:",record["didConverge"])
    return('PageRank')

def computeArticleRank(graph_name):
    result = db.run(
        "CALL gds.alpha.articleRank.write($graph_name, {"
        "writeProperty: 'ArticleRank'})"
        "YIELD nodes, iterations, createMillis, computeMillis, writeMillis, dampingFactor, writeProperty"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("ArticleRank run on",record["nodes"],"nodes in",record["iterations"],"iterations and",record["computeMillis"],"ms. DampingFactor=", record["dampingFactor"])
    return('ArticleRank')

def computeBetweennessCentrality(graph_name):
    result = db.run(
        "CALL gds.alpha.betweenness.write($graph_name, {"
        "writeProperty: 'BetweennessCentrality'})"
        "YIELD nodes, minCentrality, maxCentrality, sumCentrality, createMillis, computeMillis, writeMillis"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("BetweennessCentrality run on",record["nodes"],"nodes in",record["computeMillis"],"ms. minCentrality:",record["minCentrality"],", maxCentrality:",record["maxCentrality"],", sumCentrality:",record["sumCentrality"])
    return('BetweennessCentrality')

def computeClosenessCentrality(graph_name):
    result = db.run(
        "CALL gds.alpha.closeness.write($graph_name, {"
        "writeProperty: 'ClosenessCentrality'})"
        "YIELD nodes, computeMillis"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("ClosenessCentrality run on",record["nodes"],"nodes in",record["computeMillis"],"ms.")
    return('ClosenessCentrality')

def computeDegreeCentrality(graph_name):
    result = db.run(
        "CALL gds.alpha.degree.write($graph_name, {"
        "writeProperty: 'DegreeCentrality'})"
        "YIELD nodes, computeMillis"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("DegreeCentrality run on",record["nodes"],"nodes in",record["computeMillis"],"ms.")
    return('DegreeCentrality')

def computeEigenvectorCentrality(graph_name):
    result = db.run(
        "CALL gds.alpha.eigenvector.write($graph_name, {"
        "writeProperty: 'EigenvectorCentrality'})"
        "YIELD nodes, computeMillis"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("EigenvectorCentrality run on",record["nodes"],"nodes in",record["computeMillis"],"ms.")
    return('EigenvectorCentrality')


def computeLouvainCommunity(graph_name):
    result = db.run(
        "CALL gds.louvain.write($graph_name, {"
        "writeProperty: 'LouvainCommunity' })"
        "YIELD communityCount, modularity, modularities, ranLevels, nodePropertiesWritten, computeMillis"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("LouvainCommunity run on",record["nodePropertiesWritten"],"nodes in",record["computeMillis"],"ms. communityCount:",record["communityCount"],", modularity:",record["modularity"],",modularities:",record["modularities"],",ranLevels:",record["modularities"])
    return('LouvainCommunity')

def computeLabelPropagationCommunity(graph_name):
    result = db.run(
        "CALL gds.labelPropagation.write($graph_name, {"
        "writeProperty: 'LabelPropagationCommunity' })"
        "YIELD communityCount, ranIterations, nodePropertiesWritten, computeMillis, didConverge"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("LabelPropagationCommunity run on",record["nodePropertiesWritten"],"nodes in",record["computeMillis"],"ms. communityCount:",record["communityCount"],", ranIterations:",record["ranIterations"],",didConverge:",record["didConverge"])
    return('LabelPropagationCommunity')

def computeWeaklyConnectedComponents(graph_name):
    result = db.run(
        "CALL gds.wcc.write($graph_name, {"
        "writeProperty: 'WeaklyConnectedComponents' })"
        "YIELD computeMillis, nodePropertiesWritten, componentCount"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("WeaklyConnectedComponents run on",record["nodePropertiesWritten"],"nodes in",record["computeMillis"],"ms. componentCount:",record["componentCount"])
    return('WeaklyConnectedComponents')

def computeTriangleCount(graph_name):
    result = db.run(
        "CALL gds.alpha.triangleCount.write($graph_name, {"
        "writeProperty: 'TriangleCount' })"
        "YIELD computeMillis, triangleCount, nodeCount"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("TriangleCount run on",record["nodeCount"],"nodes in",record["computeMillis"],"ms. TriangleCount:",record["triangleCount"])
    return('TriangleCount')

def computeModularityOptimization(graph_name):
    result = db.run(
        "CALL gds.beta.modularityOptimization.write($graph_name, {"
        "writeProperty: 'ModularityOptimization' })"
        "YIELD computeMillis, nodes, didConverge, ranIterations, modularity, communityCount"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("ModularityOptimization run on",record["nodes"],"nodes in",record["computeMillis"],"ms and",record["ranIterations"],"iterations. didConverge:",record["didConverge"],",modularity:",record["didConverge"],",communityCount:",record["communityCount"])
    return('ModularityOptimization')

def computeStronglyConnectedComponents(graph_name):
    result = db.run(
        "CALL gds.alpha.scc.write($graph_name, {"
        "writeProperty: 'StronglyConnectedComponents' })"
        "YIELD computeMillis, nodes, communityCount, p1, p5, p10, p25, p50, p75, p90, p99, p100"
        , {"graph_name":graph_name}
    )
    for record in result:
        print("StronglyConnectedComponents run on",record["nodes"],"nodes in",record["computeMillis"],"ms . communityCount:",record["communityCount"],",p1:",record["p1"],",p5:",record["p5"],",p10:",record["p10"],",p25:",record["p25"],",p50:",record["p50"],",p75:",record["p75"],",p90:",record["p90"],",p99:",record["p99"],",p100:",record["p100"])
    return('StronglyConnectedComponents')

def computeAllCentralitiesAndCommunities(graph_name = "mygraph"):
    if not isGraphInDGSCatalog(graph_name):
        createGraphInDGDSCatalog(graph_name)
    if not isGraphInDGSCatalog(graph_name + "_undirected"):
        createUndirectedGraphInGDSCatalog(graph_name + "_undirected")
    computePageRank(graph_name)
    computeArticleRank(graph_name)
    computeBetweennessCentrality(graph_name)
    computeClosenessCentrality(graph_name)
    computeDegreeCentrality(graph_name)
    computeEigenvectorCentrality(graph_name)
    computeLouvainCommunity(graph_name)
    computeLabelPropagationCommunity(graph_name)
    computeWeaklyConnectedComponents(graph_name)
    computeTriangleCount(graph_name + "_undirected")
    computeModularityOptimization(graph_name)
    computeStronglyConnectedComponents(graph_name)




computePageRank("mygraph")

computeAllCentralitiesAndCommunities()




********************************************************************************
# QC properties

# list all node keys
def dfWebsiteProperties():
    res = db.run("MATCH (w:Website) UNWIND keys(w) AS key RETURN collect(distinct key)")
    for i in res:
        columns=i[0]
        df = pd.DataFrame(columns=columns)
    res = db.run("MATCH (w:Website) RETURN(w)")
    for r in res:
        df = df.append(dict(r["w"]), ignore_index=True)
    return(df)

df = dfWebsiteProperties()

df.describe(include='all')
df['LouvainCommunity'].describe()

df['StronglyConnectedComponents'].hist()
df['LouvainCommunity'].head(20)

df['site_name']
'''
# TODO:
- refaire test computation all
- check pourquoi pagerank ne converge pas

et tester sur différents graph D0 D1 IN et DISCO
- faire des graph des propriétés (histogrammes, valeurs min max)
- faire algo qui normalise automatiquement les prop:
    - size avec petit/grand
    - couleurs : tester continuous et categorielles ?

check comment ameliorer le graph et les relationships
voir comment ajouter un bouton pour choisir ce qui est mis en size et ce qui est en couleur
'''

for n in graph.nodes.match():
    if "/" in n["site_name"]:
        name = n["site_name"].split(" /")[0]
        exists = graph.nodes.match(site_name = name).first()
        if exists != None:
            print("merge:", exists['site_name'],name, n["site_name"])
        else:
            print("error: no not without / ",name, n["site_name"] )
graph.nodes.match(site_name = "6play.fr").first()['site_name']


'''
Les checks à faire :
- que les noms n['D0_name'] == n['D1_name']== n['D2_name']==n['site_name'] ou alors None
- faire une fonction merge node
- virer les "/", merger les nodes si besoin
- faire une fonction qui change les twitters et youtube nodes des noeuds DISCO
- faire une fonction check/clean DB:
    - renvoie les stats basiques (nombre de noeuds par types / links par types, qu'est-ce qui a été importé (D0, D1, D2, combien IN), mondiplo et autres...
    - vire les /, merge les doublons (voir comment se comporte sur les DISCO)
    - propose des nouveaux crawls

Faire procédure réutilisable pour les imports customs:
- trouver moyen d'importer mondediplo:
    - load les 2 fichiers mondiplo en df
    - pour chaque entité mondiplo propose equivalence dans base
    - met ça dans df pour check manuel
- pareil pour ACPM

Import des réseaux sociaux
'''
********************************************************************************

def findNodeNamesInconsistencies(D0_name = 'D0_name', D1_name = 'D1_name', D2_name = 'D0_name', site_name = 'site_name'):
    print("1: Checking name inconsistencies for ",len(graph.nodes.match())," nodes in DB" )
    D0_ct = D1_ct = D2_ct = st_none = 0
    for n in graph.nodes.match():
        #print(n[D0_name],n[D1_name], n[D2_name], n[site_name])
        error = 0
        if n[site_name] == None:
            st_none = st_none +1
            error = 1
        else:
            if n[D0_name]==n[D1_name]== n[D2_name]==n[site_name]:
                D0_ct = D0_ct +1
                D1_ct = D1_ct +1
                D2_ct = D2_ct +1
            else:
                if (n[D0_name]==None or n[D0_name]==n[site_name]):
                    if n[D0_name]==n[site_name]:
                        D0_ct = D0_ct +1
                else:
                    print("D0: ",n[D0_name], n[site_name])
                    error=1

                if (n[D1_name]==None or n[D1_name]==n[site_name]):
                    if n[D1_name]==n[site_name]:
                        D1_ct = D1_ct +1
                else:
                    print("D1: ",n[D1_name], n[site_name])
                    error=1

                if (n[D2_name]==None or n[D2_name]==n[site_name]):
                    if n[D2_name]==n[site_name]:
                        D2_ct = D2_ct +1
                else:
                    print("D2: ",n[D2_name], n[site_name])
                    error=1

                if (n[D0_name]==n[D1_name]==n[D2_name]==None):
                    error=1

        if error ==1:
            print("error: incompatible node names with ",n[D0_name],n[D1_name], n[D2_name], n[site_name])
    print(st_none, "None site_name, ", D0_ct, "D0 nodes", D1_ct, "D1 nodes", D2_ct, "D2 nodes")


findNodeNamesInconsistencies()








*****************


def importcrawlcsv(csvfilepath, depth = 0):
    df = pd.read_csv(csvfilepath)

    if depth == 0:
        for index, row in df.iterrows():
            node = graph.nodes.match(D0_id = str(row['webentity_id'])).first()
            try:
                if (node['D0_nb_crawled_pages'] == None or node['D0_nb_crawled_pages'] < row['nb_crawled_pages']):
                    node['D0_max_depth']=row['max_depth']
                    node['D0_nb_pages']=row['nb_pages']
                    node['D0_nb_crawled_pages']=row['nb_crawled_pages']
                    node['D0_nb_pages_indexed']=row['nb_pages_indexed']
                    node['D0_nb_unindexed_pages']=row['nb_unindexed_pages']
                    node['D0_nb_links']=row['nb_links']
                    node['D0_start_urls']=row['start_urls']
                    graph.push(node)
            except:
                if row['globalStatus']=="UNSUCCESSFUL":
                    print('not crawled :', row['webentity_id'],";",row['webentity_name'])
                else:
                    print("error with :", row['webentity_id'],";",row['webentity_name'])

    if depth == 1:
        for index, row in df.iterrows():
            node = graph.nodes.match(D1_id = str(row['webentity_id'])).first()
            try:
                if (node['D1_nb_crawled_pages'] == None or node['D1_nb_crawled_pages'] < row['nb_crawled_pages']):
                    node['D1_max_depth']=row['max_depth']
                    node['D1_nb_pages']=row['nb_pages']
                    node['D1_nb_crawled_pages']=row['nb_crawled_pages']
                    node['D1_nb_pages_indexed']=row['nb_pages_indexed']
                    node['D1_nb_unindexed_pages']=row['nb_unindexed_pages']
                    node['D1_nb_links']=row['nb_links']
                    node['D1_start_urls']=row['start_urls']
                    graph.push(node)
            except:
                if row['globalStatus']=="UNSUCCESSFUL":
                    print('not crawled :', row['webentity_id'],";",row['webentity_name'])
                else:
                    print("error with :", row['webentity_id'],";",row['webentity_name'])

    if depth == 2:
        for index, row in df.iterrows():
            node = graph.nodes.match(D2_id = str(row['webentity_id'])).first()
            try:
                if (node['D2_nb_crawled_pages'] == None or node['D2_nb_crawled_pages'] < row['nb_crawled_pages']):
                    node['D2_max_depth']=row['max_depth']
                    node['D2_nb_pages']=row['nb_pages']
                    node['D2_nb_crawled_pages']=row['nb_crawled_pages']
                    node['D2_nb_pages_indexed']=row['nb_pages_indexed']
                    node['D2_nb_unindexed_pages']=row['nb_unindexed_pages']
                    node['D2_nb_links']=row['nb_links']
                    node['D2_start_urls']=row['start_urls']
                    graph.push(node)
            except:
                if row['globalStatus']=="UNSUCCESSFUL":
                    print('not crawled :', row['webentity_id'],";",row['webentity_name'])
                else:
                    print("error with :", row['webentity_id'],";",row['webentity_name'])




# Done, 159 errors
csvcrawledpathD0 = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_crawls.csv"
importcrawlcsv(csvcrawledpathD0, 0)

# Done, 388 errors
csvcrawledpathD1 = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D1_crawls.csv"
importcrawlcsv(csvcrawledpathD1, 1)


csvcrawledpathD2 = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005_Websites01_D2_crawls.csv"
importcrawlcsv(csvcrawledpathD2, 2)





def importStartPages(csvfilepath, depth = 0):
    ''' imports csv export from hyphe, update "Di_start_pages" field on node based on csv
    '''
    df = pd.read_csv(csvfilepath)

    if depth == 0:
        for index, row in df.iterrows():
            if not pd.isnull(row['START PAGES']):
                node = graph.nodes.match(D0_id = str(row['ID'])).first()
                try:
                    node['D0_start_pages']=row['START PAGES']
                    graph.push(node)
                except:
                    print("error with :", row['ID'],";",row['NAME'])

    if depth == 1:
        for index, row in df.iterrows():
            if not pd.isnull(row['START PAGES']):
                node = graph.nodes.match(D0_id = str(row['ID'])).first()
                try:
                    node['D1_start_pages']=row['START PAGES']
                    graph.push(node)
                except:
                    print("error with :", row['ID'],";",row['NAME'])

    if depth == 2:
        for index, row in df.iterrows():
            if not pd.isnull(row['START PAGES']):
                node = graph.nodes.match(D0_id = str(row['ID'])).first()
                try:
                    node['D2_start_pages']=row['START PAGES']
                    graph.push(node)
                except:
                    print("error with :", row['ID'],";",row['NAME'])



csvstartpathD0 = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0.csv"
importStartPages(csvstartpathD0, 0)

csvstartpathD1 = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D01.csv"
importStartPages(csvstartpathD1, 1)

csvstartpathD2 = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005_Websites01_D2IN.csv"
importStartPages(csvstartpathD2, 2)





'''

Check les noeuds IN qui n'ont pas du tout de homepage ou startpage
Il y en a ??
Si oi, regarde dans fichier et importe
'''


'''
Check db:
fonction qui stat :
regarde les noeuds IN:
- Combien ont D0 / D1 et D2
- qui n'a pas un des 3 ?
- check nodes qui n'ont pas même name en D0,D1 et D2
- check pareil les links:
combien avec D0, D1, D2  uniquement
D0-D1, D1-D2, D0-D2
D0-D1-D2

'''

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
#TUTO : This creates a Node, add a property list and update db
a = Node("Website",site_name ="a")
graph.merge(a, 'Website', 'site_name' )

aa = list(graph.nodes.match(site_name = "a"))[0]
aa
aa['drop']=["drop"]
aa
graph.push(aa)
graph.nodes.match(site_name = "a").first()
aaa=list(graph.nodes.match(site_name = "a"))[0]
aa['drop'].append('test')
aa['drop']
graph.push(aaa)
graph.nodes.match(site_name = "a").first()

graph.delete(aaa)

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

source_n
a = Node("Website",site_name ="a")
graph.relationships.match((a,b),r_type="LINKS_TO").first()


////////////
************************************************************************************






********************************************************************************
#QC:
# QC
for node in dataD1['nodes']:
    if graph.nodes.match("Website", D1_id = node['id']).first()==None:
        print(node)
# Ce sont les nodes non importés dans base



len(data['links'])
len(data['nodes'])

len(graph.nodes.match("Website"))
len(graph.relationships.match())


gexfPath = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_IN.gexf"
gexffilepath = gexfPath

def importGexf(gexffilepath, depth = 0):
    # imports or update all nodes / relationships in gexf file from hyphe

    G= nx.read_gexf(gexffilepath, node_type=None, relabel=False, version='1.1draft')
    data = nx.json_graph.node_link_data(G)

    for node in data['nodes']:
        nodematch = graph.nodes.match(site_name =node['label']).first()
        if nodematch == None:
            try:
                nodematch = Node('Website', site_name = node['label'])
                nodematch.__primarylabel__ = 'Website'
                nodematch.__primarykey__ = 'site_name'
                graph.merge(nodematch)
            except:
                print("could not import ", node['label'])

        for key in node.keys():
            nodematch["D" + str(depth) + "_" + key] = node[key]
            graph.push(nodematch)

    for link in data['links']:
        if depth ==0:
            source_n = graph.nodes.match("Website", D0_id = link['source']).first()
            target_n = graph.nodes.match("Website", D0_id = link['target']).first()
        if depth == 1:
            source_n = graph.nodes.match("Website", D1_id = link['source']).first()
            target_n = graph.nodes.match("Website", D1_id = link['target']).first()
        if depth == 2:
            source_n = graph.nodes.match("Website", D2_id = link['source']).first()
            target_n = graph.nodes.match("Website", D2_id = link['target']).first()
        if depth == 3:
            source_n = graph.nodes.match("Website", D3_id = link['source']).first()
            target_n = graph.nodes.match("Website", D3_id = link['target']).first()
        relmatch = graph.relationships.match((source_n,target_n),r_type="LINKS_TO").first()

        if relmatch == None:
            rel = Relationship(source_n, "LINKS_TO", target_n)
            rel["count_D" + str(depth)]=link['count']
            graph.merge(rel)
        else:
            relmatch["count_D" + str(depth)]=link['count']
            graph.push(relmatch)


pathD0Disco = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_DISCO.gexf"
importGexf(pathD0Disco, 0)

pathD1Disco = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D1_DISCO.gexf"
importGexf(pathD1Disco, 1)

pathD2Disco = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005_Websites01_D2_DISCO.gexf"
importGexf(pathD1Disco, 2)

********************************************************************************

# This imports in db data from 202005Websites01_D1.csv
WebD1_df=pd.read_csv("C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D01.csv")

for index, row in WebD1_df.iterrows():
    node = graph.nodes.match(D0_id = str(row['ID'])).first()
    try:
        node['D1_home_page'] = row['HOME PAGE']
        node['D1_start_pages'] = row['START PAGES']
        graph.push(node)
    except:
        pass

# This imports crawl data :
Crawl_D1_df = pd.read_csv("C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D1_crawls.csv")

for index, row in Crawl_D1_df.iterrows():
    node = graph.nodes.match(D1_id = str(row['webentity_id'])).first()
    try:
        node['D1_max_depth']=row['max_depth']
        node['D1_nb_pages']=row['nb_pages']
        node['D1_nb_crawled_pages']=row['nb_crawled_pages']
        node['D1_nb_pages_indexed']=row['nb_pages_indexed']
        node['D1_nb_unindexed_pages']=row['nb_unindexed_pages']
        node['D1_nb_links']=row['nb_links']
        graph.push(node)
    except:
        pass

********************************************************************************

'''
Calculer:
- check comment installer les algos de graph
- le nb de connection in/out de chaque noeud, pour chaque D -APOC degree function
- quels noeuds sont connectés / non connectés
-
'''
********************************************
# This computes global in and out degree for all nodes
len(graph.nodes.match(D0_status = "IN"))
len(graph.nodes.match(D1_status = "IN"))
len(graph.nodes.match(D2_status = "IN"))
for node in graph.nodes.match(D0_status = "IN"):
    print(node['site_name'], len(graph.relationships.match((node,), r_type="LINKS_TO")),
    len(graph.relationships.match((None,node), r_type="LINKS_TO")))


graph.nodes.match(D0_status = "IN").first()





lemonde_n = graph.nodes.match(site_name="lemonde.fr").first()
Link_to_lemonde_r = graph.match(r_type="LINKS_TO", nodes=(None, lemonde_n))
for rel in Link_to_lemonde_r:
    print(rel['count_D2'], rel.nodes[0]['site_name'])
    # %% codecell
    # liste des sites que lemonde link avec le count
for r in graph.match((graph.nodes.match(site_name="lemonde.fr").first(),)):
    print(r['count_D2'], r.end_node['site_name'])
