import networkx as nx
from py2neo import Graph, Node, Relationship
import pandas as pd
import random




graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))

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



# This imports all gexf files (takes time)

pathD0IN = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D0_IN.gexf"
importGexf(pathD0IN, 0)

pathD1IN = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005Websites01_D1_IN.gexf"
importGexf(pathD1IN, 1)

# This has not been done entirely
pathD2IN = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\HypheExport20200520\\202005_Websites01_D2_IN.gexf"
importGexf(pathD2IN, 2)
