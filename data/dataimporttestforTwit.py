import networkx as nx
from py2neo import Graph, Node, Relationship
import pandas as pd
import random
from neo4j import GraphDatabase, basic_auth
import matplotlib

graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

# from this list only RS nodes are created (from hyphe file)
RSlist_gexfSiteName = ["facebook.com","fr-fr.facebook.com","fr-ca.facebook.com",
    "th-th.facebook.com", "es-la.facebook.com", "de-de.facebook.com",
    "developers.facebook.com", "business.facebook.com","m.facebook.com", "web.facebook.com",
    "twitter.com", "mobile.twitter.com","fr.twitter.com",
    "plus.google.com",
    "pinterest.com", "fr.pinterest.com", "uk.pinterest.com", "pl.pinterest.com",
    "linkedin.com", "fr.linkedin.com","ca.linkedin.com","tg.linkedin.com", "be.linkedin.com",
    "nl.linkedin.com", "ch.linkedin.com", "uk.linkedin.com","it.linkedin.com",
    "youtube.com"]

def importGexfNodesAndRel(gexffilepath, depth = 0):
    '''
    Reads gexf network file from hyphe, update or create all nodes in neo4j database:
    Website, Facebook, Linkedin, Pinterest and Twitter nodes
    Print . for each 100 nodes/links imported, 1000 for each 1000
    "depth" is used to prefix new properties on node and rel. Value can be 0, 1 or 2
    properties are imported as list, each member of the list is a non unique value from doublons
    '''
    G= nx.read_gexf(gexffilepath, node_type=None, relabel=False, version='1.1draft')
    data = nx.json_graph.node_link_data(G)
    totnbnodes=len(data['nodes'])
    print(totnbnodes," nodes found in gexf")
    i=1

    for node in data['nodes']:
        i=i+1
        site_name = node['label'].lower().split(" /")[0]

        if site_name in RSlist_gexfSiteName:
            # twitter node
            if "twitter" in node['label'].lower():
                if len(node['label'].lower().split(" /")) < 2:
                    print("I don't create", node['label'].lower())
                    nodematch = None
                else:
                    user_name = node['label'].lower().split(" /")[1]
                    user_name = user_name.replace("%20%e2%80%8f","").replace("%40%20%40","").replace("%40suivre%20sur%20twitter:%20%40","")
                    user_name = user_name.replace("%20","").replace("%40","").replace("%e2%81%a9","")
                    user_name = user_name.replace("%0apour","").replace("%0apou","").replace("%0apo","")
                    user_name = user_name.replace("%e2%80%a6","").replace("%e2%80%8e","").replace("%29","")
                    user_name = user_name.replace("%21","").replace("%c3%a9","e").replace("%c3%a1","a")
                    if "%" in user_name:
                        print("cannot create twitter node because of name:",node['label'])
                        nodematch = None
                    else:
                        nodematch = graph.nodes.match('Twitter', user_name = user_name).first()
                        if nodematch == None:
                            try:
                                nodematch = Node('Twitter', user_name = user_name)
                                nodematch.__primarylabel__ = 'Twitter'
                                nodematch.__primarykey__ = 'user_name'
                                graph.merge(nodematch)
                            except:
                                print("cannot create twitter node, unknown error", node['label'])
                                nodematch = None

            # facebook node
            elif "facebook" in node['label'].lower():
                if len(node['label'].lower().split(" /")) < 2 or ".php" in node['label'].lower():
                    nodematch = None
                    print("I don't create", node['label'].lower())
                else:
                    user_name = node['label'].lower().split(" /")[1]
                    if ".php" in user_name or user_name=="name":
                        print("cannot create facebook node because of name:",node['label'])
                        nodematch = None
                    else:
                        nodematch = graph.nodes.match('Facebook',user_name = user_name).first()
                        if nodematch == None:
                            try:
                                nodematch = Node('Facebook', user_name = user_name)
                                nodematch.__primarylabel__ = 'Facebook'
                                nodematch.__primarykey__ = 'user_name'
                                graph.merge(nodematch)
                            except:
                                print("cannot create facebook node, unknown error", node['label'])
                                nodematch = None


            # pinterest node
            elif "pinterest" in node['label'].lower():
                if len(node['label'].lower().split(" /")) < 2:
                    nodematch = None
                    print("I don't create", node['label'].lower())
                else:
                    user_name = node['label'].lower().split(" /")[1]
                    if user_name=="pin":
                        print("cannot create pinterest node because of name:",node['label'])
                        nodematch = None
                    else:
                        nodematch = graph.nodes.match('Pinterest',user_name = user_name).first()
                        if nodematch == None:
                            try:
                                nodematch = Node('Pinterest', user_name = user_name)
                                nodematch.__primarylabel__ = 'Pinterest'
                                nodematch.__primarykey__ = 'user_name'
                                graph.merge(nodematch)
                            except:
                                print("cannot create pinterest node, unknown error", node['label'])
                                nodematch = None


            # linkedin node
            elif "linkedin" in node['label'].lower():
                if len(node['label'].lower().split(" /")) < 2:
                    nodematch = None
                    print("I don't create", node['label'].lower())
                else:
                    user_name = node['label'].lower().split(" /")[1]
                    user_name = user_name.replace("%c3%a9", "e").replace("%26", "et").replace("%27","").replace("%c3%a7", "c").replace("%c3%bb","u")

                    nodematch = graph.nodes.match('Linkedin', user_name = user_name).first()
                    if nodematch == None:
                        try:
                            nodematch = Node('Linkedin', user_name = user_name)
                            nodematch.__primarylabel__ = 'Linkedin'
                            nodematch.__primarykey__ = 'user_name'
                            graph.merge(nodematch)
                        except:
                            print("cannot create linkedin node, unknown error", node['label'])
                            nodematch = None

            # youtube not imported
            else:
                nodematch = None

        else:
            if "twitter" in site_name or "facebook" in site_name or "linkedin" in site_name or "pinterest" in site_name:
                nodematch = None
                print(node['label'], "node not created (and it's OK)")

            else:
                site_name = node['label'].lower().split(" /")[0]
                nodematch = graph.nodes.match(site_name =site_name).first()
                if nodematch == None:
                    try:
                        nodematch = Node('Website', site_name = site_name)
                        nodematch.__primarylabel__ = 'Website'
                        nodematch.__primarykey__ = 'site_name'
                        graph.merge(nodematch)
                    except:
                        print("could not import ", node['label'])
                        nodematch = None

        # Create properties on nodes
        if nodematch != None:
            for key in node.keys():
                if nodematch["D" + str(depth) + "_" + key] == None:
                    nodematch["D" + str(depth) + "_" + key] = [node[key]]
                else:
                    if node[key] in nodematch["D" + str(depth) + "_" + key]:
                        pass
                    else:
                        nodematch["D" + str(depth) + "_" + key].append(node[key])
            graph.push(nodematch)
        else:
            pass
        if i%100 == 0:
            print(".", end=" ")
        if i%1000 ==0:
            print(i,"/",totnbnodes)
    print(i," nodes read")
    print(len(graph.nodes.match("Website")), "Websites in db after import")
    print(len(graph.nodes.match("Twitter")), "Twitter in db after import")
    print(len(graph.nodes.match("Facebook")), "Facebook in db after import")
    print(len(graph.nodes.match("Pinterest")), "Pinterest in db after import")
    print(len(graph.nodes.match("Linkedin")), "Linkedin in db after import")

    # Links
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
********************************************************************************
# This is what to run to get a brand new full DB
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
# Till here
********************************************************************************
# This is for test:


results = db.run("MATCH (w:Twitter) RETURN w.user_name")
df_twit = pd.DataFrame([r["w.user_name"] for r in results])

df_twit





********************************************************************************
'''
TODO :
- checker les noeuds sans liens (MATCH (n:Twitter) WHERE NOT (n)--() RETURN n.user_name)
- checker les relations multiples entre 2 noeuds pour faire les sommes de count (si besoin)
avec
match (n1)-[r1]->(n2)<-[r2]-(n1)
where type(r1)=type(r2)
return r1, r2
-



Règles des doublons trop compliqué. Zap pour quickness

- check des imports
- règles des doublons:
    - pour noeuds :
      - à la fin d'import, doit merger les prop lists en post processing? (1 import = 1 liste. voir ce qu'additionne et ce que garde)
        - quand importe sur noeuds qui existe, importe D0x_properties sous forme de liste
        - Compare D0x à D0 si existe. est-ce que garde tout (1 liste par import ?) ou prend celui qui a les plus gros chiffres (non si importe juste un truc en plus) ou plus récent ? abs
        -> regarder, faire ça par propriété, et recalculer les in /out degrés au lieu de les importer ?
    - pour les rel:
        - pour D0:
            - check les count D0. SI > 1 c'est quand même chelou
            - veut être restrictif (sur DISCO only): Si 3 imports successifs, garde celui qui est dans 2 ou plus, supprime les 1
        A FAIRE :
         pour les nons D0 ? Importe DiX avec count dans liste. si doublon dans même import additionne les count. A la fin prend le count le plus grand?
'''
********************************************************************************

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



*********************************************



# check que les names et prop sont cohérents :
results = graph.nodes.match("Website")
for r in results:
    if r["site_name"].lower() not in [x.lower() for x in r["D1_name"]]:
        print(r["site_name"].lower(), [x.lower() for x in r["D1_name"]])



results = graph.nodes.match("Facebook")
for r in results:
    if r["user_name"].lower() not in [x.lower().split(" /")[1] for x in r["D0_name"]]:
        print(r["user_name"].lower(), [x.lower().split(" /")[1] for x in r["D0_name"]])

results = graph.nodes.match("Twitter")
for r in results:
    if r["user_name"].lower() != r["D0_name"].lower().split(" /")[1]:
        print(r)

results = graph.nodes.match("Pinterest")
for r in results:
    if r["user_name"].lower() not in [x.lower().split(" /")[1] for x in r["D0_name"]]:
        print(r["user_name"].lower(), [x.lower().split(" /")[1] for x in r["D0_name"]])

results = graph.nodes.match("Linkedin")
for r in results:
    if r["user_name"].lower() not in [x.lower().split(" /")[1] for x in r["D0_name"]]:
        print(r["user_name"].lower(), [x.lower().split(" /")[1] for x in r["D0_name"]])
