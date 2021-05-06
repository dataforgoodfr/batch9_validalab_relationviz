# %% md
# This code
- compares gexf and firstpage for RS links,
- create a csv file for manual edition of the RS usernames
- load the file and create or update links and RSnodes
- It also creates entities

# %% A Initialisation
import networkx as nx
from py2neo import Graph, Node, Relationship
import pandas as pd
from neo4j import GraphDatabase, basic_auth

graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

# results = db.run("MATCH (w:Website) WHERE 'IN' in w.D1_status RETURN w.site_name, w.D1_homepage")
# df = pd.DataFrame([r["w.D1_homepage"] for r in results])

# %% Load gexf
gexfD0DISCO="C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\202007WebsitesRS_D0\\202007WebsiteRSD0.gexf"
G= nx.read_gexf(gexfD0DISCO, node_type=None, relabel=False, version='1.1draft')
data = nx.json_graph.node_link_data(G)
totnbnodes=len(data['nodes'])
print(totnbnodes," nodes found in gexf")


# %% A Lance les firstpages : import des procedures
import sys
dataPath = 'C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data'
if dataPath not in sys.path:
    sys.path.insert(0, dataPath)

from FromNotebook import getFirstPageNode, getFirstPageRSNode, myUrlParse
# %% This downloads all firstpages if not there and put results in firstPageRS_df

medialistD0path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\202007WebsitesRS_D0\\20200709_medialistForD0.txt"
firstPageRS_df = pd.DataFrame(columns=["site", "fb", "twit", "yt_user","yt_url","sc","insta","dm"])
with open(medialistD0path, "r", encoding='utf-8') as file:
    content = file.readlines()
    content = [x.strip() for x in content]
    for c in content:
        #getFirstPageNode(c)
        RS = getFirstPageRSNode(c)
        firstPageRS_df = firstPageRS_df.append({
             "site": c,
             "fb":  RS[0],
             "twit" : RS[1],
             "yt_user": RS[2],
             "yt_url":RS[3],
             "sc":RS[4],
             "insta":RS[5],
             "dm":RS[6]
              }, ignore_index=True)
print(firstPageRS_df.columns)
firstPageRS_df['site_name']=firstPageRS_df['site']

for site in firstPageRS_df['site']:
        firstPageRS_df.loc[firstPageRS_df["site"] == site, ["site_name"]] = myUrlParse(site)
firstPageRS_df[firstPageRS_df['yt_url']!=""][0:25]

firstPageRS_df['id']=""

crawled_df = pd.read_csv("C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\202007WebsitesRS_D0\\202007WebsiteRSD0.csv", sep=",")
crawled_df.head()
i=0
for site in firstPageRS_df['site']:
    #print(str(crawled_df[crawled_df["START PAGES"]== str(site)]['NAME']))
    try:
        firstPageRS_df.loc[firstPageRS_df["site"] == site, ["site_name"]] = crawled_df[crawled_df["START PAGES"]== str(site)]['NAME'].tolist()[0]
        firstPageRS_df.loc[firstPageRS_df["site"] == site, ["id"]] = crawled_df[crawled_df["START PAGES"]== str(site)]['ID'].tolist()[0]
    except:
        print("error with ", site)
firstPageRS_df





# %% Sandbox
'''
gexfRS_df = pd.DataFrame(columns=["site", "fb", "twit", "yt_user","yt_url","sc","insta","dm","Li"])
with open(medialistD0path, "r", encoding='utf-8') as file:
    content = file.readlines()
    content = [x.strip() for x in content]
    gexfRS_df['site'] =content


gexfRS_df['site']
gexfRS_df.loc[gexfRS_df["site"] == "test", ["twit"]] = "test2"
gexfRS_df["site"] = "test"
gexfRS_df

#regarde un link
for link in data['links']:
    for node in data['nodes']:
        if node['id'] == link['target']:
            #si le node qui est en target contient "youtube"
            if "youtube" in node['label'].lower():
                for node2 in data['nodes']:
                    if node2['id'] == link['source']: #va chercher la source
                        print(node2['label'], node['label'])
            if "twitter" in node['label'].lower():
                for node2 in data['nodes']:
                    if node2['id'] == link['source']:
                        print(node2['label'], node['label'])
            if "facebook" in node['label'].lower():
                for node2 in data['nodes']:
                    if node2['id'] == link['source']:
                        print(node2['label'], node['label'])'''
RSlist_gexfSiteName = ["facebook.com","fr-fr.facebook.com","fr-ca.facebook.com",
    "th-th.facebook.com", "es-la.facebook.com", "de-de.facebook.com",
    "developers.facebook.com", "business.facebook.com","m.facebook.com", "web.facebook.com",
    "twitter.com", "mobile.twitter.com","fr.twitter.com",
    "plus.google.com",
    "pinterest.com", "fr.pinterest.com", "uk.pinterest.com", "pl.pinterest.com",
    "linkedin.com", "fr.linkedin.com","ca.linkedin.com","tg.linkedin.com", "be.linkedin.com",
    "nl.linkedin.com", "ch.linkedin.com", "uk.linkedin.com","it.linkedin.com",
    "youtube.com"]
#gexfRS_df = pd.DataFrame(columns=["site", "fb", "twit", "yt_user","yt_url","sc","insta","dm","Li"])

# %% This links adds hyphe RS in firstPageRS_df (from firstpage)
#
firstPageRS_df["twit_hyphe"]=""
firstPageRS_df["fb_hyphe"]=""
firstPageRS_df["pi_hyphe"]=""
firstPageRS_df["li_hyphe"]=""
for link in data['links']:
    for node in data['nodes']:
        if node['id'] == link['target']:
            i=i+1
            site_name = node['label'].lower().split(" /")[0]

            if site_name in RSlist_gexfSiteName:
                # twitter node
                if "twitter" in node['label'].lower():
                    if len(node['label'].lower().split(" /")) < 2:
                        pass
                    else:
                        user_name = node['label'].lower().split(" /")[1]
                        user_name = user_name.replace("%20%e2%80%8f","").replace("%40%20%40","").replace("%40suivre%20sur%20twitter:%20%40","")
                        user_name = user_name.replace("%20","").replace("%40","").replace("%e2%81%a9","")
                        user_name = user_name.replace("%0apour","").replace("%0apou","").replace("%0apo","")
                        user_name = user_name.replace("%e2%80%a6","").replace("%e2%80%8e","").replace("%29","")
                        user_name = user_name.replace("%21","").replace("%c3%a9","e").replace("%c3%a1","a")
                        if "%" in user_name:
                            pass
                        else:
                            for node2 in data['nodes']:
                                if node2['id'] == link['source']:
                                    #print(node2['id'], user_name)
                                    firstPageRS_df.loc[firstPageRS_df["id"] == int(node2['id']), ["twit_hyphe"]] = user_name
                # facebook node
                elif "facebook" in node['label'].lower():
                    if len(node['label'].lower().split(" /")) < 2 or ".php" in node['label'].lower():
                        pass
                    else:
                        user_name = node['label'].lower().split(" /")[1]
                        if ".php" in user_name or user_name=="name" or user_name=="pages"or user_name=="groups" or user_name=="sharer":
                            pass
                        else:
                            for node2 in data['nodes']:
                                if node2['id'] == link['source']:
                                    #print(node2['id'], user_name)
                                    firstPageRS_df.loc[firstPageRS_df["id"] == int(node2['id']), ["fb_hyphe"]] = user_name
                # pinterest node
                elif "pinterest" in node['label'].lower():
                    if len(node['label'].lower().split(" /")) < 2:
                        pass
                    else:
                        user_name = node['label'].lower().split(" /")[1]
                        if user_name=="pin":
                            pass
                        else:
                            for node2 in data['nodes']:
                                if node2['id'] == link['source']:
                                    #print(node2['id'], user_name)
                                    firstPageRS_df.loc[firstPageRS_df["id"] == int(node2['id']), ["pi_hyphe"]] = user_name
                # linkedin node
                elif "linkedin" in node['label'].lower():
                    if len(node['label'].lower().split(" /")) < 2:
                        pass
                    else:
                        user_name = node['label'].lower().split(" /")[1]
                        user_name = user_name.replace("%c3%a9", "e").replace("%26", "et").replace("%27","").replace("%c3%a7", "c").replace("%c3%bb","u")
                        for node2 in data['nodes']:
                            if node2['id'] == link['source']:
                                #print(node2['id'], user_name)
                                firstPageRS_df.loc[firstPageRS_df["id"] == int(node2['id']), ["li_hyphe"]] = user_name

firstPageRS_df[(firstPageRS_df.fb != firstPageRS_df.fb_hyphe)][['site',  'fb' ,'fb_hyphe']][0:50]

firstPageRS_df[(firstPageRS_df.twit != firstPageRS_df.twit_hyphe)][['site',  'twit' ,'twit_hyphe']]

firstPageRS_df[(firstPageRS_df.fb_hyphe == None)][['site',  'fb' ,'fb_hyphe']][0:50] #Yeah !!!

firstPageRS_df.to_csv("C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\202007WebsitesRS_D0\\firstPageRS_df.csv", sep='\t', encoding='utf-8')
# export to csv for MANUAL CONTROL

# %% md
check firstPageRS_df.csv and create firstPageRS_df_edit.csv after QC/edit

# %% A load new df_edit for Import in Neo4j
firstPageRS_df_edit = pd.read_csv("C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\202007WebsitesRS_D0\\firstPageRS_df_edit.csv", sep=",")
firstPageRS_df_edit.head()


# %% A This imports RS (nodes and links) from file in DB
RS_df = firstPageRS_df_edit[['site','site_name','fb_final', 'twit_final', 'pi_hyphe', 'li_hyphe', 'yt_url', 'yt_user' ]]

for index, row in RS_df.iterrows():
    webmatch = graph.nodes.match('Website',site_name =row['site_name'].lower() ).first()
    if webmatch == None:
        results = db.run("MATCH (w:Website) WHERE $site_name IN w.D0_label RETURN w ",{"site_name":row['site_name'] })
        trucmoche = 0
        for r in results:
            trucmoche = trucmoche + 1
            sn = r['w']['site_name']
        webmatch = graph.nodes.match('Website',site_name =sn.lower() ).first()
        if trucmoche != 1:
            print("no node for Website:", row['site_name'], "not importing RS nodes" )
            continue
    #print(nodematch['site_name'])
    # is there an entity ?
    rel = graph.match(r_type="OWNED_BY", nodes=(webmatch, None))
    if len(rel)==0:
        entnode = Node('Entity', entity_name = webmatch['site_name'])
        entnode.__primarylabel__ = 'Entity'
        entnode.__primarykey__ = 'entity_name'
        OWNED_BY = Relationship.type("OWNED_BY")
        graph.merge(OWNED_BY(webmatch,entnode) )
        rel = graph.match(r_type="OWNED_BY", nodes=(webmatch, None))
    if len(rel)>1:
        print('several owners for this website : ',webmatch['site_name'])
        # TODO : Here are nodes with several owners ! What to do with that ?
    for r in rel:
        entitymatch=r.nodes[1]
    #twitter:
    if not pd.isnull(row['twit_final']):
        twitmatch = graph.nodes.match('Twitter', user_name = row['twit_final']).first()
        if twitmatch == None:
            twitmatch = Node('Twitter', user_name = row['twit_final'])
            twitmatch.__primarylabel__ = 'Twitter'
            twitmatch.__primarykey__ = 'user_name'
        OWNED_BY = Relationship.type("OWNED_BY")
        graph.merge(OWNED_BY(twitmatch,entitymatch) )
    # facebook:
    if not pd.isnull(row['fb_final']):
        fbmatch = graph.nodes.match('Facebook', user_name = row['fb_final']).first()
        if fbmatch == None:
            fbmatch = Node('Facebook', user_name = row['fb_final'])
            fbmatch.__primarylabel__ = 'Facebook'
            fbmatch.__primarykey__ = 'user_name'
        OWNED_BY = Relationship.type("OWNED_BY")
        graph.merge(OWNED_BY(fbmatch,entitymatch))
    # Pinterest
    if not pd.isnull(row['pi_hyphe']):
        pimatch = graph.nodes.match('Pinterest', user_name = row['pi_hyphe']).first()
        if pimatch == None:
            pimatch = Node('Pinterest', user_name = row['pi_hyphe'])
            pimatch.__primarylabel__ = 'Pinterest'
            pimatch.__primarykey__ = 'user_name'
        OWNED_BY = Relationship.type("OWNED_BY")
        graph.merge(OWNED_BY(pimatch,entitymatch))
    # linkedin
    if not pd.isnull(row['li_hyphe']):
        limatch = graph.nodes.match('Linkedin', user_name = row['li_hyphe']).first()
        if limatch == None:
            limatch = Node('Linkedin', user_name = row['li_hyphe'])
            limatch.__primarylabel__ = 'Linkedin'
            limatch.__primarykey__ = 'user_name'
        OWNED_BY = Relationship.type("OWNED_BY")
        graph.merge(OWNED_BY(limatch,entitymatch))
    # Youtube
    if not pd.isnull(row['yt_user']):
        ytmatch = graph.nodes.match('Youtube', user_name = row['yt_user']).first()
        if ytmatch == None:
            ytmatch = Node('Youtube', user_name = row['yt_user'], url=row['yt_url'])
            ytmatch.__primarylabel__ = 'Youtube'
            ytmatch.__primarykey__ = 'user_name'
        OWNED_BY = Relationship.type("OWNED_BY")
        graph.merge(OWNED_BY(ytmatch,entitymatch))

# manual stuff :
# %% md
Looks like it imported everything OK !!

# %% sandobox, test and checcks
for i in range(1,10):
    if i==3:
        continue
    print(i)


row
RS_df[['site', 'site_name', 'yt_url', 'yt_user']]

graph.nodes.match('Website',site_name ="lemonde.fr".lower() ).first()
twitmatch = graph.nodes.match('Twitter', user_name = row['twit_final']).first()
print(twitmatch)
graph.match(r_type="OWNED_BY", nodes=(twitmatch, None)).first()



for
webmatch['site_name']
len(rel)
# creer entity + link OWNED_BY
entnode = Node('Entity', entity_name = webmatch['site_name'])
entnode.__primarylabel__ = 'Entity'
entnode.__primarykey__ = 'entity_name'
OWNED_BY = Relationship.type("OWNED_BY")
graph.merge(OWNED_BY(webmatch,entnode) )


graph.nodes.match('Entity',entity_name= webmatch['site_name']).first()
graph.match(r_type="OWNED_BY", nodes=(webmatch, None)).first()
for r in graph.match(r_type="OWNED_BY", nodes=(None, entitymatch)):
    print(r.start_node['user_name'], r.start_node.labels)




relRS = graph.match(r_type="LINKS_TO", nodes=(nodematch, None))
for r in relRS:
    print(r)
nodematch.has_label("Website")
for r in graph.match((nodematch,)):
    if r.end_node.has_label("Facebook"):
        print(r.end_node['user_name'], r.end_node.labels)
RS_df




nodematch = graph.nodes.match('Website',site_name ="lemonde.fr".lower() ).first()
nodematch
for r in graph.match(r_type="OWNED_BY", nodes=(nodematch, None)):
    print(r)
    print(r.nodes[1]['entity_name'])
    print(r)
s
results = db.run("MATCH (w:Website) WHERE \""+s+"\" IN w.D0_label RETURN w ")
for r in results:
    print(r['w']['site_name'])

for node in graph.nodes.match('Website'):
    if "chouard" in node['site_name']:
        print(node)

graph.nodes.match('Website',site_name = "Institutsapiens.fr" ).first()






# %% test


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


%% codecell
# This is a test
print('hello')
