#!/usr/bin/env python
import os
from json import dumps
from flask import Flask, g, Response, request

from neo4j import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path='/static/')

password = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", password))

def get_db():
    if not hasattr(g, 'neo4j_db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()

@app.route("/")
def get_index():
    return app.send_static_file('index_medias.html')

def serialize_media(media):
    return {
        'site_name': media['site_name']
        #'alex_rank': media['alex_rank'],
        #'label': media['label'],
        #'title': media['title'],
        #'categorie': "site"
        }

def serialize_linkers(linker):
    return {
        'count': linker['count'],
        'site_name': linker['linker']['site_name'],
        'alex_rank': linker['linker']['alex_rank'],
        'type': "site",
        'description': linker['linker']['title']
    }

@app.route("/graph")
def get_graph():
    db = get_db()

    sizeProp = 'PageRank'
    colorProp = 'LouvainCommunity'

    results = db.run(
        "MATCH loop = (w1:Website)-[r1:LINKS_TO]->(w2:Website)-[r2:LINKS_TO]->(w1)"
        "WHERE w1.StronglyConnectedComponents < 5 and w2.louvain < 5, "
        "RETURN w1.site_name as W1, w1.PageRank as w1pr, w1.StronglyConnectedComponents as w1c, "
        "w2.site_name as W2, w2.PageRank as w2pr, w2.StronglyConnectedComponents as w2c,"
        "r1.count_D2 + r2.count_D2 as D2", {"sizeProp": sizeProp}
        )

    nodes=[]
    rels=[]
    for record in results:
        # Replaced this 2 lines by the others
        w1 = {"name": record["W1"], "label": "website", "size": record["w1pr"]*10, "Community": record["w1c"] }
        w2 = {"name": record["W2"], "label": "website", "size": record["w2pr"]*10, "Community": record["w2c"]}

        #w1 = {"name": record["W1"], "label": "website", "size": 5, "Community": 1}
        #w2 = {"name": record["W2"], "label": "website", "size": 5, "Community": 1}

        try:
            w1_id = nodes.index(w1)
        except:
            nodes.append(w1)
            w1_id = nodes.index(w1)

        try:
            w2_id = nodes.index(w2)
        except:
            nodes.append(w2)
            w2_id = nodes.index(w2)

        #count = record["W1W2count"] +record["W2W1count"]
        rel = {'source': w1_id, 'target': w2_id, 'count': record["D2"]}
        if rel not in rels:
            rels.append(rel)

    for node in nodes:
        node['id']=nodes.index(node)
        print(node)

    return Response(dumps({"nodes": nodes, "links": rels}),
                    mimetype="application/json")


@app.route("/search")
def get_search():
    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.run("MATCH (w) "
                 "WHERE w.site_name =~ $site_name "
                 "RETURN w", {"site_name": "(?i).*" + q + ".*"}
        )
        return Response(dumps([serialize_media(record[0]) for record in results]),
                        mimetype="application/json")


@app.route("/media/<site_name>")
def get_media(site_name):
    db = get_db()
    results = db.run("MATCH (w {site_name:$site_name})<-[r:LINKS_TO]-(w2) "
             "RETURN w2 as linker, (coalesce(r.count_D2,0) + coalesce(r.count_D1,0) + coalesce(r.count_D0,0)) as count ORDER BY count DESC LIMIT 5", {"site_name": site_name})

    return Response(dumps([serialize_linkers(record) for record in results]),
                        mimetype="application/json")


if __name__ == '__main__':
    app.run(port=8080)
