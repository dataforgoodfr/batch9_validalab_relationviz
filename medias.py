#!/usr/bin/env python
import os
from json import dumps
from flask import Flask, g, Response, request

from neo4j import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path='/static/')

password = os.getenv("NEO4J_PASSWORD")
password = "Password"
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

def serialize_linkers(record):
    return {
        'site_name': record['name'],
        'type': record['type'],
        'cite': record['cite'],
        'estcite': record['estcite']
    }

def serialize_search(record):
    return{
        'site_name': record['name'],
        'type': record['type'],
        'entity': record['e.name']
    }

@app.route("/graph")
def get_graph():
    db = get_db()

    # debug
    # db = driver.session()

    sizeProp = 'PageRank'
    colorProp = 'LouvainCommunity'
    reltype = 'LINKS_TO_tot'

    results = db.run(
        "MATCH path=(w1:Website {WCC_bij:1})-[:LINKS_TO_tot_bij]-(w2:Website)"
        #"WHERE EXISTS ((w1)-[:LINKS_TO_tot_bij*1..2]-(:Website {site_name:\"lemonde.fr\"}))"
        #"MATCH loop = (w1:Website)-[:"+reltype+"]->(w2:Website)-[:"+reltype+"]->(w1) "
        #"WHERE w1.StronglyConnectedComponents < 5 and w2.louvain < 5 "
        "RETURN w1.site_name as W1, w1.PageRank as w1pr, w1.decodex as w1c, "
        "w2.site_name as W2, w2.PageRank as w2pr, w2.decodex as w2c"
         ,{"sizeProp": sizeProp}
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
        rel = {'source': w1_id, 'target': w2_id, 'count': 0}
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
            "WHERE w.name =~ $site_name "
            "OPTIONAL MATCH (w)-[:OWNED_BY]->(e:Entity) "
            "OPTIONAL MATCH (e)<-[:OWNED_BY]-(n) "
            "WITH e, n, reduce(sum = 0, x IN collect(e.name)|sum + coalesce(n.PageRanktot,0)) AS sum "
            "RETURN n.name as name,LABELS(n)[0] as type, n.PageRanktot, e.name,sum order by e.name, sum desc",
            {"site_name": "(?i).*" + q + ".*"})

        #results = db.run("MATCH (w) "
        #         "WHERE w.site_name =~ $site_name "
        #         "RETURN w LIMIT 10", {"site_name": "(?i).*" + q + ".*"}
        #)
        return Response(dumps([serialize_search(record) for record in results]),
            mimetype="application/json")
        #return Response(dumps([serialize_media(record[0]) for record in results]),
        #                mimetype="application/json")


@app.route("/media/<site_name>")
def get_media(site_name):
    db = get_db()
    results = db.run("MATCH (w {name:$site_name})<-[r:LINKS_TO_tot]-(w2) "
            "OPTIONAL MATCH (w2)<-[r2:LINKS_TO_tot]-(w) "
            "RETURN w2.name as name, LABELS(w2)[0] as type, sum(r.count) as cite, sum(r2.count) as estcite ORDER BY cite DESC LIMIT 10", {"site_name": site_name})

    return Response(dumps([serialize_linkers(record) for record in results]),
                        mimetype="application/json")


if __name__ == '__main__':
    app.run(port=8080)


# %% sandbox
site_name = "lemonde.fr"



q
results = db.run("MATCH (w) "
         "WHERE w.site_name =~ $site_name "
         "RETURN w LIMIT 10", {"site_name": "(?i).*" + q + ".*"}
)
dumps([serialize_media(record[0]) for record in results])


results = db.run("MATCH (w) "
    "WHERE w.name =~ $site_name "
    "OPTIONAL MATCH (w)-[:OWNED_BY]->(e:Entity) "
    "OPTIONAL MATCH (e)<-[:OWNED_BY]-(n) "
    "WITH e, n, reduce(sum = 0, x IN collect(e.name)|sum + coalesce(n.PageRanktot,0)) AS sum "
    "RETURN n.name as name,LABELS(n)[0] as type, n.PageRanktot, e.name,sum order by e.name, sum desc",
    {"site_name": "(?i).*" + q + ".*"})
dumps([serialize_search(record) for record in results])
