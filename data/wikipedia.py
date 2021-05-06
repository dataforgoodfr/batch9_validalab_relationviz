# %% init
from py2neo import Graph, Node, Relationship
import pandas as pd
from neo4j import GraphDatabase, basic_auth
import wikipedia
import wptools
import os

graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

wikipedia.set_lang("fr")

# Wikipedia API info : https://stackabuse.com/getting-started-with-pythons-wikipedia-api/
# %% md
'''
TODO sur wikipedia:
- pour chaque website / Entity (twitter ?):
    - [X] trouver si page wiki correspond
    - scrap page (summary, infobox, categories)

Puis scrapper l'arbre de categorie
'''

# %% search for wikipedia pages corresponding to entity nodes. Create wiki_search.csv for manual edit / QC
wikipath = "C:\\Users\\Jo\\Documents\\Tech\\ScrapData\\20200709\\wiki\\"

results = db.run("MATCH (e:Entity) RETURN e.entity_name ")

for r in results:
    if r['e.entity_name'] in wiki_df["entity_name"].values:
        continue
    else :
        with open(wikipath + "wiki_search.csv", "a", encoding='utf-8') as f:
            try:
                f.write(r['e.entity_name']+";"+wikipedia.search(r['e.entity_name'], results=1)[0]+'\n')
            except:
                print("No wikisearch for:", r['e.entity_name'])

wiki_df = pd.read_csv(wikipath + "wiki_search.csv", encoding='utf-8', sep =";", names=["entity_name", "wikisearch"])
# %% md
Here, open and manual check of wiki_search.csv in libreoffice.
Create a third columns with is:
- nothing if wikisearch is OK
- None if no reference in wikipedia
- NEW_NAME if better wiki page for the entity
Save new file as wiki_search_edit.csv
# %% import edited wiki pages as wiki nodes and OWNED_BY connections
wikiedit_df = pd.read_csv(wikipath + "wiki_search_edit.csv", encoding='utf-8', sep =";", names=["entity_name", "wikisearch", "manual"])

for index, row in wikiedit_df.iterrows():
    if pd.isnull(row["manual"]):
        page_name = row["wikisearch"]
    else:
        if row["manual"] == "None":
            continue
        else:
            page_name = row["manual"]
    entity_n = graph.nodes.match("Entity", entity_name = row["entity_name"]).first()
    wiki_n = Node("Wikipedia", page_name = page_name )
    wiki_n.__primarylabel__ = 'Wikipedia'
    wiki_n.__primarykey__ = 'page_name'
    wiki_n["url"]="https://fr.wikipedia.org/wiki/"+page_name
    OWNED_BY = Relationship.type("OWNED_BY")
    graph.merge(OWNED_BY(wiki_n, entity_n))
    print(entity_n["entity_name"], wiki_n["url"])

# %% Download  wiki pages (with infobox)as files from wiki nodes

results = graph.nodes.match("Wikipedia")
for wiki_n in results:
    # regarde si fichier existe. Si non,
    if os.path.isfile(wikipath + wiki_n['page_name']+".wikipage"):
        print(wiki_n['page_name']+".wikipage already fetched.")
        continue
    try:
        page = wptools.page(wiki_n['page_name'], lang ='fr')
        page.get_parse()
    except:
        print("error 1 with name:",wiki_n['page_name'])
        continue
    try:
        with open(wikipath + wiki_n['page_name']+".wikipage" , "w", encoding='utf-8') as file:
            file.write(str(page.data))
    except:
        print("error 2 with name:",wiki_n['page_name'])

# manual doanload for "Usbek_&_Rica_(magazine)", problem with "&" in name
usbek_name = "Usbek_&_Rica_(magazine)"
if not os.path.isfile(wikipath + usbek_name + ".wikipage"):
    page = wptools.page(usbek_name, lang ='fr')
    page.get_parse()
    with open(wikipath + usbek_name+".wikipage" , "w", encoding='utf-8') as file:
        file.write(str(page.data))
        if os.path.isfile(wikipath + usbek_name+".wikipage"):
            pass

# %% Imports from wikipedia files : infobox, categories and wikidata ref.
# Also creates 2 files with all infobox keys and all categories
all_keys =[]
all_cat =[]
results = graph.nodes.match("Wikipedia")
for wiki_n in results:
    print(wiki_n['page_name'])
    try:
        with open(wikipath + wiki_n['page_name']+".wikipage" , encoding='utf-8') as file:
            page = eval(file.read())
    except:
        print("problem with file name")
        continue
    try:
        for key in page['infobox'].keys():
            if key not in all_keys:
                all_keys.append(key)
            wiki_n[key] = page['infobox'][key]
            print(key, ":",page['infobox'][key])
    except:
        print("no infobox")

    indiv_cat = []
    for i in range(1,len(page['parsetree'].split("[[Catégorie:"))):
        cat = page['parsetree'].split("[[Catégorie:")[i].split("]]")[0].split("|*")[0]
        indiv_cat.append(cat)
        if cat not in all_cat:
            all_cat.append(cat)
    wiki_n["categories"] = indiv_cat
    print(indiv_cat)
    wiki_n['wikibase'] = page['wikibase']
    print(page['wikibase'])
    try:
        graph.push(wiki_n)
    except:
        print("cannot push node :", wiki_n)
    print("**************************************************************************")

len(all_keys)
len(all_cat)

with open(wikipath + "all_infobox_keys.txt" , "w", encoding='utf-8') as file:
    file.write(str(all_keys))
with open(wikipath + "all_categories.txt" , "w", encoding='utf-8') as file:
    file.write(str(all_cat))

# %% import wikipedia Summary:

results = graph.nodes.match("Wikipedia")
for wiki_n in results:
    print(wiki_n['page_name'])
    try:
        wiki_n['summary'] = wikipedia.summary(wiki_n['page_name'], sentences =5)
        graph.push(wiki_n)
    except:
        print("error with", wiki_n['page_name'])

# %% test

    try:
        for key in page.data['infobox'].keys():
            wiki_n[key] = page.data['infobox'][key]
    except:
        pass
    try:
        wiki_n['wikidata_url'] = page.data['wikidata_url']
    except:
        pass
    graph.push(wiki_n)
    print(wiki_n['page_name'])
results = graph.nodes.match("Wikipedia").limit(3)
for r in results:
    print(r)



n = graph.nodes.match("Wikipedia").first()
n
n['test'] = "[test]"
graph.push(n)
del n['test']

# %% Wikipedia module tuto

import wikipedia

print(wikipedia.suggest("lemonde.fr"))
# set wikipedia language
wikipedia.set_lang("fr")

# get search results
print(wikipedia.search("bienpublic.com", results=5))
print(wikipedia.search("lemonde.fr", results=1))
#from page, get summary
print(wikipedia.summary('Le Bien public', sentences =3))
print(wikipedia.summary('Fakir (journal)', sentences =5))

# Page :
page = wikipedia.page('Fakir (journal)')

# page title
page.title

## All page content
print(wikipedia.page('Fakir (journal)').content)

# page url
wikipedia.page('Fakir (journal)').url

# page external contents
wikipedia.page('Fakir (journal)').references

wikipedia.page('Fakir (journal)').categories


wikipedia.page('Fakir (journal)').links


wikipedia.page('Fakir (journal)').images


# %% Tuto wptools

import wptools
page = wptools.page('Fakir_(journal)', lang ='fr')

page.get_query()

print(page.data['extext'])
page.data['image']

page.images(['kind', 'url'])

page = wptools.page('Fakir_(journal)', lang ='fr')
page.get_parse()

page.data['infobox']

print(page.data['wikidata_url'])
print(page.data['wikitext'])


cat = wptools.category('Catégorie:Presse_écrite_en_France_par_genre',lang ='fr' )


cat.get_members()

cat.data['members']
cat.data['subcategories']
