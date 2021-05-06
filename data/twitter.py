# %% Init
import pandas as pd
from TwitterAPI import TwitterAPI, TwitterPager
from pathlib import Path
import datetime
from py2neo import Graph, Node, Relationship
from neo4j import GraphDatabase, basic_auth

graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

api_key = "a valid API Key"
api_secret_key = "a valid secred"

path = "C:\\Users\\Jo\\Documents\\Tech\\ScrapData\\20200709"  # <<<  hardcoded

def create_api():
    config = dict(
        twitter=dict(
            api_key=api_key,
            api_secret_key=api_secret_key
        )
    )
    api = TwitterAPI(config['twitter']['api_key'],
                     config['twitter']['api_secret_key'],
                     auth_type='oAuth2'
                     )
    return (api)

api=create_api()

def respectful_api_request(*args):
    '''Respects api limits and retries after waiting.'''
    r = api.request(*args)
    if r.headers['x-rate-limit-remaining'] == '0':
        waiting_time = int(
            r.headers['x-rate-limit-reset']) - int(round(time.time()))
        click.echo(
            'Hit the API limit. Waiting for refresh at {}.'
            .format(datetime.datetime.utcfromtimestamp(int(r.headers['x-rate-limit-reset']))
                    .strftime('%Y-%m-%dT%H:%M:%SZ')))
        time.sleep(waiting_time)
        return (respectful_api_request(*args))
    return(r)

def write_twitpro_file(screen_name):
    '''
    Returns twitter profile file path from twitter user name
    if no file, download it using twitter API
    '''
    screen_name = screen_name.lower()
    # If no file, download :
    cursor = -1
    twitpro_file = path + "/twitter/" + screen_name + '.twitprofile'

    if not Path(twitpro_file).is_file():
        try:
            r = respectful_api_request('users/show', {'screen_name': screen_name, 'cursor': cursor})
            for items in r:
                with open(twitpro_file, "w", encoding='utf-8') as file:
                    file.write(str(items))
            return(twitpro_file, 1)

        except:
            error = str(datetime.datetime.now()) + " Error 18 : " + screen_name + " is not a twitter user"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return("",0)
    else:
        return(twitpro_file,0)

def push_twitpro_in_Neo4j(screen_name):
    '''
    From twitter screen name, download twitter profile info and push into Neo4j.
    Save intermediary twitpro file if it does not exists
    '''
    screen_name = screen_name.lower()
    keys = ['id','name','screen_name','location','description','desc_url','url','followers_count',
                  'friends_count', 'listed_count', 'created_at', 'verified', 'statuses_count', 'profile_image_url']

    (twitpro_file,i) = write_twitpro_file(screen_name)

    if twitpro_file != "":
    # if file exists, reads it and parse into twit_profile_extracted_keys
        with open(twitpro_file, encoding="utf8") as f:
            twit_profile = eval(f.read())
            twit_profile_extracted_keys = {k: twit_profile.get(k) for k in keys}
            if twit_profile.get('entities').get('url')==None:
                twit_profile_extracted_keys['url']= None
            else:
                twit_profile_extracted_keys['url']= twit_profile.get('entities')['url']['urls'][0]['expanded_url']

       # Finds twitter node
        twitter_n = graph.nodes.match("Twitter", user_name = screen_name).first()
        # update properties
        for k in keys:
            twitter_n[k] = twit_profile_extracted_keys[k]
        # push in DB
        tx = graph.begin()
        tx.push(twitter_n)
        tx.commit()
        return(twitter_n)
    else:
        return("")


# %% Download twitter profiles :
results = db.run("match(t:Twitter)-[:OWNED_BY]->(e:Entity) return t.user_name")
counter = 0
for r in results:
    (file,i)=write_twitpro_file(r['t.user_name'])
    counter = counter+i
    print(counter, r['t.user_name'])
    if counter > 250:
        break
# %% Imports all Twitter profiles files in DB

results = db.run("match(t:Twitter)-[:OWNED_BY]->(e:Entity) return t.user_name")
for r in results:
    try:
        print(r['t.user_name'],"imported as", push_twitpro_in_Neo4j(r['t.user_name'])['user_name'])
    except:
        print("ERROR :",r['t.user_name'])

# %% md
Code for Download and import looks good
Done !

# %% init

def import_twecoll_dat(dat_name, twecoll_path):
    ''' from twecoll dat files and corresponding fdat folder, creates or updates
    twitter nodes and FOLLOWS link. Don't import unknown .f files
    '''
    dat_file = twecoll_path + dat_name
    columns = ["twitid","name","scraptype", "friends_count", "followers_count", "nsp", "tweets","date_creation","tw_url","img","loc"]
    twecoll_df = pd.read_csv(dat_file, sep=",",names=columns)

    # Update or create twitter nodes with IDs from file
    rowlen = len(twecoll_df)
    print("part1: check and update for",rowlen,"nodes")
    for index, row in twecoll_df.iterrows():
        #print(row['username'].lower())
        if index%10 == 0:
            print(".", end=" ")
        if index%100 ==0:
            print(index ,"/",rowlen)
        try:
            twitmatch = graph.nodes.match("Twitter", user_name = row['name'].lower()).first()
            if twitmatch['id'] == row['twitid']:
                pass
            else:
                if twitmatch['id'] == None:
                    twitmatch['id'] = row['twitid']
                    graph.push(twitmatch)
                else:
                    print("Error : 2 different twitter IDs for ",row['username'])

        except:
            print("no twitter nodefound for",row['name'],". Creating it")
            twitmatch = Node('Twitter', user_name = row['name'].lower())
            twitmatch.__primarylabel__ = 'Twitter'
            twitmatch.__primarykey__ = 'user_name'
            twitmatch['id'] = row['twitid']
            twitmatch['name'] = row['name'].lower()
            for k in ['friends_count','followers_count','date_creation','loc']:
                twitmatch[k] = row[k]
            twitmatch
            graph.merge(twitmatch)
            #print("OK")

    print("part2: create FOLLOWS relationships")
    # Create links if twitter node exists in base
    for index, row in twecoll_df.iterrows():
        if index%10 == 0:
            print(".", end=" ")
        if index%100 ==0:
            print(index ,"/",rowlen)
        file_id =  twecoll_path + "fdat\\" + str(row['twitid']) + ".f"
        try:
            with open(file_id) as f:
                content = f.readlines()
                content = [x.strip() for x in content] # remove whitespace characters like `\n` at the end of each line
                source_n = graph.nodes.match("Twitter", user_name = row['name'].lower()).first()
                for c in content:
                    if c == str(row['twitid']):
                        continue
                    try:
                        target_n = graph.nodes.match("Twitter", id = int(c)).first()
                        if target_n != None:
                            #print(source_n['user_name'], target_n['user_name'])
                            FOLLOWS = Relationship.type("FOLLOWS")
                            graph.merge(FOLLOWS(source_n, target_n))
                        #print("creating FOLLOWS link between", source_n['user_name'], target_n['user_name'])
                    except:
                        pass
        except:
            print("no file for ", row['name'])

# %% Import dat files

twecoll_path = "C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\twecoll\\20200723Twecoll\\"
dat_name = "IN.dat"

dat_name = "owned_usernames.dat"

import_twecoll_dat(dat_name, twecoll_path)

import_twecoll_dat("skipped.dat", twecoll_path)

# This should be done but takes a lot of time ...
import_twecoll_dat("owned_usernames.dat", twecoll_path)


'''
BUG SOLVED : creates "None" relationship on Twitter nodes (x:Twitter)-[r:None]-
'''
# Do that for clean up
print("count None relationships:", [r['count(r)'] for r in db.run("MATCH ()-[r:None]->() RETURN count(r)")])
print("count None relationships on 2 different twitter nodes:",[r['count(r)'] for r in db.run("MATCH (t1:Twitter)-[r:None]->(t2:Twitter) WHERE t1<>t2 RETURN count(r)")])
db.run("match ()-[r:None]->() DElETE r")



''' BUG SOLVED
part2: create FOLLOWS relationships
. 0 / 706
. . . . . . . . . . 100 / 706
. . . . . . . . . . 200 / 706
. . . . . . . . . . 300 / 706
. . . . . . . . . . 400 / 706
. . . . . . . . . . 500 / 706
. . . . . . . . . . 600 / 706
. . . ---------------------------------------------------------------------------
FileNotFoundError                         Traceback (most recent call last)
<ipython-input-7-3415bf37fcee> in <module>
----> 1 import_twecoll_dat("owned_usernames.dat", twecoll_path)

<ipython-input-4-7499bcb0f2a8> in import_twecoll_dat(dat_name, twecoll_path)
     48             print(index ,"/",rowlen)
     49         file_id =  twecoll_path + "\\fdat\\" + str(row['twitid']) + ".f"
---> 50         with open(file_id) as f:
     51             content = f.readlines()
     52             content = [x.strip() for x in content]

FileNotFoundError: [Errno 2] No such file or directory: 'C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data\\twecoll\\20200723Twecoll\\\\fdat\\358053394.f''''
