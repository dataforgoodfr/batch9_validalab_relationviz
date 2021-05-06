# %% codecell
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import networkx as nx
import json
import datetime
from random import randrange
from time import sleep
from py2neo import Graph, Node, Relationship
from urllib.parse import unquote
import urllib.request



from TwitterAPI import TwitterAPI, TwitterPager
import datetime
import click
from pathlib import Path
# %% codecell
# set path with all files
scrap_path = "C:\\Users\\Jo\\Documents\\Tech\\ScrapData"
path = (scrap_path+"\\20200114_test")
gexfPath = 'C:/Users/Jo/Nextcloud2/Documents/tech/11Medialist/2811NewListD2-IN.gexf'

twitpath = path + "\\twitter"

graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))

df_twecoll = pd.read_csv("C:/Users/Jo/Nextcloud2/Documents/tech/twecoll/20200128_Tw_MediaList.dat",header=None)
twecoll_columns =["id","screen_name","relationship","friends","followers","no_se","tweets","start_date","site","picture","location"]
df_twecoll.columns = twecoll_columns


G = nx.read_gexf(gexfPath, node_type=None, relabel=False, version='1.1draft')
data = nx.json_graph.node_link_data(G)

os.chdir(path)
clean_http = re.compile(r"https?://(www\.)?")

ytAPI_client_secrets_file_path = "C:\\Users\\Jo\\Documents\\Tech\\Notebooks\\client_secret_1054515006139-e0p34lt262qbh4af24t2po4c5ophspjo.apps.googleusercontent.com.json"
# %% codecell
NoScrapSites = ("discord.gg", "deezer.com", "snapchat.com", "facebook.com", "play.google.com", "youtube.com",
"tumblr.com", "bitly.com", "linkedin.com", "vk.com", "spotify.com", "telegram.org", "apple.co", "netvibes.com",
"google.com", "dailymotion.com", "telegram.me", "amazon.com", "plus.google.com", "mesopinions.com_fr",
"flipboard.com", "tipeee.com", "viadeo.com", "paypal.com", "soundcloud.com", "twitter.com",
"itunes.apple.com", "instagram.com", "wikipedia.org", "pinterest.com", "pearltrees.com", "ausha.co", "jemabonne.fr")
# %% codecell
#twitter API :
api_key = "2WIJPjcDeA2VSVYxVfVLnYcVS"
api_secret_key = "bgnd3RKSF3VtqVlk0NX15uhRkPmHKqzLfhQlA3OehCEttXLOeG"

# %% codecell
def myUrlParse(url):
    '''parse url and returns correct sitename'''
    from urllib import parse
    # Check liste des exceptions

    site_name = parse.urlsplit(url).netloc.replace("www.","").lower()

    # Exceptions :
    # 1/ extensions for sites in french
    if parse.urlsplit(url).path.replace("/", "") == "fr" :
        site_name = site_name + "_fr"

    # 2/ same for 20min.ch/ro
    if parse.urlsplit(url).path.replace("/", "") == "ro" :
        site_name = site_name + "_ro"

    # 3/ same for ajplus.net/francais
    if parse.urlsplit(url).path.replace("/", "") == "francais" :
        site_name = site_name + "_francais"

    # 4/ same for gijn.org/gijn-en-francais
    if parse.urlsplit(url).path.replace("/", "") == "gijn-en-francais" :
        site_name = site_name + "_francais"

    # 5/ for sites like afrique.tv5monde.com to be reduced to tv5monde.com
    shortersites = ("tv5monde.com","canardpc.com","seloger.com","facebook.com","linkedin.com","pinterest.com",
                    "wikipedia.org","tumblr.com","spotify.com","tendanceouest.com","erfm.fr","viadeo.com",
                    "gulli.fr", "humanite.fr","globalresearch.ca")
    for reduc in shortersites:
        if reduc in site_name:
            site_name = reduc

    #6/ Switcher dictionary
    switch_dict = {"amzn.to":"amazon.com", "bit.ly":"bitly.com", "landing.wuaki.tv":"rakuten.tv",
        "t.me":"telegram.org","mrmondialisation.net":"mrmondialisation.org","tv5mondeplus.com":"tv5monde.com",
                   "fipradio.fr":"fip.fr", "phantasia.be":"mrmondialisation.org", "lesoffrescanal.fr":"canalplus.fr"}
    if site_name in switch_dict:
        site_name = switch_dict[site_name]


    # myUrlParse.site_name doit retourner sitename, myUrlParse.url doit retourner l'url nettoyée
    return(site_name)

# %% codecell
def getFirstPageNode(url):
    '''From url, download or read firstpage and extract sitename, url and title'''
    site_name = myUrlParse(url)

    # exception for noScrapSites list
    if site_name in NoScrapSites:
        return(site_name, "")

    title =""
    try:
        # if firstpage file already downloaded
        page = open(site_name + "_FirstPage.html", "r", encoding='utf-8')
        soup = BeautifulSoup(page, "html.parser")
        # print(i, ": opening ", site_name,"from file")

    except:
        # if no file, try accessing url
        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.content, "html.parser")
            with open(site_name + "_FirstPage.html", "w", encoding='utf-8') as file:
                file.write(str(soup))
                #print(i, ": downloaded ", site_name, "from site and file written")

        except:
            #print("ERROR with ", site_name, ": no file and cannot access site. SKIPPING" )
            error = str(datetime.datetime.now()) + " Error 1 : " + site_name + ": no file and cannot access site. SKIPPING"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")

            # Write in dead site file
            with open("DeadSites.txt", "a", encoding='utf-8') as file:
                file.write(site_name + "; " + url +"\n")


    try:
        title = soup.find("title").get_text()
    except:
        error = str(datetime.datetime.now()) + " Error 2 : cannot parse " + site_name
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")

    return(site_name, title )

# %% codecell
def getAlexaInfoNode(url):
    '''From url or sitename, download or read Alexa page and extract alexa rank'''
    url_alexa_start="https://www.alexa.com/siteinfo/"
    site_name = myUrlParse(url)

    # exception for noScrapSites list
    if site_name in NoScrapSites:
        return("")

    rank=""

    try:
        # if alexa file already downloaded
        page = open(site_name + "_alexa.html", "r", encoding='utf-8')
        soup = BeautifulSoup(page, "html.parser")
        #print(i, ": opening ", site_name,"from file")

    except:
        # if no file, try accessing alexa
        try:
            url_alex_full = url_alexa_start + site_name
            page = requests.get(url_alex_full)
            soup = BeautifulSoup(page.content, "html.parser")

            with open(site_name + "_alexa.html", "w", encoding='utf-8') as file:
                file.write(str(soup))
            #print(i, ": downloaded ", site_name, "from site and file written")

        except:
            error = str(datetime.datetime.now()) + " Error 3 : " + site_name + ": no Alex file and cannot access Alex site. SKIPPING"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            #print("ERROR with ", site_name, ": no file and cannot access site. SKIPPING" )
            #err1.append(site_name)


        sleep_time = randrange(10)
        print("Sleeping for ", sleep_time, "seconds ...")
        sleep(sleep_time)

    try:
        # Rank
        rank = soup.find_all(class_="rankmini-rank")[0].get_text().strip().strip("#").strip(",")
        try:
            rank = int(rank.replace(',', ''))
        except:
            pass

    except:
        error = str(datetime.datetime.now()) + " Error 4 : " + site_name + " no Alexa rank in file"
        # print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        #print("ERROR parsing Alexa")

    return(rank)
# %% codecell
def getHypheInfoNode(url, data = data):
    '''From url or sitename, extract Hyphe info node from gefx file or return site_name if not in gefx'''
    site_name = myUrlParse(url)
    #G = nx.read_gexf(gexfPath, node_type=None, relabel=False, version='1.1draft')
    #data = nx.json_graph.node_link_data(G)

    result = site_name
    for n in data['nodes']:
        try:
            if myUrlParse(n['homepage']) == site_name or n['name'].lower() == site_name:
                result = n
        except:
            if n['name'].lower() == site_name:
                result = n

    if result == site_name:
        error = str(datetime.datetime.now()) + " Error 5 : " + site_name + " not in hyphe."
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        with open("NotInHyphe.txt", "a", encoding='utf-8') as file:
            file.write(site_name + "; " + url  + "\n")


    return(result)
# %% codecell
def PushUrlInfoInNeo4j(url):
    title=""
    alex_rank=""
    pages_total=""
    hyphe_date=""
    pages_crawled=""
    label=""
    HypheInfo=""
    site_name = myUrlParse(url)

    # exception for noScrapSites list
    if site_name in NoScrapSites:
        a = Node('NoScrap', site_name=site_name)
        a.__primarylabel__ = 'NoScrap'
        a.__primarykey__ = 'site_name'
        graph.merge(a)
        return(a)

    if site_name=="":
        error = str(datetime.datetime.now()) + " Error 12 : " + url +" is not a valid URL. Cannot write in db"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        return

    (site_name, title ) = getFirstPageNode(url)
    alex_rank = getAlexaInfoNode(url)
    HypheInfo = getHypheInfoNode(url)
    if HypheInfo !="":

        try:
            pages_total = HypheInfo['pages_total']
            hyphe_date = HypheInfo['last_modification_date']
            pages_crawled = HypheInfo['pages_crawled']
            label = HypheInfo['label']
        except:
            pass

    try:
        a = Node('Website', site_name=site_name, title=title,
             alex_rank = alex_rank, pages_total = pages_total, hyphe_date = hyphe_date,
             pages_crawled = pages_crawled, label = label)
        a.__primarylabel__ = 'Website'
        a.__primarykey__ = 'site_name'
        graph.merge(a)
        return(a)
    except:
        error = str(datetime.datetime.now()) + " Error 6 : Cannot write " + site_name + " in db"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")

# %% codecell
def getFacebookUsername(url):
    '''if url is facebook url, returns facebook username. Otherwise returns "" '''
    try:
        res = url.replace('https://www.facebook.com/', '').replace('https://facebook.com/', '').replace('http://www.facebook.com/', '').replace('https://fr-fr.facebook.com/', '').replace('http://fr-fr.facebook.com/','').rstrip('/')
        if res.startswith("pages"):
            m = re.search('/(.+?)/', res)
            if m:
                res = m.group(1)
        if '/' in res:
            res=""
    except:
        res=""
    return(res)
# %% codecell
def getTwitterUsername(url):
    '''if url is twitter url, returns twitter username. Otherwise returns "" '''
    try:
        res = url.replace('https://twitter.com/', '').replace('http://twitter.com/', '').replace('?lang=fr', '').replace('?ref_src=twsrc%5Etfw', '').replace('https://www.twitter.com/','').replace('#!/','').rstrip('/')
        res = res.replace(' ','').replace('@','').replace("\n","").replace("http://www.twitter.com/","")
        if '/' in res:
            res=""
        if len(res)>36:
            res=""
        if 'share?' in res:
            res=""
    except:
        res=""
    return(res)
# %% codecell
def getYouTubeNameAndurl(url):
    '''extract youtube name (user, chaine...) and start page. If link is channel or no link, returns "" '''
    yt_user=""
    yt_channel=""
    yt_user=""
    try:
        if url.startswith('https://www.youtube.com/user/') | url.startswith('http://www.youtube.com/user/'):
            yt_user = url.replace('https://www.youtube.com/user/','').replace('http://www.youtube.com/user/','').rstrip('/').lower()
        if url.startswith('https://www.youtube.com/channel/') | url.startswith('https://youtube.com/channel/'):
            yt_user = "channel"
        if '/' not in url.replace("https://www.youtube.com/c/","").replace('http://www.youtube.com/','').replace('https://www.youtube.com/',''):
            yt_user = url.replace("https://www.youtube.com/c/","").replace('http://www.youtube.com/','').replace('https://www.youtube.com/','').lower()
        if yt_user=="":
            url=""
    except:
        url=""
    return(yt_user,url)
# %% codecell
def getFirstPageRSNode(url):
    '''From url or sitename, read firstpage and extract Social Network info'''
    site_name = myUrlParse(url)
    (fb,twit,yt_user,yt_url,sc,insta,dm)=("","","","","","","")

    # exception for noScrapSites list
    if site_name in NoScrapSites:
        return(fb,twit,yt_user,yt_url,sc,insta,dm)

    try:
        # if firstpage file already downloaded
        page = open(site_name + "_FirstPage.html", "r", encoding='utf-8')
        soup = BeautifulSoup(page, "html.parser")
        # print(i, ": opening ", site_name,"from file")

    except:
        # if no file, error
        error = str(datetime.datetime.now()) + " Error 7 - " + site_name + ": no firstpage file. Please download first "
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")

    try:
        for link in soup.find_all('a'):
            link_url=str(link.get('href'))
            if "facebook" in link_url:
                fb = getFacebookUsername(link_url).lower()
            if "twitter" in link_url:
                twit = getTwitterUsername(link_url).lower()
            if "youtube" in link_url:
                (yt_user,yt_url) = getYouTubeNameAndurl(link_url)
                if yt_user == "channel":
                    yt_user = site_name
# Removed snapchat, insta and dailymotion.
# TODO : build SC, Insta & Dailymotion url parser
            '''if "snapchat" in link_url:
                sc = link_url
            if "instagram" in link_url:
                insta = link_url
            if "dailymotion" in link_url:
                dm = link_url'''
    except:
        error = str(datetime.datetime.now()) + " Error 8 : " + site_name + ": cannot parse RS"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")


    return(fb,twit,yt_user,yt_url,sc,insta,dm)
# %% codecell
def PushRSUrlInfoInNeo4j(url):
    '''From url or sitename, read firstpage, extract SN info and push nodes and relationship in neo4j'''

    (site_name, title ) = getFirstPageNode(url)
    siteNodeRes = graph.nodes.match("Website", site_name=site_name)
    sitenodenb = len(siteNodeRes)

    if sitenodenb != 1:
        error = str(datetime.datetime.now()) + " Error 9 : " + site_name + " has " + str(sitenodenb) + " nodes in DB"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
    else:
        siteNode=siteNodeRes.first()
        (fb,twit,yt_user,yt_url,sc,insta,dm)= getFirstPageRSNode(url)

        tx = graph.begin()

        if fb != "":
            fb_n = Node("Facebook", user = fb)
            tx.merge(fb_n, "Facebook", "user" )
            site_fb_r = Relationship(siteNode, "LINKS_TO", fb_n)
            tx.merge(site_fb_r)

        if twit != "":
            twit_n = Node("Twitter", user = twit)
            tx.merge(twit_n, "Twitter", "user" )
            site_twit_r = Relationship(siteNode, "LINKS_TO", twit_n)
            tx.merge(site_twit_r)

        if yt_user != "":
            yt_n = Node("Youtube", user = yt_user, url = yt_url )
            tx.merge(yt_n, "Youtube", "user" )
            site_yt_r = Relationship(siteNode, "LINKS_TO", yt_n)
            tx.merge(site_yt_r)

        tx.commit()
    return(siteNode)

# %% codecell
def PushHypheLinksToNeo4j(url):
    '''From url or sitename, extract hyphe connections and push them in Neo4j if nodes exists'''
    ''' Gives back connections to and from this site that does not exist in DB'''
    site_name = myUrlParse(url)
    source_match = ''
    sourceID = ''

    # exception for noScrapSites list
    if site_name in NoScrapSites:
        return

    # print("1 start find site hyphe ID")

    # Find site hyphe ID
    for n in data['nodes']:
        if n['name'].lower() == site_name:
            sourceID = n['id']
            source_match = graph.nodes.match("Website", site_name = site_name)
            if len(source_match) != 1:
                error = str(datetime.datetime.now()) + " Error 10 : " + site_name + " has " + str(len(source_match)) + " nodes in DB"
                print(error)
                with open("error_file.txt", "a", encoding='utf-8') as file:
                    file.write(error +"\n")

    # print("2 sourceID = ", sourceID)

    if (sourceID == ''):
        error = str(datetime.datetime.now()) + " Error 11 : " + site_name + " has no connection in Hyphe."
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")

    else:
        # print("3 start links loop")

        # Find links where ID is source
        for l in data['links']:
            if l['source']== str(sourceID):

                # print("4 : l = ", l)

                for n in data['nodes']:
                    # find target
                    if n['id']==str(l['target']):
                        target_name = n['name'].lower()
                        # print(target_name)
                        target_match = graph.nodes.match("Website", site_name = target_name)
                        # print(len(target_match))

                        if (len(target_match) != 1):
                            error = str(datetime.datetime.now()) + " Error 12 : " + target_name + " has " + str(len(target_match)) + " nodes in DB"
                            print(error)
                            with open("error_file.txt", "a", encoding='utf-8') as file:
                                file.write(error +"\n")

                        else:
                            source_n = source_match.first()
                            target_n = target_match.first()
                            # print(source_n, target_n)


                        # create connections
                        if ((len(source_match) != 1) or (len(target_match) != 1)):
                            # print("lensource = ", len(source_match))
                            # print("lentarget = ",len(target_match))
                            error = str(datetime.datetime.now()) + " Error 13 : " + l['id'] + " cannot be created in db"
                            print(error)
                            with open("error_file.txt", "a", encoding='utf-8') as file:
                                file.write(error +"\n")

                        else:
                            # print("start creating relationship in neo4j")
                            tx = graph.begin()
                            source_n = source_match.first()
                            target_n = target_match.first()
                            rel = Relationship(source_n, "LINKS_TO", target_n)
                            rel["count_D2"]=l['count']
                            tx.merge(rel)
                            tx.commit()
                            #print("Created ", source_n['label'], " - ", target_n['label'], "connection from hyphe" )

# %% codecell

# %% codecell

# %% codecell

# %% markdown
#
# # TODO :
# - [] check errors
# - [] actualise twitter info (date, lieu, nom... Trouver comment avoir site !! )
# - [] Que faire quand pas fichier dat ou twt (pour faire au cas par cas)
# - [] check twitter id dans base. SI yapas, check dans fichier
# %% codecell
def ImportTwitterFollowInNeo4j(url):
    site_name = myUrlParse(url)
    twitter_n = ""

    # Trouve si url est dans la base et si point vers un twitter account
    for r in graph.match((graph.nodes.match(site_name=site_name).first(),)):
        if r.end_node.has_label("Twitter"):
            twitter_n = r.end_node

    if twitter_n == "":
        error = str(datetime.datetime.now()) + " Error 14 : " + str(url) + " has no twitter node in db"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        return

    # If no twitter ID is base, find it in file

    if twitter_n['tw_id'] == None:
        try:
            # Write ID in neo4j
            twitter_n['tw_id'] = int(df_twecoll.loc[df_twecoll['screen_name']==twitter_n['user'], 'id'].iloc[0])
        except:
            error = str(datetime.datetime.now()) + " Error 16 : " + twitter_n['user'] + " has no twecoll info"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return

    # Open twecoll file corresponding to ID
    file_id = "C:/Users/Jo/Nextcloud2/Documents/tech/twecoll/fdat/" + str(twitter_n['tw_id']) + ".f"

    try:
        with open(file_id) as f:
            content = f.readlines()
            # remove whitespace characters like `\n` at the end of each line
            content = [x.strip() for x in content]

            for c in content:
                if int(c) in list(df_twecoll['id']):
                    follows_username = df_twecoll.loc[df_twecoll['id']==int(c), 'screen_name'].iloc[0].lower()
                    target_n = graph.nodes.match("Twitter",user = follows_username).first()
                    if target_n == None:
                        error = str(datetime.datetime.now()) + " Error 15 : " + follows_username + " has no twitter node in db"
                        print(error)
                        with open("error_file.txt", "a", encoding='utf-8') as file:
                            file.write(error +"\n")
                        continue
                    target_n["tw_id"] = c

                    tx = graph.begin()
                    rel = Relationship(twitter_n, "FOLLOWS", target_n)
                    tx.push(target_n)
                    tx.push(twitter_n)
                    tx.merge(rel)
                    tx.commit()
                    #print(rel)



    except:
        error = str(datetime.datetime.now()) + " Error 17 : " + twitter_n['user'] + " has no twecoll info"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        with open("NoTwecoll.txt", "a", encoding='utf-8') as file:
            file.write(twitter_n['user'] +"\n")
        return



# %% markdown
# # Twitter API
# %% codecell
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

# %% codecell
api=create_api()
# %% codecell
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
# %% codecell
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
            return(twitpro_file)

        except:
            error = str(datetime.datetime.now()) + " Error 18 : " + screen_name + " is not a twitter user"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return("")
    else:
        return(twitpro_file)
# %% codecell
def push_twitpro_in_Neo4j(screen_name):
    '''
    From twitter screen name, download twitter profile info and push into Neo4j.
    Save intermediary twitpro file if it does not exists
    '''
    screen_name = screen_name.lower()
    keys = ['id','name','screen_name','location','description','desc_url','url','followers_count',
                  'friends_count', 'listed_count', 'created_at', 'verified', 'statuses_count']

    twitpro_file = write_twitpro_file(screen_name)

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
        twitter_n = graph.nodes.match("Twitter", user = screen_name).first()
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

# %% markdown
# # Youtube API
# %% codecell
def create_YT_API():

    import os
    import google_auth_oauthlib.flow
    import googleapiclient.discovery
    import googleapiclient.errors
    import json

    scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = ytAPI_client_secrets_file_path

    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
    credentials = flow.run_console()
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    return(youtube)

# %% codecell
youtube = create_YT_API()
# %% codecell
def write_ytpro_file(screen_name):
    ''' If file doesnt exists, from youtube screen name download profile info and write on file
    returns file location or "" if user does not exists
    '''

    screen_name = screen_name.lower()
    ytpro_file = path + "/youtube/" + screen_name + '.ytprofile'

    if not Path(ytpro_file).is_file():
        try:
            request = youtube.channels().list(
                part="brandingSettings,contentDetails,id,snippet,statistics",
                forUsername= screen_name
            )
            response = request.execute()

            if len(response["items"])==0:
                error = str(datetime.datetime.now()) + " Error 19 : " + screen_name + " is not a youtube user"
                print(error)
                with open("error_file.txt", "a", encoding='utf-8') as file:
                    file.write(error +"\n")
                return("")

            with open(ytpro_file, "w", encoding='utf-8') as file:
                file.write(str(response['items'][0]))

            return(ytpro_file)


        except:
            error = str(datetime.datetime.now()) + " Error 19 : " + screen_name + " is not a youtube user"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return("")

    else:
        return(ytpro_file)

# %% codecell
def write_ytsub_file(screen_name):
    ''' If file doesnt exists, from youtube screen name download public subscriptions  and write on file
    returns file location or "" if user does not exists or subscriptions not public
    '''

    screen_name = screen_name.lower()
    ytpro_file = write_ytpro_file(screen_name)
    ytsub_file = path + "/youtube/" + screen_name + '.ytsubscriptions'

    if ytpro_file != "":
    # if file exists, reads it and parse into twit_profile_extracted_keys
        with open(ytpro_file, encoding="utf8") as f:
            yt_profile = eval(f.read())

        yt_channelID = yt_profile['id']

        subscriptions =[]

        if not Path(ytsub_file).is_file():

            try: #call for public subscriptions
                request = youtube.subscriptions().list(
                    part="snippet,contentDetails",
                    channelId=yt_channelID,
                    maxResults=50
                )
                res = request.execute()

                for item in res['items']:
                    subscriptions.append(item)

                try:
                    nextPageToken = res['nextPageToken']

                except:
                    nextPageToken = None

                while (nextPageToken):
                    try:
                        request = youtube.subscriptions().list(
                            part="snippet,contentDetails",
                            channelId=yt_channelID,
                            maxResults=50,
                            pageToken=nextPageToken
                        )
                        res = request.execute()


                        for item in res['items']:
                            subscriptions.append(item)

                        nextPageToken = res['nextPageToken']

                    except:
                        break

                with open(ytsub_file, "w", encoding='utf-8') as file:
                    file.write(str(subscriptions))

                return(ytsub_file)

            except:
                ytsub_file=""
                return(ytsub_file)

        else:
            return(ytsub_file)

    else:
        return(ytsub_file)
# %% codecell
def write_ytvideos_file(screen_name):
    ''' If file doesnt exists, from youtube screen name download videos info from upload playlist and write on file
    returns file location or "" if user does not exists or no upload playlist
    '''

    screen_name = screen_name.lower()
    ytpro_file = write_ytpro_file(screen_name)
    ytvideo_file = path + "/youtube/" + screen_name + '.ytvideos'

    if ytpro_file != "":
        with open(ytpro_file, encoding="utf8") as f:
            yt_profile = eval(f.read())

        upload_playlist_ID = yt_profile['contentDetails']['relatedPlaylists']['uploads']
        videos =[]

        if not Path(ytvideo_file).is_file():

            try:
                request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    maxResults=50,
                    playlistId=upload_playlist_ID
                    )
                response_playlistItems = request.execute()

                for item in response_playlistItems['items']:
                    videos.append(item)

                try:
                    nextPageToken = response_playlistItems['nextPageToken']

                except:
                    nextPageToken = None

                while (nextPageToken):
                    try:
                        request = youtube.playlistItems().list(
                        part="snippet,contentDetails",
                        maxResults=50,
                        playlistId=upload_playlist_ID,
                        pageToken=nextPageToken
                        )
                        res = request.execute()

                        for item in res['items']:
                            videos.append(item)

                        nextPageToken = res['nextPageToken']

                    except:
                        break

                with open(ytvideo_file, "w", encoding='utf-8') as file:
                    file.write(str(videos))

                return(ytvideo_file)

            except:
                ytvideo_file =""
                return(ytvideo_file)

        else:
            return(ytvideo_file)

    else:
        return("")
# %% codecell
def write_ytpro_file_fromChannelID(channelID):
    ''' If file doesnt exists, from youtube screen name download profile info and write on file
    returns file location or "" if user does not exists
    '''

    # Check if channelID already in base, in this case return yt_profile from base
    for nodes in graph.nodes.match("Youtube"):
        if nodes['channelID'] == channelID:
            if nodes['ytpro_file']:
                ytpro_file=nodes['ytpro_file']
            else:
                if nodes['customUrl']:
                    screen_name = nodes['customUrl']
                else:
                    if nodes['title']:
                        screen_name = nodes['title'].lower().replace(" ", "_")
                    else:
                        print("this channelID has no title!!!!")
                        return("")
                screen_name = screen_name.lower()
                ytpro_file = path + "/youtube/" + screen_name + '.ytprofile'

            return(ytpro_file)

    request = youtube.channels().list(
        part="brandingSettings,contentDetails,id,snippet,statistics",
        id= channelID
        )
    response = request.execute()

    if len(response["items"])==0:
        error = str(datetime.datetime.now()) + " Error 20 : " + channelID + " is not a youtube channelID"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        return("")

    try:
        screen_name = response["items"][0]['snippet']['customUrl']

    except:
        try:
            screen_name = response['items'][0]['snippet']['title'].lower().replace(" ", "_")
        except:
            print("this channelID has no title!!!!")
            return("")

    screen_name = screen_name.lower()
    ytpro_file = path + "/youtube/" + screen_name + '.ytprofile'

    if not Path(ytpro_file).is_file():
        try:
            with open(ytpro_file, "w", encoding='utf-8') as file:
                file.write(str(response['items'][0]))

            return(ytpro_file)
        except:
            print(screen_name, " is to weird for us. CHange the name !")
            return("")

    else:
        return(ytpro_file)
# %% codecell
def write_yt_links_file(screen_name):
    '''
    '''

    screen_name = screen_name.lower()
    ytpro_file = write_ytpro_file(screen_name)
    ytlinks_file = path + "/youtube/" + screen_name + '.ytlinks'

    if Path(ytlinks_file).is_file():
        return(ytlinks_file)

    if ytpro_file =="":
        return("")

    with open(ytpro_file, encoding="utf8") as f:
        yt_profile = eval(f.read())

    about_url = "https://www.youtube.com/channel/" + yt_profile['id'] +"/about"
    about_page = urllib.request.urlopen(about_url)
    about_soup = BeautifulSoup(about_page)
    links =[]

    for a in about_soup.find_all("a", class_ = "about-channel-link"):
        for ref in a['href'].split('q='):
            if 'http' in ref:
                about_title = a['title']
                about_link = unquote(ref.split('&')[0]).split('#utm')[0]
                already=False
                for link in links:
                    if link['url']== about_link:
                        already=True
                if not already:
                    links.append({'title':about_title,'url':about_link})

    with open(ytlinks_file, "w", encoding='utf-8') as file:
                file.write(str(links))


    return(ytlinks_file)

# %% codecell
def write_yt_files_from_yt_url(url):
    screen_name=""

    CustomChannels = {"alimentationgenerale1":"UCgmfhT7laKrH4C4d3KnqyNw","numerama":"UCAz-755tH3m8_BwaluzdJwQ",
    "FondationPol%C3%A9mia":"UCMf8__hXS4X7j2eTY37Mqiw","mercato":"UCrzDtXyuSBch2u_31JJj-Dw",
    "SanteplusmagOfficiel":"UC_c2DThPTzuw3DB3fdNbZAw",
    "TouteleuropeEu":"UCvrqvZ1ABclMKlMLQKfutQA","TSAToutsurlAlg%C3%A9rie":"UC0a2EoPfE6Rh50V8ZDqeH0w",
    "ajplussaha":"UCEg4_mU2CqqtQxWwzL_uh-w","aleteiafr":"UCphfPDeHIsRXcQ_v7nIjL1Q",
    "letemps":"UCam7yYDSKFIpwAscmyzOsQQ","euractiv":"UCDCgvwyjJqZWYs6j0D2FF6g"}

    # if url is not
    if myUrlParse(url) != "youtube.com":
        error = str(datetime.datetime.now()) + " Error 21 : " + url + " is not a youtube url"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        return


    # check if channelID
    if "youtube.com/channel" in url:
        channelID = url.replace('https://www.youtube.com/channel/',"").replace('https://youtube.com/channel/',"").replace("/featured","")[0:24]
        channelID = channelID.split("/")[0]
        ytpro_file = write_ytpro_file_fromChannelID(channelID)

        try:
            with open(ytpro_file, encoding="utf8") as f:
                yt_profile = eval(f.read())

            try:
                screen_name= yt_profile['snippet']['customUrl']

            except:
                screen_name = yt_profile['snippet']['title'].lower().replace(" ", "_")

        except:
            error = str(datetime.datetime.now()) + " Error 23 : "+ channelID + " is not a valid youtube channel"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return


    # check if user
    if "user" in url:
        url=url.replace("?feature=watch", "").replace("/videos","").replace('?sub_confirmation=1','').replace("?feature=guide","")
        screen_name = url.split("user/")[1].split("/")[0]
        ytpro_file = write_ytpro_file(screen_name)


    # This code doesn't take any other format
    # check if results
    if "youtube.com/results" in url:
        if url == "https://www.youtube.com/results?search_query=riposte+laique":
            channelID = "UCv7I-SNC4Con0BfdLEWHUPA"
            ytpro_file = write_ytpro_file_fromChannelID(channelID)
            screen_name = "ciceropicas"
        else:
            return("results")

    # Check if playlist
    if "youtube.com/playlist" in url:
        return("playlist")

    # else if link du type http://www.youtube.com/watch?v=dtUH2YSFlVU, fait rien
    if "youtube.com/watch" in url:
        return("watch")


    # If url is like that : "https://www.youtube.com/c/alimentationgenerale1" or "http://www.youtube.com/taranisnews"
    if screen_name=="":
        if "/c/" in url:
            customName = url.split("/c/")[1]
        else:
            customName = url.split("youtube.com/")[1]

        if customName in CustomChannels:
            channelID = CustomChannels[customName]

        else:
            # ID request
            request = youtube.channels().list(
                part="id",
                forUsername = customName
                )
            response = request.execute()
            try:
                channelID = response['items'][0]['id']
            except:
                error = str(datetime.datetime.now()) + " Error 24 : "+ url + " is not a valid youtube url"
                print(error)
                with open("error_file.txt", "a", encoding='utf-8') as file:
                    file.write(error +"\n")
                return

        ytpro_file = write_ytpro_file_fromChannelID(channelID)

        try:
            with open(ytpro_file, encoding="utf8") as f:
                yt_profile = eval(f.read())

            try:
                screen_name= yt_profile['snippet']['customUrl']

            except:
                screen_name = yt_profile['snippet']['title'].lower().replace(" ", "_")

        except:
            error = str(datetime.datetime.now()) + " Error 25 : "+ channelID + " is not a valid youtube channel"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return


    # check if customURL cohérent

    ytsub_file = write_ytsub_file(screen_name)

    try:
        with open(ytsub_file, encoding="utf8") as f:
            yt_sub = eval(f.read())
    except:
        yt_sub =""

    yt_links_file = write_yt_links_file(screen_name)

    return(ytpro_file,ytsub_file,yt_links_file)

# %% codecell
def write_yt_pro_file_from_yt_url(url):
    screen_name=""

    CustomChannels = {"alimentationgenerale1":"UCgmfhT7laKrH4C4d3KnqyNw","numerama":"UCAz-755tH3m8_BwaluzdJwQ",
    "FondationPol%C3%A9mia":"UCMf8__hXS4X7j2eTY37Mqiw","mercato":"UCrzDtXyuSBch2u_31JJj-Dw",
    "SanteplusmagOfficiel":"UC_c2DThPTzuw3DB3fdNbZAw",
    "TouteleuropeEu":"UCvrqvZ1ABclMKlMLQKfutQA","TSAToutsurlAlg%C3%A9rie":"UC0a2EoPfE6Rh50V8ZDqeH0w",
    "ajplussaha":"UCEg4_mU2CqqtQxWwzL_uh-w","aleteiafr":"UCphfPDeHIsRXcQ_v7nIjL1Q",
    "letemps":"UCam7yYDSKFIpwAscmyzOsQQ","euractiv":"UCDCgvwyjJqZWYs6j0D2FF6g"}

    # if url is not
    if myUrlParse(url) != "youtube.com":
        error = str(datetime.datetime.now()) + " Error 21 : " + url + " is not a youtube url"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        return


    # check if channelID
    if "youtube.com/channel" in url:
        channelID = url.replace('https://www.youtube.com/channel/',"").replace('https://youtube.com/channel/',"").replace("/featured","")[0:24]
        channelID = channelID.split("/")[0]
        ytpro_file = write_ytpro_file_fromChannelID(channelID)

        try:
            with open(ytpro_file, encoding="utf8") as f:
                yt_profile = eval(f.read())

            try:
                screen_name= yt_profile['snippet']['customUrl']

            except:
                screen_name = yt_profile['snippet']['title'].lower().replace(" ", "_")

        except:
            error = str(datetime.datetime.now()) + " Error 23 : "+ channelID + " is not a valid youtube channel"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return


    # check if user
    if "user" in url:
        url=url.replace("?feature=watch", "").replace("/videos","").replace('?sub_confirmation=1','').replace("?feature=guide","")
        screen_name = url.split("user/")[1].split("/")[0]
        ytpro_file = write_ytpro_file(screen_name)


    # This code doesn't take any other format
    # check if results
    if "youtube.com/results" in url:
        if url == "https://www.youtube.com/results?search_query=riposte+laique":
            channelID = "UCv7I-SNC4Con0BfdLEWHUPA"
            ytpro_file = write_ytpro_file_fromChannelID(channelID)
            screen_name = "ciceropicas"
        else:
            return("results")

    # Check if playlist
    if "youtube.com/playlist" in url:
        return("playlist")

    # else if link du type http://www.youtube.com/watch?v=dtUH2YSFlVU, fait rien
    if "youtube.com/watch" in url:
        return("watch")


    # If url is like that : "https://www.youtube.com/c/alimentationgenerale1" or "http://www.youtube.com/taranisnews"
    if screen_name=="":
        if "/c/" in url:
            customName = url.split("/c/")[1]
        else:
            customName = url.split("youtube.com/")[1]

        if customName in CustomChannels:
            channelID = CustomChannels[customName]

        else:
            # ID request
            request = youtube.channels().list(
                part="id",
                forUsername = customName
                )
            response = request.execute()
            try:
                channelID = response['items'][0]['id']
            except:
                error = str(datetime.datetime.now()) + " Error 24 : "+ url + " is not a valid youtube url"
                print(error)
                with open("error_file.txt", "a", encoding='utf-8') as file:
                    file.write(error +"\n")
                return

        ytpro_file = write_ytpro_file_fromChannelID(channelID)

        try:
            with open(ytpro_file, encoding="utf8") as f:
                yt_profile = eval(f.read())

            try:
                screen_name= yt_profile['snippet']['customUrl']

            except:
                screen_name = yt_profile['snippet']['title'].lower().replace(" ", "_")

        except:
            error = str(datetime.datetime.now()) + " Error 25 : "+ channelID + " is not a valid youtube channel"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return


    return(ytpro_file)
# %% codecell
def push_yt_profile_inNeo4j(url):
    '''
    For existing yt node : from its url download the yt pro files and push youtube node with profile data
    returns node or if node doesn't exist, do nothing
    '''

    keys = {'id':'channelID',
        'snippet/title':'title',
        'snippet/description':'description',
        'snippet/customUrl':'customUrl',
        'snippet/publishedAt':'publishedAt',
        'snippet/country':'country',
        'contentDetails/relatedPlaylists/uploads':'uploadPlaylist',
        'statistics/viewCount':'viewCount',
        'statistics/commentCount':'commentCount',
        'statistics/subscriberCount':'subscriberCount',
        'statistics/hiddenSubscriberCount':'hiddenSubscriberCount',
        'statistics/videoCount':'videoCount',
        'brandingSettings/channel/keywords':'keywords'
       }

    # find node with the corresponding youtube url
    yt_n = graph.nodes.match(url=url).first()
    if yt_n == None:
        return

    # download les files
    if write_yt_files_from_yt_url(url)in (None, "watch" , "playlist" , "results"):
        if write_yt_files_from_yt_url(url)== None:
            errortype =""
        else:
            errortype=write_yt_files_from_yt_url(url)
        error = str(datetime.datetime.now()) + " Error 22 : "+ errortype +" - "+ url + " is not a valid youtube url. Please check the corresponding yt node"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
        return

    ytpro_file = write_yt_pro_file_from_yt_url(url)

    # update node info with youtube profile info
    with open(ytpro_file, encoding="utf8") as f:
        yt_profile = eval(f.read())

    yt_n['ytpro_file'] = ytpro_file

    for key in keys:
        try:
            if len(key.split('/'))==1:
                yt_n[keys[key]]= yt_profile[key]
            if len(key.split('/'))==2:
                yt_n[keys[key]] = yt_profile[key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
            if len(key.split('/'))==3:
                yt_n[keys[key]] = yt_profile[key.split('/')[len(key.split('/'))-3]][key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
        except:
            pass

    # push le node
    tx = graph.begin()
    tx.push(yt_n)
    tx.commit()
    return(yt_n)
# %% codecell
# not used... Delete ?

def push_yt_featured_channels_inNeo4j(url):
    '''
    '''
    keys = {'id':'channelID',
        'snippet/title':'title',
        'snippet/description':'description',
        'snippet/customUrl':'customUrl',
        'snippet/publishedAt':'publishedAt',
        'snippet/country':'country',
        'contentDetails/relatedPlaylists/uploads':'uploadPlaylist',
        'statistics/viewCount':'viewCount',
        'statistics/commentCount':'commentCount',
        'statistics/subscriberCount':'subscriberCount',
        'statistics/hiddenSubscriberCount':'hiddenSubscriberCount',
        'statistics/videoCount':'videoCount',
        'brandingSettings/channel/keywords':'keywords'
       }

    # find node with the corresponding youtube url
    yt_n = graph.nodes.match(url=url).first()

    # check les files
    (ytpro_file,ytsub_file,yt_links_file) = write_yt_files_from_yt_url(url)

    with open(ytpro_file, encoding="utf8") as f:
        yt_profile = eval(f.read())

    try:
        for channel in yt_profile['brandingSettings']['channel']['featuredChannelsUrls']:
            print(channel)
            #cherche si ya dejà un node avec channelID
            yt_featchan_n = graph.nodes.match(channelID=channel).first()

            feat_chan_file = write_ytpro_file_fromChannelID(channel)
            with open(feat_chan_file, encoding="utf8") as file:
                feat_chan_pro = eval(file.read())

            if not yt_featchan_n:
                # créé node from pro_fil
                # créer node yt_featchan_n
                yt_user = feat_chan_file.replace(path + "/youtube/","").replace('.ytprofile',"")
                yt_featchan_n = Node("Youtube", user = yt_user)


            # fill node properties from pro_file:
            for key in keys:
                try:
                    if len(key.split('/'))==1:
                        yt_featchan_n[keys[key]]= feat_chan_pro[key]
                    if len(key.split('/'))==2:
                        yt_featchan_n[keys[key]] = feat_chan_pro[key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
                    if len(key.split('/'))==3:
                        yt_featchan_n[keys[key]] = feat_chan_pro[key.split('/')[len(key.split('/'))-3]][key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
                except:
                    pass


            source_n = yt_n
            target_n = yt_featchan_n
            rel = Relationship(source_n, "LINKS_TO", target_n)
            rel["yt_type"]="featured_Channel"
            #tx = graph.begin()
            #tx.merge(yt_featchan_n, "Youtube", "user" )
            #tx.merge(rel)
            #tx.commit()
            print("source: ", source_n['user']," target: ",target_n['user'], " rel:", rel)

    except:
        pass

# %% codecell
def push_ytnode_featured_channels_inNeo4j(yt_n):
    '''
    from yt node, create featured channels nodes and LINKS_TO links
    returns all relationships as list of relationships
    '''

    keys = {'id':'channelID',
        'snippet/title':'title',
        'snippet/description':'description',
        'snippet/customUrl':'customUrl',
        'snippet/publishedAt':'publishedAt',
        'snippet/country':'country',
        'contentDetails/relatedPlaylists/uploads':'uploadPlaylist',
        'statistics/viewCount':'viewCount',
        'statistics/commentCount':'commentCount',
        'statistics/subscriberCount':'subscriberCount',
        'statistics/hiddenSubscriberCount':'hiddenSubscriberCount',
        'statistics/videoCount':'videoCount',
        'brandingSettings/channel/keywords':'keywords'
       }
    all_rel =[]
    # from node, go to pro_file
    if yt_n['ytpro_file']:
        ytpro_file = yt_n['ytpro_file']
    else:
        try:
            ytpro_file = push_yt_profile_inNeo4j(yt_n['url'])['ytpro_file']
        except:
            error = str(datetime.datetime.now()) + " Error 23b : node" +str(yt_n) + " has no valid profile"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return




    # check si node exists (from channelID)
        # if no, create it (download profile)
        # if yes build link


    # check les files

    # read the channels.
    with open(ytpro_file, encoding="utf8") as f:
        yt_profile = eval(f.read())

    try:
        # for each channel:
        for channel in yt_profile['brandingSettings']['channel']['featuredChannelsUrls']:
            #cherche si ya dejà un node avec channelID
            yt_featchan_n = graph.nodes.match(channelID=channel).first()

            feat_chan_file = write_ytpro_file_fromChannelID(channel)
            with open(feat_chan_file, encoding="utf8") as file:
                feat_chan_pro = eval(file.read())

            if not yt_featchan_n:
                # créé node from pro_fil
                # créer node yt_featchan_n
                yt_user = feat_chan_file.replace(path + "/youtube/","").replace('.ytprofile',"")
                yt_featchan_n = Node("Youtube", user = yt_user)


            # fill node properties from pro_file:
            for key in keys:
                try:
                    if len(key.split('/'))==1:
                        yt_featchan_n[keys[key]]= feat_chan_pro[key]
                    if len(key.split('/'))==2:
                        yt_featchan_n[keys[key]] = feat_chan_pro[key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
                    if len(key.split('/'))==3:
                        yt_featchan_n[keys[key]] = feat_chan_pro[key.split('/')[len(key.split('/'))-3]][key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
                except:
                    pass


            source_n = yt_n
            target_n = yt_featchan_n
            rel = Relationship(source_n, "LINKS_TO", target_n)
            rel["yt_type"]="featured_Channel"
            tx = graph.begin()
            tx.merge(yt_featchan_n, "Youtube", "user" )
            tx.merge(rel)
            tx.commit()
            all_rel.append(rel)
            #print("source: ", source_n['user']," target: ",target_n['user'], " rel:", rel)

    except:
        pass
    return(all_rel)

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell
# KEEP : This is all Youtube nodes that comes from Website !

for yt_n in graph.nodes.match("Youtube"):
    link_to_yt_n = graph.match(r_type="LINKS_TO", nodes=(None, yt_n))
    for rel in link_to_yt_n:
        if rel.nodes[0].has_label("Website"):
            print(rel.nodes[0]['label'], " -> ", yt_n['title'])
# %% codecell

# %% markdown
# # Continue here
#
# %% codecell

# This builds all featured channels links from youtube nodes that comes from website

for yt_n in graph.nodes.match("Youtube"):
    link_to_yt_n = graph.match(r_type="LINKS_TO", nodes=(None, yt_n))
    for rel in link_to_yt_n:
        if rel.nodes[0].has_label("Website"):
            print(yt_n['url'])
            try:
                push_yt_profile_inNeo4j(yt_n['url'])
                print(push_ytnode_featured_channels_inNeo4j(yt_n))
            except:
                print("ERROR WITH NODE", node)

# %% codecell
# and build relationships directly from nodes
# %% codecell
# THIS HAS TO BE TESTED !!!!!!!!!


def push_ytnode_links_inNeo4j(yt_n):
    all_rel=[]

    if yt_n['ytpro_file']: # ça marche pas quand ya watch par exemple
        screen_name = yt_n['ytpro_file'].replace(path + "/youtube/","").replace(".ytprofile","")

        yt_links_file = write_yt_links_file(screen_name)
        with open(yt_links_file, encoding="utf8") as f:
            yt_links = eval(f.read())

        for link in yt_links:
            #Build node
            target_n = PushUrlInfoInNeo4j(link['url'])

            # if twitter : cree twitter node
            if getTwitterUsername(link['url'])!="":
                target_n = push_twitpro_in_Neo4j(screen_name)

            # if youtube : créé yt node
            if getYouTubeNameAndurl(link['url'])!=('', ''):
                # if node doesnt exist: create it
                if graph.nodes.match(url=url).first()==None:
                    tx = graph.begin()
                    target_n = Node("Youtube", user = yt_user, url = yt_url )
                    tx.merge(target_n, "Youtube", "user" )
                    tx.commit()

                # Push profile info to the node
                push_yt_profile_inNeo4j(link['url'])

            # if facebook : cree fb node
            if getFacebookUsername(link['url'])!="":
                tx = graph.begin()
                fb = getFacebookUsername(link['url'])
                target_n = Node("Facebook", user = fb)
                tx.merge(target_n, "Facebook", "user" )
                tx.commit()

            #Build link
            rel = Relationship(yt_n, "LINKS_TO",target_n)
            rel["yt_type"]="link"
            all_rel.append(rel)

# %% codecell

# %% codecell
# starts from yt_n
screen_name = yt_n['ytpro_file'].replace(path + "/youtube/","").replace(".ytprofile","")

write_yt_links_file(screen_name)
yt_links_file = write_yt_links_file(screen_name)
with open(yt_links_file, encoding="utf8") as f:
    yt_links = eval(f.read())

for link in yt_links:

    if getTwitterUsername(link['url'])!="":
        print(link['url'],myUrlParse(link['url']),getTwitterUsername(link['url']))
        #twit_n = push_twitpro_in_Neo4j(screen_name)
    else:
        if getFacebookUsername(link['url'])!="":
            print(link['url'],myUrlParse(link['url']),getFacebookUsername(link['url']))
        else:
            if getYouTubeNameAndurl(link['url'])!=('', ''):
                print(link['url'],myUrlParse(link['url']),getYouTubeNameAndurl(link['url']))
            else:
                print(link['url'],myUrlParse(link['url']) )
# %% codecell
# KEEP : This is all Youtube nodes that comes from Website !

for yt_n in graph.nodes.match("Youtube").limit(50):
    link_to_yt_n = graph.match(r_type="LINKS_TO", nodes=(None, yt_n))
    for rel in link_to_yt_n:
        if rel.nodes[0].has_label("Website"):
            try:
                print(yt_n['ytpro_file'].replace(path + "/youtube/","").replace(".ytprofile",""))
            except:
                print(yt_n)
            #push_ytnode_links_inNeo4j(yt_n)
            #print("")
# %% codecell
yt_n
# %% codecell
url="https://www.mediapart.fr/"
PushUrlInfoInNeo4j(url)
# %% codecell
def PushRSUrlInfoInNeo4j(url):
    '''From url or sitename, read firstpage, extract SN info and push nodes and relationship in neo4j'''

    (site_name, title ) = getFirstPageNode(url)
    siteNodeRes = graph.nodes.match("Website", site_name=site_name)
    sitenodenb = len(siteNodeRes)

    if sitenodenb != 1:
        error = str(datetime.datetime.now()) + " Error 9 : " + site_name + " has " + str(sitenodenb) + " nodes in DB"
        print(error)
        with open("error_file.txt", "a", encoding='utf-8') as file:
            file.write(error +"\n")
    else:
        siteNode=siteNodeRes.first()
        (fb,twit,yt_user,yt_url,sc,insta,dm)= getFirstPageRSNode(url)

        tx = graph.begin()

        if fb != "":
            fb_n = Node("Facebook", user = fb)
            tx.merge(fb_n, "Facebook", "user" )
            site_fb_r = Relationship(siteNode, "LINKS_TO", fb_n)
            tx.merge(site_fb_r)

        if twit != "":
            twit_n = Node("Twitter", user = twit)
            tx.merge(twit_n, "Twitter", "user" )
            site_twit_r = Relationship(siteNode, "LINKS_TO", twit_n)
            tx.merge(site_twit_r)

        if yt_user != "":
            yt_n = Node("Youtube", user = yt_user, url = yt_url )
            tx.merge(yt_n, "Youtube", "user" )
            site_yt_r = Relationship(siteNode, "LINKS_TO", yt_n)
            tx.merge(site_yt_r)

        tx.commit()
    return(siteNode)

# %% codecell
graph.nodes.match("Youtube").first()['test']
# %% codecell
graph.nodes.match("Youtube").first()
# %% codecell
bourgogne_n
# %% codecell
yt_n = Node("Youtube", user = yt_user, url = yt_url )
yt_n['test']="test"
yt_n
# %% codecell
tx = graph.begin()
tx.push(bourgogne_n )
tx.commit()
# %% codecell

# %% markdown
# # ICI
# %% codecell

# %% codecell


# %% codecell

# %% codecell

# %% codecell
# pareil pour links
with open(yt_links_file, encoding="utf8") as f:
    yt_links = eval(f.read())

for link in yt_links:
    print(link['url'],myUrlParse(link['url']) )

    if getTwitterUsername(link['url'])!="":
        print(getTwitterUsername(link['url']))
        #twit_n = push_twitpro_in_Neo4j(screen_name)
    if getFacebookUsername(link['url'])!="":
        print(getFacebookUsername(link['url']))
    if getYouTubeNameAndurl(link['url'])!=('', ''):
        print(getYouTubeNameAndurl(link['url']))

    # SI c'est réseau social



# et creer FOLLOWS relationships pour subscriptions
# %% codecell
import glob
linkslist={""}
path2 = path+"\\youtube\*.ytlinks"
i=0
linknum = 0
for yt_links_file in glob.glob(path2):
    linknum = linknum+1
    #print(linknum, yt_links_file)

    with open(yt_links_file) as f:
        yt_links = eval(f.read())
    for link in yt_links:
        linkslist.add(myUrlParse(link['url']))

len(linkslist)
for link in linkslist:
    if graph.nodes.match(site_name=link).first()==None:
        print(link)

# Check node et construit
# Creer lien LINKS TO vers site

# créé lien LINKS TO pour les feature channels
# Cree lien FOLLOWS pour subscriptions


# Faire pareil sur twitter avec les twitter profiles

# Inserer les liens Alexa
# Checker les sites Alexa

# Monde diplo
# ACPM

# Tester recherche de site + liens
# %% codecell

# %% codecell

# %% codecell

# %% codecell
yt_links_file
with open(yt_links_file) as f:
    yt_link = f.read()
print(yt_link)
# %% codecell
# Trouve noeud. Si existe, builds relationship to channel. Otherwise create it.
# %% codecell
#linkslist={"1"}
linkslist.add("2")
# %% codecell
print(linkslist)
# %% codecell
myUrlParse("http://www.gq.com/")
# %% codecell

# %% codecell
for node in graph.nodes.match():
    try:
        if "gq" in node['site_name']:
            print(node)
    except:
        pass
# %% codecell

# %% codecell

# %% codecell

# %% codecell
yt_profile['brandingSettings']['channel']
# %% codecell
# il manque creer les noeuds et connections pour ces channels:
for channel in yt_profile['brandingSettings']['channel']['featuredChannelsUrls']:
    print(channel)


# %% codecell
# creer aussi les liens FOLLOWS sur ces comptes :
with open(ytsub_file, encoding="utf8") as f:
        yt_sub = eval(f.read())

for sub in yt_sub:
    print(sub['snippet']['title'])
# %% codecell
# et les liens pour ces liens:
with open(yt_links_file, encoding="utf8") as f:
    yt_links = eval(f.read())
for link in yt_links:
    print(link['url'])
print(link['url'])
# %% codecell
# Virer les trucs qui correspondent à mediapart sur le noeud fakir
yt_n
# %% codecell
# liste des sites que le youtube node link : avant
for r in graph.match((yt_n,)):
    print(r, r.end_node)
# %% codecell
# ça ça chope le noeud qui correspond au site et créé le lien yt vers site
PushRSUrlInfoInNeo4j(link['url'])
tx = graph.begin()
site_yt_r = Relationship(yt_n, "LINKS_TO", PushRSUrlInfoInNeo4j(link['url']))
tx.merge(site_yt_r)

tx.commit()
# %% codecell
# Et si refait ça :liste des sites que le youtube node link : avant
for r in graph.match((yt_n,)):
    print(r, r.end_node)

# C'est bon !!
# %% markdown
# # Refait avec glamourParis
#
# %% codecell
url="https://www.youtube.com/channel/UCSnniCDEE-rapQ6zriW0LfQ"
# %% codecell
# trouve noeuds youtube avec cette url
yt_n = graph.nodes.match(url=url).first()
yt_n
# %% codecell
# check les files
# Doesn't work because of quota
'''(ytpro_file,ytsub_file,yt_links_file) = write_yt_files_from_yt_url(url)'''
# Try this :
url="https://www.youtube.com/user/glamourparis"
(ytpro_file,ytsub_file,yt_links_file) = write_yt_files_from_yt_url(url)
# %% codecell
with open(ytpro_file, encoding="utf8") as f:
        yt_profile = eval(f.read())
yt_profile
# %% codecell
# ca definie les keys et test le bousin en printant
keys = {'id':'channelID',
        'snippet/title':'title',
        'snippet/description':'description',
        'snippet/customUrl':'customUrl',
        'snippet/publishedAt':'publishedAt',
        'snippet/country':'country',
        'contentDetails/relatedPlaylists/uploads':'uploadPlaylist',
        'statistics/viewCount':'viewCount',
        'statistics/commentCount':'commentCount',
        'statistics/subscriberCount':'subscriberCount',
        'statistics/hiddenSubscriberCount':'hiddenSubscriberCount',
        'statistics/videoCount':'videoCount',
        'brandingSettings/channel/keywords':'keywords'
       }
i=0
for key in keys:
    i=i+1
    try:
        if len(key.split('/'))==1:
            print(i,key, yt_profile[key], keys[key])
        if len(key.split('/'))==2:
            print(i,key, yt_profile[key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]],keys[key])
        if len(key.split('/'))==3:
            print(i,key, yt_profile[key.split('/')[len(key.split('/'))-3]][key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]], keys[key])
    except:
        print("error",key )
# %% codecell
# ajoute les trucs dans le node
i=0
for key in keys:
    i=i+1
    try:
        if len(key.split('/'))==1:
            yt_n[keys[key]]= yt_profile[key]
            print(i,key, yt_profile[key], keys[key])
        if len(key.split('/'))==2:
            yt_n[keys[key]] = yt_profile[key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
            print(i,key, yt_profile[key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]],keys[key])
        if len(key.split('/'))==3:
            yt_n[keys[key]] = yt_profile[key.split('/')[len(key.split('/'))-3]][key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
            print(i,key, yt_profile[key.split('/')[len(key.split('/'))-3]][key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]], keys[key])
    except:
        print("error",key )

yt_n
# %% codecell
# push le node
tx = graph.begin()
tx.push(yt_n)
tx.commit()

# %% codecell
url_channel="https://www.youtube.com/channel/UCSnniCDEE-rapQ6zriW0LfQ"
# check le node:
yt_n = graph.nodes.match(url=url_channel).first()
yt_n
# %% codecell
# il manque creer les noeuds et connections pour ces channels:
for channel in yt_profile['brandingSettings']['channel']['featuredChannelsUrls']:
    print(channel)
# %% codecell
# creer aussi les liens FOLLOWS sur ces comptes :
with open(ytsub_file, encoding="utf8") as f:
        yt_sub = eval(f.read())

for sub in yt_sub:
    print(sub['snippet']['title'])
# %% codecell
# et les liens pour ces liens:
with open(yt_links_file, encoding="utf8") as f:
    yt_links = eval(f.read())
for link in yt_links:
    print(link['url'])
# Pas de liens
# %% codecell
yt_links_file
# %% codecell
yt_links
# %% codecell

# %% codecell

# %% codecell
for key1 in s.keys():
    print(key1)
    if type(yt_profile[key1])==dict:
        for key2 in yt_profile[key1].keys():
            print("1 ", key2)
            if type(yt_profile[key1][key2])==dict:
                for key3 in yt_profile[key1][key2].keys():
                    print("1 2 ", key3)
                    if type(yt_profile[key1][key2][key3])==dict:
                        for key4 in yt_profile[key1][key2][key3].keys():
                            print("1 2 3 ", key4)
                            if type(yt_profile[key1][key2][key3][key4])==dict:
                                for key5 in yt_profile[key1][key2][key3][key4].keys():
                                    print("1 2 3 4", key5)
# %% codecell

# %% codecell
node = graph.nodes.match(url=url).first()

keys =
# %% codecell

# %% codecell
keys = ['id','name','screen_name','location','description','desc_url','url','followers_count',
                  'friends_count', 'listed_count', 'created_at', 'verified', 'statuses_count']

    twitpro_file = write_twitpro_file(screen_name)

for k in keys:
            twitter_n[k] = twit_profile_extracted_keys[k]
        # push in DB
        tx = graph.begin()
        tx.push(twitter_n)
        tx.commit()
        return(twitter_n)
# %% codecell

# %% codecell

# %% codecell
for node in graph.nodes.match("Youtube"):
    url = node['url']
    if ("user" in url) or ("channel" in url) or ("playlist" in url) or ("watch" in url):
        pass
    else:
        if "c/" in url:
            custom_name = url.split("/c/")[1]
        else:
            custom_name = url.split("/")[3]

        request = youtube.search().list(
            part="id,snippet",
            maxResults=1,
            q=custom_name,
            type="channel"
            )
        response = request.execute()

        try:
            print(custom_name, response['items'][0]['snippet']['title'])
        except:
            print("error with", custom_name)
# %% codecell

# %% codecell
i=0
j=0
for node in graph.nodes.match("Youtube"):
    i=i+1
    url=node['url']
    print(url)
    try:
        print(write_yt_files_from_yt_url(url))
    except:
        j=j+1
print(i,j)
# %% codecell
write_yt_files_from_yt_url(graph.nodes.match("Youtube").first()['url'])
# %% codecell
graph.nodes.match("Youtube").first()
# %% codecell

# %% codecell

# %% codecell

# %% codecell
screen_name = screen_name.lower()
ytpro_file = write_ytpro_file(screen_name)
ytlinks_file = path + "/youtube/" + screen_name + '.ytlinks'

if ytpro_file != "":
    with open(ytpro_file, encoding="utf8") as f:
        yt_profile = eval(f.read())

about_url = "https://www.youtube.com/channel/" + yt_profile['id'] +"/about"
about_page = urllib.request.urlopen(about_url)
about_soup = BeautifulSoup(about_page)

# %% codecell
links = []
# %% codecell
for a in about_soup.find_all("a", class_ = "about-channel-link"):
    for ref in a['href'].split('q='):
        if 'http' in ref:
            about_title = a['title']
            about_link = unquote(ref.split('&')[0])
            already=False
            for link in links:
                if link['url']== about_link:
                    already=True
            if not already:
                links.append({'title':about_title,'url':about_link})
# %% codecell
links
# %% codecell
for link in links:
    print(link['url'])
# %% codecell
# Si channelID, faire ça d'abord :
channelID = "UC2wrXSZMid7OiCVwCfVpHiA"
ytpro_file = write_ytpro_file_fromChannelID(channelID)

try:
    with open(ytpro_file, encoding="utf8") as f:
        yt_profile = eval(f.read())

    try:
        screen_name= yt_profile['snippet']['customUrl']
    except:
        screen_name = response['items'][0]['snippet']['title'].lower().replace(" ", "_")

except:
    pass
# %% codecell
screen_name = "lemediaofficiel"
# %% codecell
ytsub_file = write_ytsub_file(screen_name)

try:
    with open(ytsub_file, encoding="utf8") as f:
        yt_sub = eval(f.read())
except:
    yt_sub =""

ytvideo_file = write_ytvideos_file(screen_name)

try:
    with open(ytvideo_file, encoding="utf8") as f:
        yt_video = eval(f.read())
except:
    yt_video =""

print(screen_name, " has ", len(yt_sub), " public subscriptions and ", len(yt_video), " videos")


# %% codecell
screen_name = "ufcquechoisir"
ytsub_file = write_ytsub_file(screen_name)

try:
    with open(ytsub_file, encoding="utf8") as f:
        yt_sub = eval(f.read())
except:
    yt_sub =""
print(len(yt_sub))

# %% codecell
screen_name = 'toulouseinfos'
ytsub_file = write_ytsub_file(screen_name)
# %% codecell
print("there is ", len(graph.nodes.match("Youtube")), "youtube channels in base")
cha = 0
use = 0

for node in graph.nodes.match("Youtube"):
    if "channel" in str(node['url']):
        cha = cha +1
        print("cha : ",cha)
    else:
        if "user" in str(node['url']):
            use = use +1
            print("use : ", use)

        else:
            print(node)



print(cha, " channels and ", use, "users")
# %% codecell
# TODO WITH QUOTA :
# 1
for node in graph.nodes.match("Youtube"):
    if "user" in str(node['url']):
        screen_name = node['user'].replace("?feature=watch", "").replace("/videos","").replace('?sub_confirmation=1','').replace("?feature=guide","")
        ytsub_file = write_ytsub_file(screen_name)
        #ytvideo_file = write_ytvideos_file(screen_name)
        try:
            with open(ytsub_file, encoding="utf8") as f:
                yt_sub = eval(f.read())
        except:
            yt_sub =""
            '''
        try:
            with open(ytvideo_file, encoding="utf8") as f:
                yt_video = eval(f.read())
        except:
            yt_video =""
            '''
        write_yt_links_file(screen_name)
        with open(write_yt_links_file(screen_name), encoding="utf8") as f:
            links = eval(f.read())
        print(screen_name, " has ", len(yt_sub), " public subscriptions and ", len(links)," links" )
# %% codecell
# 2
for node in graph.nodes.match("Youtube"):
    if "channel" in str(node['url']):
        channelID = node['url'].replace('https://www.youtube.com/channel/',"").replace('https://youtube.com/channel/',"").replace("/featured","")[0:24]

        ytpro_file = write_ytpro_file_fromChannelID(channelID)
        try:
            with open(ytpro_file, encoding="utf8") as f:
                yt_profile = eval(f.read())

            try:
                screen_name= yt_profile['snippet']['customUrl']

            except:
                screen_name = yt_profile['snippet']['title'].lower().replace(" ", "_")

        except:
            print("error with channelID ", channelID)

        ytsub_file = write_ytsub_file(screen_name)

        try:
            with open(ytsub_file, encoding="utf8") as f:
                yt_sub = eval(f.read())
        except:
            yt_sub =""


        write_yt_links_file(screen_name)
        with open(write_yt_links_file(screen_name), encoding="utf8") as f:
            links = eval(f.read())

        print(screen_name, " has ", len(yt_sub), " public subscriptions and ", len(links)," links" )

# %% codecell
# 3
for node in graph.nodes.match("Youtube"):
    if not ("channel") in str(node['url']):
        if not ("user") in str(node['url']):
            if not "watch" in str(node['url']):
                if not "playlist" in str(node['url']):
                    if not "%" in str(node['url']):
                        if not "result" in str(node['url']):
                            screen_name = node['user']
                            ytsub_file = write_ytsub_file(screen_name)
                            try:
                                with open(ytsub_file, encoding="utf8") as f:
                                    yt_sub = eval(f.read())
                            except:
                                yt_sub =""

                            write_yt_links_file(screen_name)
                            try:
                                links=""
                                with open(write_yt_links_file(screen_name), encoding="utf8") as f:
                                    links = eval(f.read())
                            except:
                                pass
                            print(screen_name, " has ", len(yt_sub), " public subscriptions and ", len(links)," links" )
# %% codecell
for node in graph.nodes.match("Youtube"):
    if not ("channel") in str(node['url']):
        if not ("user") in str(node['url']):
            if not "watch" in str(node['url']):
                if not "playlist" in str(node['url']):
                    if not "%" in str(node['url']):
                        if not "result" in str(node['url']):
                            print(node)
# %% codecell
screen_name = "numerama"
request = youtube.channels().list(
    part="id",
    forUsername= screen_name
    )
response = request.execute()
response
# %% codecell
for node in graph.nodes.match("Youtube"):
    print(node['url'])
# %% codecell
# Que faire pour les watch ?

for node in graph.nodes.match("Youtube"):
    if "channel" in str(node['url']):
        pass
    else:
        if "user" in str(node['url']):
            pass
        else:
            if "watch" in str(node['url']):
                print(node['url'],node['user'])
# %% codecell

# %% codecell

# %% codecell
for node in graph.nodes.match("Twitter"):
    if "youtube" in str(node).lower():
        print(node)
# %% codecell

# %% codecell

# %% markdown
# # Whois
# %% codecell
import subprocess

# %% codecell
for node in graph.nodes.match("Website"):
    site_name = node["site_name"]
    getWhois(site_name)

# %% codecell
def getWhois(site_name):
    ''' From url or sitename, download or read whois info'''

    # exception for noScrapSites list
    if site_name in NoScrapSites:
        print(site_name,": noscrap")
        return
    try:
        # if whois fiule already downloaded
        with open(site_name + ".whois", "r", encoding='utf-8') as page:
            print(site_name,": already file : ",len(page.read()))
            return(page)

    except:
        result = subprocess.run([r"C:\Program Files\WhoIs\whois64.exe",site_name],capture_output=True, text = True)
        with open(site_name + ".whois", "w", encoding='utf-8') as file:
                file.write(result.stdout)
        print(site_name,": downloaded whois: ",len(result.stdout))
        time.sleep(1)
        return(result.stdout)

# %% codecell
# Parse Whois
# %% codecell
randrange(0,len(graph.nodes.match("Website")))
# %% codecell
def randomWebNode():
    seed = randrange(0,len(graph.nodes.match("Website")))
    return(list(graph.nodes.match("Website"))[seed])
# %% codecell
node = randomWebNode()
# %% codecell
node = randomWebNode()
site_name = node["site_name"]
with open(site_name + ".whois", "r", encoding='utf-8') as page:
    whois = page.read()
print(site_name,",",len(whois),"characters,",len(whois.split("\n"))," lines,", len(whois.split(site_name))-1,"site-names")
# %% codecell
nicparselist2 = ["type:", "contact:", "address:", "country:", "phone:", "e-mail:", "registrar:", "anonymous:"]
nicparselist3 = ["Name:", "Organization:","Street:","City:", "State/Province:", "Postal Code:", "Country:", "Phone:", "Email:"]
try:
    print("domain:", whois.lower().split("domain:")[1].split("\n")[0])
    print("created", whois.lower().split("created:")[1].split("\n")[0])
except:
    pass
try:
    print("Creation Date:", whois.lower().split("creation date:")[1].split("\n")[0])
    print("Registrant Country:", whois.split("Registrant Country:")[1].split("\n")[0])
except:
    pass

for i in range(0, len(whois.split("nic-hdl:"))-1):
    print(i, "nic-hdl:", whois.split("nic-hdl:")[i+1].split("\n")[0])
    whoispart = whois.split("nic-hdl:")[i+1]
    for word in nicparselist2:
        for j in range(0, len(whoispart.split(word))-1):
            print(i, word, whoispart.split(word)[j+1].split("\n")[0])

try:
    print("Domain Name:", whois.split("Domain Name:")[1].split("\n")[0])
    whois_good = whois.split("Domain Name:")[1]
    for k in range(0, len(whois_good.split("ID:"))-1):
        whoispart = whois_good.split("ID:")[k+1]
        for word in nicparselist3:
            for j in range(0, len(whoispart.split(word))-1):
                print(k, word, whoispart.split(word)[j+1].split("\n")[0])
except:
    pass
# %% codecell
print(whois)

# %% codecell

# %% codecell

# %% codecell
i=0
for node in graph.nodes.match("Website"):
    site_name = node["site_name"]
    with open(site_name + ".whois", "r", encoding='utf-8') as page:
        whois = page.read()
    if len(whois.split("Your IP has been restricted"))>1:
        i=i+1
        print(i, site_name)


# %% codecell

# %% codecell
i=0
for node in graph.nodes.match("Website"):
    site_name = node["site_name"]
    with open(site_name + ".whois", "r", encoding='utf-8') as page:
        whois = page.read()
    if len(whois.split("Your IP has been restricted"))>1:
        i=i+1
        print(i, site_name)
        #os.remove(site_name + ".whois")
# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell
# BAD IDEA !!!!!
# %% codecell
def write_twitfriendsfile(screen_name, cursor=-1, over5000=False):
    '''Get IDs of the accounts a given account follows
    over5000 allows to collect more than 5000 friends'''

    screen_name = screen_name.lower()
    # If no file, download :
    cursor = -1
    twitfriends_file = path + "/twitter/" + screen_name + '.twitfriends'

    if not Path(twitfriends_file).is_file():
        try:
            ids = []
            r = respectful_api_request(
                'friends/ids', {'screen_name': screen_name, 'cursor': cursor})

        # todo: wait if api requests are exhausted

            if 'errors' in r.json():
                if r.json()['errors'][0]['code'] == 34:
                    return(ids)

            for item in r:
                if isinstance(item, int):
                    ids.append(item)
                elif 'message' in item:
                    print('{0} ({1})'.format(item['message'], item['code']))

            if over5000:
                if 'next_cursor' in r.json():
                    if r.json()['next_cursor'] != 0:
                        ids = ids + collect_friends(screen_name, r.json()['next_cursor'])


            with open(twitfriends_file, "w", encoding='utf-8') as file:
                    file.write(str(ids)) # Check here if good !!!
            return(twitfriends_file)

        except:
            error = str(datetime.datetime.now()) + " Error 18 : " + screen_name + " is not a twitter user"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return("")
    else:
        return(twitpro_file)


# %% codecell
len(collect_friends_fromscreen("mediapart"))
# %% codecell
# BAD IDEA  !!!!!
configurer le timer
# %% codecell
for node in graph.nodes.match("Twitter"):
    print(node['user'])
    write_twitfriendsfile(node['user'])
# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell


# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell
df2=pd.read_csv ('C:/Users/Jo/Nextcloud2/Documents/tech/11Medialist/2811NewListD2_crawls.csv')
# %% codecell
df2
# %% codecell
# get list of urls :
# %% codecell
df = pd.read_csv (path + '\\Scrap_media_sites.csv')
df
# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell


# %% codecell

# %% codecell

# %% codecell
## Liste des medias qui link Lemonde :

lemonde_n = graph.nodes.match(site_name="lemonde.fr").first()
Link_to_lemonde_r = graph.match(r_type="LINKS_TO", nodes=(None, lemonde_n))
for rel in Link_to_lemonde_r:
    print(rel['count_D2'], rel.nodes[0]['site_name'])
# %% codecell
# liste des sites que lemonde link avec le count
for r in graph.match((graph.nodes.match(site_name="lemonde.fr").first(),)):
    print(r['count_D2'], r.end_node['site_name'])
# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell

# %% codecell
graph.delete_all()
# %% codecell

# %% codecell
## TODO : check les sleeping times and debug


for url in df2['start_urls']:
    PushUrlInfoInNeo4j(url)
    PushRSUrlInfoInNeo4j(url)
# %% codecell
for url in df['url']:
    PushUrlInfoInNeo4j(url)
    PushRSUrlInfoInNeo4j(url)
# %% codecell
for url in df['url']:
    PushHypheLinksToNeo4j(url)
# %% codecell
for url in df2['start_urls']:
    PushHypheLinksToNeo4j(url)

# %% codecell
for url in df['url']:
    ImportTwitterFollowInNeo4j(url)
# %% codecell
for url in df2['start_urls']:
    ImportTwitterFollowInNeo4j(url)
# %% codecell

# %% codecell
# Pour tous les nodes twitter, download et push profile info dans neo4j
# %% codecell
len(graph.nodes.match("Twitter"))
# %% codecell
for nodes in graph.nodes.match("Twitter"):
    try:
        push_twitpro_in_Neo4j(nodes['user'])
    except:
        print(nodes['user'])
# %% codecell
# TODO :
# - extraire les @ des descriptions twitters pour lien
for node in graph.nodes.match("Twitter"):
    try:
        if "@" in node['description']:
            print(node['description'])
    except:
        pass
