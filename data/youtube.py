# %% init
from pathlib import Path
import datetime
from py2neo import Graph, Node, Relationship
from neo4j import GraphDatabase, basic_auth
import urllib.request
from bs4 import BeautifulSoup
import glob
import random, time
import os

graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

import sys
dataPath = 'C:\\Users\\Jo\\Documents\\Tech\\Atom_prj\\MyMedia-FillDB\\data' # <<<<<< hardcoded
if dataPath not in sys.path:
    sys.path.insert(0, dataPath)
from FromNotebook import myUrlParse

ytAPI_client_secrets_file_path = "C:\\Users\\Jo\\Documents\\Tech\\Notebooks\\client_secret_1054515006139-e0p34lt262qbh4af24t2po4c5ophspjo.apps.googleusercontent.com.json"

path = "C:\\Users\\Jo\\Documents\\Tech\\ScrapData\\20200709"

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

# %% Create youtube API
youtube = create_YT_API()
# %% init procedures
def write_ytpro_file(screen_name):
    """ If file doesnt exists, from youtube screen name download profile info and write on file
    returns file location or "" if user does not exists
    """

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

            return ytpro_file


        except:
            error = str(datetime.datetime.now()) + " Error 19 : " + screen_name + " is not a youtube user"
            print(error)
            with open("error_file.txt", "a", encoding='utf-8') as file:
                file.write(error +"\n")
            return ""

    else:
        return(ytpro_file)

def write_ytpro_file_fromChannelID(channelID):
    """ If file doesnt exists, from youtube screen name download profile info and write on file
    returns file location or "" if user does not exists
    """

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
                        return ""
                screen_name = screen_name.lower()
                ytpro_file = path + "/youtube/" + screen_name + '.ytprofile'

            return ytpro_file

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
        return ""

    try:
        screen_name = response["items"][0]['snippet']['customUrl']

    except:
        try:
            screen_name = response['items'][0]['snippet']['title'].lower().replace(" ", "_").replace("?", "_").replace("!", "_")
        except:
            print("this channelID has no title!!!!")
            return ""

    screen_name = screen_name.lower()
    ytpro_file = path + "/youtube/" + screen_name + '.ytprofile'

    if not Path(ytpro_file).is_file():
        try:
            with open(ytpro_file, "w", encoding='utf-8') as file:
                file.write(str(response['items'][0]))

            return(ytpro_file)
        except:
            print(screen_name, " is to weird for us. Change the name !")
            return ""

    else:
        return ytpro_file


def write_ytpro_file_fromChannelID_nocheck(channelID):
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
        return ""

    try:
        screen_name = response["items"][0]['snippet']['customUrl']

    except:
        try:
            screen_name = response['items'][0]['snippet']['title'].lower().replace(" ", "_").replace("?", "_").replace("!", "_")
        except:
            print("this channelID has no title!!!!")
            return ""

    screen_name = screen_name.lower()
    ytpro_file = path + "/youtube/" + screen_name + '.ytprofile'

    if not Path(ytpro_file).is_file():
        try:
            with open(ytpro_file, "w", encoding='utf-8') as file:
                file.write(str(response['items'][0]))

            return ytpro_file
        except:
            print(screen_name, " is to weird for us. Change the name !")
            return ""

    else:
        return(ytpro_file)



def write_yt_pro_file_from_yt_url(url):
    global ytpro_file
    screen_name=""

    CustomChannels = {"alimentationgenerale1":"UCgmfhT7laKrH4C4d3KnqyNw","numerama":"UCAz-755tH3m8_BwaluzdJwQ",
    "FondationPol%C3%A9mia":"UCMf8__hXS4X7j2eTY37Mqiw","mercato":"UCrzDtXyuSBch2u_31JJj-Dw",
    "SanteplusmagOfficiel":"UC_c2DThPTzuw3DB3fdNbZAw",
    "TouteleuropeEu":"UCvrqvZ1ABclMKlMLQKfutQA","TSAToutsurlAlg%C3%A9rie":"UC0a2EoPfE6Rh50V8ZDqeH0w",
    "ajplussaha":"UCEg4_mU2CqqtQxWwzL_uh-w","aleteiafr":"UCphfPDeHIsRXcQ_v7nIjL1Q",
    "letemps":"UCam7yYDSKFIpwAscmyzOsQQ","euractiv":"UCDCgvwyjJqZWYs6j0D2FF6g",
    "trustmyscience":"UC8gu-36a79TNI7fjJyYEzRA"}

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
            return "results"

    # Check if playlist
    if "youtube.com/playlist" in url:
        return "playlist"

    # else if link du type http://www.youtube.com/watch?v=dtUH2YSFlVU, fait rien
    if "youtube.com/watch" in url:
        return "watch"


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


    return ytpro_file

def write_ytsub_file_from_yt_profile(ytpro_file):
    """ If file doesnt exists, from previously downloaded ytpro_file, download public subscriptions  and write on file
    returns file location or "" if user does not exists or subscriptions not public
    """

    ytsub_file = ytpro_file.replace(".ytprofile",".ytsubscriptions")

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

                while nextPageToken:
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




# %% Write all yt_profile and yt_subscriptions
results = db.run("match(y:Youtube)-[:OWNED_BY]->(e:Entity) return y")
i = 0
for r in results:
    yt_n = r["y"]
    i=i+1
    if i%10 == 0:
        print(i , end=" ")
    if i%100 ==0:
        print(i)
    yt_n_py = graph.nodes.match("Youtube", url=yt_n['url']).first()
    try:
        yt_pro_file = write_yt_pro_file_from_yt_url(yt_n["url"])
        yt_n_py['yt_pro_file']=yt_pro_file
        try:
            yt_sub_file = write_ytsub_file_from_yt_profile(yt_pro_file)
            yt_n_py['yt_sub_file']=yt_sub_file
        except:
            print("error getting youtube subscriptions of: ", yt_n["url"])
    except:
        print("error getting youtube profile of: ", yt_n["url"])
    graph.push(yt_n_py)


# %% md
# YT Profile :
It works !

errors :
Error 19 : iamdieudo4 is not a youtube user
error getting youtube profile of:  https://www.youtube.com/channel/UCQ3Hc1Jief2ZiLSYfEeIiVA
DONE Error 24 : https://www.youtube.com/trustmyscience is not a valid youtube url
DONE error getting youtube subscriptions of:  https://www.youtube.com/trustmyscience
error getting youtube profile of:  https://youtube.com/channel/betterstu
error getting youtube profile of:  https://www.youtube.com/channel/UCYjbkXmibcm6JDjR7LeQk4w
error getting youtube profile of:  https://www.youtube.com/channel/UCKqTDL8eWiciu2_PjeAn9Rg
DONE error getting youtube profile of:  https://www.youtube.com/channel/UC26126xp_E65LsCwT8gYqOQ

# %% Push all profile info in db
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

for yt_n in graph.nodes.match("Youtube"):
    if (yt_n["yt_pro_file"] not in ["",None]):
        with open(yt_n["yt_pro_file"], encoding="utf8") as f:
            yt_profile = eval(f.read())
        for key in keys:
            try:
                if len(key.split('/'))==1:
                    yt_n["pro_"+keys[key]] = yt_profile[key]
                if len(key.split('/'))==2:
                    yt_n["pro_"+keys[key]] = yt_profile[key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
                if len(key.split('/'))==3:
                    yt_n["pro_"+keys[key]] = yt_profile[key.split('/')[len(key.split('/'))-3]][key.split('/')[len(key.split('/'))-2]][key.split('/')[len(key.split('/'))-1]]
            except:
                pass
    graph.push(yt_n)

# %% Imports Youtube recommendations from profile and creates Youtube node with only ChannelID is node does not exists

for yt_n in graph.nodes.match("Youtube"):
    if yt_n["yt_pro_file"] not in ["", None]:
        with open(yt_n["yt_pro_file"], encoding="utf8") as f:
            yt_profile = eval(f.read())
        try:
            for channel in yt_profile['brandingSettings']['channel']['featuredChannelsUrls']:
                nb_nodematch = len(graph.nodes.match("Youtube", pro_channelID = channel))
                if nb_nodematch > 1:
                    print("Several nodes with ID:",channel)
                else:
                    if nb_nodematch == 1:
                        target_n = graph.nodes.match("Youtube", pro_channelID = channel).first()
                    if nb_nodematch == 0:
                        target_n = Node('Youtube', channelID = channel)
                        target_n.__primarylabel__ = 'Youtube'
                        target_n.__primarykey__ = 'channelID'
                        i=2
                    RECOMMENDS = Relationship.type("RECOMMENDS")
                    graph.merge(RECOMMENDS(yt_n, target_n))
                    print(RECOMMENDS(yt_n, target_n))
        except:
            pass





# %% md
TODO:
- [X] loader les profiles des nodes qui n'ont que 'ChannelID' comme info (attention au nom des files ou des nodes à créer)
- creer les links pour subscriptions


# %% download profile and subscriptions for new youtube channels (those with only ChannelID)

results = db.run("MATCH (y:Youtube) WHERE EXISTS(y.channelID) RETURN y ")
i = 0
for r in results:
    yt_n = r["y"]
    i=i+1
    if i%10 == 0:
        print(i , end=" ")
    if i%100 ==0:
        print(i)
    # Changed code below to work with Channel
    yt_n_py = graph.nodes.match("Youtube", channelID=yt_n['channelID']).first()
    try:
        yt_pro_file = write_ytpro_file_fromChannelID_nocheck(yt_n_py['channelID'])
        yt_n_py['yt_pro_file']=yt_pro_file
        try:
            yt_sub_file = write_ytsub_file_from_yt_profile(yt_pro_file)
            yt_n_py['yt_sub_file']=yt_sub_file
        except:
            print("error getting youtube subscriptions of: ", yt_n["url"])
    except:
        print("error getting youtube profile of: ", yt_n["url"])
    if yt_n_py['user_name']==None:
        yt_n_py['user_name'] = yt_pro_file.replace(path + "/youtube/", "").replace('.ytprofile','')
    graph.push(yt_n_py)
    print(yt_n_py['user_name'])


# %% md
Here I did again "Push all profile info in db" and
"Imports Youtube recommendations from profile and creates Youtube node with only ChannelID is node does not exists"
above. Looks like working

# %% Find links and write ytlinks_file and location in nodes
#This looks like it works !

results = db.run("MATCH (y:Youtube) WHERE EXISTS(y.pro_channelID) RETURN y ")
i = 0
for r in results:
    yt_n = r['y']
    i=i+1
    if i%10 == 0:
        print(i , end=" ")
        time.sleep(random.randint(0,3))
    if i%100 ==0:
        print(i)
    if len(yt_n["pro_channelID"]) < 5:
        print("error with", yt_n["pro_channelID"])
    ytlinks_file = yt_n['yt_pro_file'].replace('ytprofile','ytlinksfile')
    if os.path.isfile('ytlinks_file'):
        pass
    else:
        chanid = yt_n["pro_channelID"]
        about_url = "https://www.youtube.com/channel/" + chanid +"/about"
        about_page = urllib.request.urlopen(about_url)
        about_soup = BeautifulSoup(about_page)

        links =[]
        for i in range(1, len(str(about_soup ).split('q=http'))):
            link_url = "http" + str(about_soup ).split('q=http')[i].split("\"")[0].replace("%2F","/").replace("%3A",":").split("%3F")[0].replace("%40","@")
            if link_url not in links:
                links.append(link_url)

        if len(links)>0:
            with open(ytlinks_file, "w", encoding='utf-8') as file:
                file.write(str(links))
            yt_n_py = graph.nodes.match("Youtube", pro_channelID=yt_n['pro_channelID']).first()
            yt_n_py['ytlinks_file'] = ytlinks_file
            graph.push(yt_n_py)
            print(yt_n_py['user_name'], ": ",len(links)," links" )


# %% md
Todo :
- regarder les myurlparse des liens de Youtube
- faire les liens dans base
- si lien vers Website existant : fait partie de Entity

# %% sandbox liens Youtube
# DO NOT EXECUTE, THIS IS FOR STATS ONLY

# Look at most linked websites
totlinks =[]
path4 = path + "\\youtube\*.ytlinksfile"
for ytlink_file in glob.glob(path4):
    with open(ytlink_file, encoding="utf8") as f:
        yt_linkfile = eval(f.read())
    for link in yt_linkfile:
        mark = 0
        try:
            for k in range(0, len(totlinks)-1):
                if myUrlParse(link) == totlinks[k]['link']:
                    totlinks[k]['number']=totlinks[k]['number']+1
                    mark = 1
        except:
            pass
        if mark == 0:
            totlinks.append({'link':myUrlParse(link), 'number':1})
        print(myUrlParse(link))


for k in range (0, len(totlinks)-1):
    if totlinks[k]['number']>3:
        print(totlinks[k]['link'])

#TODO : juste utiliser les twitter, facebook, pinterest, LinkedIn pour l'instant
# PLus tard : ajouter instagram

# %% md
Continue sandbox import links in db:
- [X] for each youtube node with link file:
- look at each link
  - [X] if link is Website, if exists : link.
  - [X] If link is twitter, facebook, pinterest, LinkedIn : if user exists : link.
  - NO check entites : if youtube links to website with entity, its probable that entity owns youtube channel
  - NO same for RS that youtube links to. To check.

 # %% Import yt links in db
RSlist = ["twitter.com", "instagram.com", "facebook.com", "play.google.com",
    "linkedin.com", "plus.google.com", "pinterest.com", "snapchat.com",
    "flicker.com", "spotify.com", "soundcloud.com", "dailymotion.com",
    "deezer.com", "tumblr.com", "chathamhouse.org", "itunes.apple.com",
    "patreon.com", "tipee.com", "twitch.tv", "vk.com", "fr.twitter.com",
    # delete those nodes in base !!!!
    "netvibes.com", "pearltrees.com", "telegram.me", "amzn.to", "t.me", "tipeee.com",
    "phantasia.be", "paypal.com"]

def testparse(url):
    from urllib import parse
    site_name = parse.urlsplit(url).netloc.replace("www.","").lower().split("&")[0].split("\u0026")[0]
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
    return(site_name)


k=l=t=fb=0
twit_count=0
results = db.run("MATCH (y:Youtube) WHERE EXISTS(y.ytlinks_file) RETURN y ")
for r in results:
    #print(r['y'])
    # open links
    with open(r['y']['ytlinks_file'], encoding="utf8") as f:
        yt_linkfile = eval(f.read())
    # si myURLparse de link pas dans RS
    for link in yt_linkfile:
        if testparse(link) in RSlist:
            if "linkedin" in link:
                try:
                    user_name = link.split("company/")[1].replace("/","")
                    li_n = graph.nodes.match("Linkedin", user_name = user_name).first()
                    if li_n is None:
                        pass
                    else:
                        youmatch = graph.nodes.match("Youtube", user_name = r['y']['user_name']).first()
                        LINKS_TO = Relationship.type("LINKS_TO")
                        graph.merge(LINKS_TO(youmatch, li_n))
                        l=l+1
                        print(l,"LI:", youmatch['user_name'], li_n['user_name'])
                except:
                    pass

            if "facebook" in link:
                user_name = link.split("/")[3].lower()
                fb_n= graph.nodes.match("Facebook", user_name = user_name).first()
                if fb_n is None:
                    pass
                else:
                    youmatch = graph.nodes.match("Youtube", user_name = r['y']['user_name']).first()
                    LINKS_TO = Relationship.type("LINKS_TO")
                    graph.merge(LINKS_TO(youmatch, fb_n))
                    fb=fb+1
                    print(fb,"FB:", youmatch['user_name'], fb_n['user_name'])

            if "twitter" in link:
                user_name = link.split("/")[3].lower()
                tw_n= graph.nodes.match("Twitter", user_name = user_name).first()
                if tw_n is None:
                    #print(user_name)
                    pass
                else:
                    youmatch = graph.nodes.match("Youtube", user_name = r['y']['user_name']).first()
                    LINKS_TO = Relationship.type("LINKS_TO")
                    graph.merge(LINKS_TO(youmatch, tw_n))
                    t=t+1
                    print(t,"TW:", youmatch['user_name'], tw_n['user_name'])
        else:
            web_match = graph.nodes.match("Website", site_name = testparse(link)).first()
            if web_match is None:
                if len(testparse(link).split("."))>2:
                    web_match = graph.nodes.match("Website", site_name =testparse(link).split(".")[1] + "." + testparse(link).split(".")[2]).first()
                if web_match is None:
                    #with open(path + "\\links_node_notfound.csv", "a", encoding='utf-8') as file:
                        #file.write(r['y']['pro_channelID'] +";"+ r['y']['user_name'] +";"+ "LINKS_TO" +";"+ link +";"+testparse(link)+"\n")
                # print(r['y']['user_name'], "LINKS_TO", link)
                    continue
            #with open(path + "\\links_node_found.csv", "a", encoding='utf-8') as file:
                #file.write(r['y']['pro_channelID'] +";"+ r['y']['user_name'] +";"+ "LINKS_TO" +";"+ link + ";" + testparse(link) + "\n")
                # print(r['y']['pro_channelID'],r['y']['user_name'], "LINKS_TO", link)
            youmatch = graph.nodes.match("Youtube", user_name = r['y']['user_name']).first()
            LINKS_TO = Relationship.type("LINKS_TO")
            graph.merge(LINKS_TO(youmatch, web_match))
            print(k,"Web:", youmatch['user_name'], web_match['site_name'])
            k=k+1

print("Linked ",k,"websites,",l,"LI,",fb,"FB,",t,"TW")


# %% md
looks like it worked.
Only links linking to existing node where imported.
And many errors due to code. Should look at that again (especially links_node_notfound.csv in scrapdata)

# %% import subscription files as FOLLOWS relationship and create new youtube channel if does not exists

i=k=0
results = db.run("MATCH (y:Youtube) WHERE EXISTS(y.yt_sub_file) return y ")
for r in results:
    if r['y']['yt_sub_file']=="":
        continue
    source_n = graph.nodes.match("Youtube", pro_channelID = r['y']['pro_channelID']).first()
    # open links
    with open(r['y']['yt_sub_file'], encoding="utf8") as f:
        yt_subfile = eval(f.read())
        #print(yt_subfile)
        for sub in yt_subfile:
            sub_chanID = sub['snippet']['resourceId']['channelId']
            sub_n = graph.nodes.match("Youtube", pro_channelID = sub_chanID).first()
            if sub_n is None:
                sub_n = graph.nodes.match("Youtube", channelID = sub_chanID).first()
                if sub_n is None:
                    sub_n = Node('Youtube', channelID = sub_chanID)
                    sub_n.__primarylabel__ = 'Youtube'
                    sub_n.__primarykey__ = 'channelID'
                    sub_n['title'] = sub['snippet']['title']
                    sub_n['description'] = sub['snippet']['description']
                    sub_n['thumbnail'] = sub['snippet']['thumbnails']['default']['url']
                    i=i+1

            FOLLOWS = Relationship.type("FOLLOWS")
            graph.merge(FOLLOWS(source_n, sub_n))
            print(source_n['user_name'], sub_n['title'])
            k=k+1
print(k, "FOLLOWS links created and", i,"new youtube nodes")













# %% BACKUP DOES NOT WORK !!!!
def write_yt_links_file_from_ytpro_file(ytpro_file):
    '''
    '''

    ytlinks_file = ytpro_file.replace(".ytprofile",".ytlinks")

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
