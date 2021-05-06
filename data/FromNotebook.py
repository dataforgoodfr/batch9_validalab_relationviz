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

scrap_path = "C:\\Users\\Jo\\Documents\\Tech\\ScrapData"
path = (scrap_path+"\\20200709")
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

NoScrapSites = ("discord.gg", "deezer.com", "snapchat.com", "facebook.com", "play.google.com", "youtube.com",
"tumblr.com", "bitly.com", "linkedin.com", "vk.com", "spotify.com", "telegram.org", "apple.co", "netvibes.com",
"google.com", "dailymotion.com", "telegram.me", "amazon.com", "plus.google.com", "mesopinions.com_fr",
"flipboard.com", "tipeee.com", "viadeo.com", "paypal.com", "soundcloud.com", "twitter.com",
"itunes.apple.com", "instagram.com", "wikipedia.org", "pinterest.com", "pearltrees.com", "ausha.co", "jemabonne.fr")

#twitter API :
api_key = "2WIJPjcDeA2VSVYxVfVLnYcVS"
api_secret_key = "bgnd3RKSF3VtqVlk0NX15uhRkPmHKqzLfhQlA3OehCEttXLOeG"

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
        #site_name = site_name + "_ro"

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

# getHypheInfoNode not copied above
# PushUrlInfoInNeo4j not copied above
# This is test for import
url = "https://www.lemonde.fr"
myUrlParse(url)
getFirstPageNode(url)
getAlexaInfoNode(url)
getFirstPageRSNode(url)
url= "https://fakirpresse.info/"
getFirstPageNode(url)
getFirstPageRSNode(url)
