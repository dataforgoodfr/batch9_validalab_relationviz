# %% init
from py2neo import Graph, Node, Relationship
from neo4j import GraphDatabase, basic_auth
import subprocess
import random, time


graph = Graph("bolt://localhost:7687", auth=("neo4j", "Password"))
driver = GraphDatabase.driver('bolt://localhost',auth=basic_auth("neo4j", "Password"))
db = driver.session()

whois_path = "C:\\Users\\Jo\\Documents\\Tech\\ScrapData\\20200709\\whois\\"

# %% fetch whois files
i = 0
results = db.run("MATCH (n:Website)-[:OWNED_BY]->(:Entity) RETURN n.site_name ")
for r in results:
    site_name = r['n.site_name']
    i = i+1
    #print(site_name)
    try:
        # if whois fiule already downloaded
        with open(whois_path + site_name + ".whois", "r", encoding='utf-8') as page:
            print(site_name,": already file : ",len(page.read()))


    except:
        result = subprocess.run([r"C:\Program Files\WhoIs\whois64.exe",site_name],capture_output=True, text = True)
        with open(whois_path + site_name + ".whois", "w", encoding='utf-8') as file:
                file.write(result.stdout)
        sleep_time = random.randint(0,5)
        print(i, site_name,": downloaded whois: ",len(result.stdout), ". Sleep for ",sleep_time)
        time.sleep(sleep_time)

        web_n = graph.nodes.match("Website", site_name = site_name).first()
        web_n['whois_file']= whois_path + site_name + ".whois"

        graph.push(web_n)



# %% sandbox





def downloadWhois(site_name):
    ''' From url or sitename, download or read whois info'''

    # exception for noScrapSites list
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


# %% test script NOT WORKING

from bs4 import BeautifulSoup
import urllib2, sys
import urllib.request

about_url = "https://www.youtube.com/channel/" + chanid +"/about"
about_page = urllib.request.urlopen(about_url)
about_soup = BeautifulSoup(about_page)

url="https://who.is/whois/lemonde.fr"


htmldata=urllib.request.urlopen(url).read()
class_name="rawWhois"  # Class For Extraction

try:
    import lxml
    parse=BeautifulSoup(htmldata,'lxml')
    print("[*] Using lxml Module For Fast Extraction")
except:
    parse=BeautifulSoup(htmldata, "html.parser")
    print("[*] Using built-in Html Parser [Slow Extraction. Please Wait ....]")


div class="col-md-12 queryResponseBodyValue">

try:
    container=parse.findAll("div",{'class':"row"})
    sections=container[1:]
    for section in sections:
        extract=section.findAll('div')
        heading=extract[0].text
        print('\n[ ',heading,' ]')
        for i in extract[1].findAll('div'):
            fortab='\t|'
            for j in i.findAll('div'):
                fortab=fortab+'----'
                line=j.text.replace('\n', ' ')
                print(fortab,'>', line)
