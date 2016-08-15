from __future__ import print_function
import os
import csv
import urllib2

'''
I swear to god sport-reference.com if you change your html format I'll jump
off a bridge but anyway thanks a lot for the free data
'''

#===========GET INITAL DATA===========
cwd = os.getcwd()
f = open(cwd+"/sports.html","r")
sportsHTML = f.read()
f.close()

sportsHTML = sportsHTML.split('<td align="left" ><a href="')
sportsHTML = [x for x in sportsHTML if (x[0:20]!='/olympics/summer/">S' and x[0:20]!='/olympics/winter/">W')]
sportsHTML = sportsHTML[1:]

#==========GET HTML FOR INDIVIDUAL SPORTS=============
if not os.path.isdir(cwd+"/indivSport"):
    
    os.mkdir(cwd+"/indivSport")
    n,total = 0,len(sportsHTML)
    for i in sportsHTML:
        print("\rRetrieving data for sport {} of {} from sports-reference.com...".format(n,total),end="")
        n+=1
        url = i[0:20]
        response = urllib2.urlopen('http://www.sports-reference.com/'+url)
        html = response.read()
        filename = url.replace("/","")
        sportF = open(cwd+"/indivSport/"+filename+".html","w")
        sportF.write(html)
        sportF.close()
    print("\rFinished retrieving data for {} sports from sports-reference.com!".format(total))

    #PARSE INDIVIDUAL SPORTS DATA
    sports = os.listdir(cwd+"/indivSport/")
    sports = [cwd+"/indivSport/"+x for x in sports]
    indiv_HTML_list = []
    for sport in sports:
        f = open(sport,"r")
        indivHTML = f.read()
        indivHTML = indivHTML.split('<td align="left" ><a href="')
        indivHTML = [x.split(">")[0] for x in indivHTML if x[0:17]=="/olympics/sports/"]
        indiv_HTML_list.extend(indivHTML)
        f.close()
else:
    print("Accessing cached sport data...")

#==========GET HTML FOR INDIVIDUAL DISCIPLINES=============
if not os.path.isdir(cwd+"/indivDisc"):
    os.mkdir(cwd+"/indivDisc")
    #download all discipline data
    n,total = 0,len(indiv_HTML_list)
    for url in indiv_HTML_list:
        print("\rRetrieving data for discipline {} of {} from sports-reference.com...".format(n,total),end="")
        n+=1
        url = url[:-1]
        response = urllib2.urlopen('http://www.sports-reference.com/'+url)
        html = response.read()
        filename = url.replace("/","")[:-1]
        sportF = open(cwd+"/indivDisc/"+filename,"w")
        sportF.write(html)
        sportF.close()
    print("\rFinished retrieving data for {} disciplines from sports-reference.com!".format(total))
else:
    print("Accessing cached discipline data...")

#===========PARSE INDIVIDUAL DISCIPLINE HTML DATA=========
print("Parsing discipline data...")
results=[]
allDisc = os.listdir(cwd+"/indivDisc/")
allDisc = [cwd+"/indivDisc/"+x for x in allDisc]
indiv_disc_list = []

def getName(abbrev):
    names = ["Aeronautics","Alpine Skiing","Alpinism","Archery","Art Competitions","Athletics","Badminton","Baseball","Basketball","Basque Pelota","Beach Volleyball","Biathlon","Bobsleigh","Boxing","Canoeing","Cricket","Croquet","Cross Country Skiing","Curling","Cycling","Diving","Equestrianism","Fencing","Figure Skating","Football","Freestyle Skiing","Golf","Gymnastics","Handball","Hockey","Ice Hockey","Jeu De Paume","Judo","Lacrosse","Luge","Military Ski Patrol","Modern Pentathlon","Motorboating","Nordic Combined","Polo","Racquets","Rhythmic Gymnastics","Roque","Rowing","Rugby","Sailing","Shooting","Short Track Speed Skating","Skeleton","Ski Jumping","Snowboarding","Softball","Speed Skating","Swimming","Synchronized Swimming","Table Tennis","Taekwondo","Tennis","Trampolining","Triathlon","Tug-Of-War","Volleyball","Water Polo","Weightlifting","Wrestling"]
    abbrevs=["AER","ASK","ALP","ARC","ART","ATH","BDM","BSB","BAS","PEL","BVO","BIA","BOB","BOX","CAN","CRI","CRO","CCS","CUR","CYC","DIV","EQU","FEN","FSK","FTB","FRS","GOL","GYM","HAN","HOK","ICH","JDP","JUD","LAX","LUG","MSP","MOP","MTB","NCO","POL","RAQ","RGY","ROQ","ROW","RUG","SAI","SHO","STK","SKE","SKJ","SNB","SOF","SSK","SWI","SYN","TTN","TKW","TEN","TMP","TRI","TOW","VOL","WAP","WLT","WRE"]
    return names[abbrevs.index(abbrev)]

#iterate through disciplines
for disc in allDisc:
    f = open(disc,"r")
    indivDisc = f.read().split('<tr valign="top"  class="">')
    indivDisc = [x.split("</tr>")[0] for x in indivDisc if x.strip()[0:37]=='<td align="right" ><a href="/olympics']
    
    #iterate through individual olympics
    for olym in indivDisc:
        event= olym.split("href=")[1].split(">")[0][18:-6].split("/")
        weird = False
        #because the html format for 1956 equestrian is weird
        if event[0] == "ian":
            event = event[1:4]
        if event[0] == "2012" and event[1]=="SWI" and event[2]=="mens-200-metres-freestyle":
            weird = True
        olym = olym.split('<td align="left"')[1:4]
        olym = [x.split(" title=") for x in olym]
        olymResults = []
        
        #iterate through individual medalists
        for medal in olym:
            if len(medal)>1:
                medalCountry,medalAthList = [],[]
                for medallist in medal[1:]:
                    medalCountry.append(medallist.split(" height")[0][1:-1])
                    medalAth = medallist.split('html">')
                    if len(medalAth)<2:
                        medalAthList.append(medalAth[0].split(">")[2].split("<")[0])
                    else:
                        medalAthList.append(medalAth[1].split("<")[0])
                if len(medalAthList)==1:
                    medalCountry = medalCountry[0]
                    medalAthList = medalAthList[0]
                medal = (medalAthList,medalCountry)
            else:
                medal = (None,None)
            olymResults.append(medal)
        results.append((event[0],getName(event[1]),event[2],olymResults[0][0],olymResults[1][0],olymResults[2][0],olymResults[0][1],olymResults[1][1],olymResults[2][1]))
        
#==========WRITE RESULTS====================   
print("Writing data...")        
f = open(cwd+"/results.csv","w")
writer = csv.writer(f)
writer.writerow(("Year","Sport","Discipline","Gold_Medalist","Silver_Medalist","Bronze_Medalist","Gold_Nation","Silver_Nation","Bronze_Nation"))
for values in results:
    writer.writerow(values)
f.close()        
    