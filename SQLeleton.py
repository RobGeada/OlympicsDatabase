import pyspark
from pyspark.sql import Row
import os,sys
import inquirer

#==========SETUP SPARK====================
spark = pyspark.SparkContext("local[*]")
spark.setLogLevel("OFF")
from pyspark.sql import SQLContext
from pyspark.sql.functions import avg
sqlc = SQLContext(spark)


medals = sqlc.read.json("results.json")
medals.registerTempTable("Medals")

#=========PROGRAM FLOW HELPERS============
def wait(label):
    raw_input("waiting for user input at {}".format(label))

def printTitle():
	os.system('clear')
	print("===================================================")
	print("Welcome to")   
	print("    _____ ____    __        __     __              ")
	print("   / ___// __ \  / /  ___  / /__  / /_____  ____   ")
	print("   \__ \/ / / / / /  / _ \/ / _ \/ __/ __ \/ __ \  ")
	print("  ___/ / /_/ / / /__/  __/ /  __/ /_/ /_/ / / / /  ")
	print(" /____/\___\_\/_____|___/_/\___/\__/\____/_/ /_/   ")
	print("")
	print("==============Version 1.0====8/15/2016=============\n")


#==========U/I HELPER FUNCTIONS==================
def getSfx(n):
	suffixes = ["th","st","nd","rd","th","th","th","th","th", "th"]
	return "{}".format(n) + suffixes[n%10]

def expandSlct(dict):
	columns = ""
	columnList = []
	for i in dict["DesiredFields"]: 
		columns += i
		columns += ","
		columnList.append(i)
	return (columns[:-1],columnList)

def expandCrit(list):
	queryList = []
	for tuple in list:
		col = tuple[0]["Criteria"]
		term= tuple[1]
		queryList.append("{} LIKE '%{}%'".format(col,term))
	return queryList

def parseQueries(queries,format):
	outQuery = ""
	for i in format:
		if (i == "("):
			outQuery += "("
		elif (i == ")"):
			outQuery += ")"
		elif (i == "a"):
			outQuery += " AND "
		elif (i == "o"):
			outQuery += " OR "
		elif (i == "n"):
			outQuery += " NOT "
		else:
			outQuery += queries[int(i)-1]
	return outQuery

def getOrdering(options):
	questionOrder= [
			inquirer.List('Ordering',
				message="Which columns would you like to sort by?",
				choices=expandSlct(options)[1],
				),
			]
	return inquirer.prompt(questionOrder)["Ordering"]

#=========INTERESTING QUERIES===========
def podiumNat(gol, sil, bro,format=None):
	if format == "SQL":
		return "SELECT Year,Sport,Discipline,Gold_Medalist,Silver_Medalist,Bronze_Medalist FROM Medals WHERE"+\
		"(Gold_Nation LIKE '%{}%' AND Silver_Nation LIKE '%{}%' AND Bronze_Nation LIKE '%{}%')".format(gol,sil,bro)
	else:
		return "(Gold_Nation LIKE '%{}%' AND Silver_Nation LIKE '%{}%' AND Bronze_Nation LIKE '%{}%')".format(gol,sil,bro)

def podiumAth(gol, sil, bro,format=None):
	if format == "SQL":
		return "SELECT Year,Sport,Discipline,Gold_Medalist,Silver_Medalist,Bronze_Medalist FROM Medals WHERE "+\
		"(Gold_Medalist LIKE '%{}%' AND Silver_Medalist LIKE '%{}%' AND Bronze_Medalist LIKE '%{}%')".format(gol,sil,bro)
	else:
		return "(Gold_Medalist LIKE '%{}%' AND Silver_Medalist LIKE '%{}%' AND Bronze_Medalist LIKE '%{}%')".format(gol,sil,bro)


def uniques(nat):
	return "SELECT Year,Sport,Discipline,Gold_Nation,Silver_Nation,Bronze_Nation "+\
	"FROM Medals WHERE "+\
	podiumNat(nat[0],nat[1],nat[2]) +\
	" OR "+\
	podiumNat(nat[0],nat[2],nat[1]) +\
	" OR "+\
	podiumNat(nat[1],nat[0],nat[2]) +\
	" OR "+\
	podiumNat(nat[1],nat[2],nat[0]) +\
	" OR "+\
	podiumNat(nat[2],nat[1],nat[0]) +\
	" OR "+\
	podiumNat(nat[2],nat[0],nat[1])

def oneOlympian(name):
	return "SELECT Year,Sport,Discipline,Gold_Medalist,Silver_Medalist,Bronze_Medalist "+\
	"FROM Medals WHERE "+\
	"(Gold_Medalist LIKE '%{}%')".format(name) +\
	" OR "+\
	"(Silver_Medalist LIKE '%{}%')".format(name) +\
	" OR "+\
	"(Bronze_Medalist LIKE '%{}%')".format(name)


#========BEGIN UI MAIN LOOP=============

while 1:
	#Ask user what columns to display
	printTitle()
	questionPath = [
		inquirer.List('QueryType',
			message="What type of search would you like to perform?",
			choices=["Specific Podium (By Country)","Specific Podium (By Athlete/Team)","Medaling Countries","Specific Olympian", "Most Common Podiums (By Nation)","Most Common Podiums (By Athlete/Team)","Custom","Manual SQL"],
			),
		]
	path = inquirer.prompt(questionPath)['QueryType']
	if path=="Custom":
		#Ask user what columns to display
		printTitle()
		question1 = [
			inquirer.Checkbox('DesiredFields',
				message="Use space to select the columns to display",
				choices=["Year","Sport","Discipline","Gold_Nation","Silver_Nation","Bronze_Nation","Gold_Medalist","Silver_Medalist","Bronze_Medalist"],
				),
			]
		selectThese = inquirer.prompt(question1)
		#ask user what queries they'd like to input
		content = False
		criteria,n_crit = [],0
		while not content:
			n_crit+=1
			question2 = [
				inquirer.List('Criteria',
					message="Which column defines your {} search query?".format(getSfx(n_crit)),
					choices=["Year","Sport","Discipline","Gold_Nation","Silver_Nation","Bronze_Nation","Gold_Medalist","Silver_Medalist","Bronze_Medalist"],
					),
				]
			searchCol = inquirer.prompt(question2)
			matches = raw_input("This column should contain: ")
			criteria.append((searchCol,matches))
			userContent = raw_input("Add another critera (y/n)? ")
			if userContent != "y":
				content = True

		#determine how the queries should be parsed
		printTitle()
		visibleCols = expandSlct(selectThese)[0]
		print "Selecting columns {}...\n".format(visibleCols)
		queryList = expandCrit(criteria)

		#figure out if we need to do any weird query parsing
		if n_crit == 1:
			print "You have defined 1 search criterion:"
			finalQuery = queryList[0]
		else:
			print "You have defined {} search criteria:".format(n_crit)
			for i, item in enumerate(queryList):
				print "{}: {}".format(i+1,item)
			print "\nUse parantheses, 'a', 'n',and 'o' to specify query parsing."
			print "For example, ((1a2)o3)n4 reads ((1 AND 2) OR 3) NOT 4."
			validParse = False
			while not validParse:
				try:
					parsing = raw_input("Parsing: ")
					parsing = parsing.replace(" ","")
					finalQuery = parseQueries(queryList,parsing)
					validParse=True
				except:
					print "Invalid parsing string. Try again!\n"

		#determine the desired data ordering
		printTitle()
		print "Performing the following search:"
		print "SELECT {} FROM Medals WHERE {}".format(visibleCols,finalQuery)
		orderCol = getOrdering(selectThese)

		#perform the sql search
		targetMedal = sqlc.sql("SELECT {} FROM Medals WHERE {}".format(visibleCols,finalQuery))
		targetMedal.orderBy(orderCol).show(1000)
	
	elif path == "Specific Podium (By Country)":
		nation1=raw_input("What country won gold?   ")
		nation2=raw_input("What country won silver? ")
		nation3=raw_input("What country won bronze? ")
		visDict = {'DesiredFields': ['Year', 'Sport', 'Discipline', 'Gold_Nation', 'Silver_Nation', 'Bronze_Nation']}
		orderCol = getOrdering(visDict)
		targetMedal = sqlc.sql(podiumNat(nation1,nation2,nation3,format="SQL"))
		targetMedal.orderBy(orderCol).show(1000)

	elif path == "Specific Podium (By Athlete/Team)":
		nation1=raw_input("Who won gold?   ")
		nation2=raw_input("Who won silver? ")
		nation3=raw_input("Who won bronze? ")
		visDict = {'DesiredFields': ['Year', 'Sport', 'Discipline', 'Gold_Medalist', 'Silver_Medalist', 'Bronze_Medalist']}
		orderCol = getOrdering(visDict)
		targetMedal = sqlc.sql(podiumAth(nation1,nation2,nation3,format="SQL"))
		targetMedal.orderBy(orderCol).show(1000)

	elif path == "Medaling Countries":
		nation1=raw_input("Specify a medaling country:        ")
		nation2=raw_input("Specify another medaling country:  ")
		nation3=raw_input("Specify the last medaling country: ")
		visDict = {'DesiredFields': ['Year', 'Sport', 'Discipline', 'Gold_Nation', 'Silver_Nation', 'Bronze_Nation']}
		orderCol = getOrdering(visDict)
		targetMedal = sqlc.sql(uniques((nation1,nation2,nation3)))
		targetMedal.orderBy(orderCol).show(1000)
	
	elif path == "Specific Olympian":
		olympian=raw_input("What Olympian would you like to search for? ")
		visDict = {'DesiredFields': ['Year', 'Sport', 'Discipline', 'Gold_Medalist', 'Silver_Medalist', 'Bronze_Medalist']}
		orderCol = getOrdering(visDict)
		targetMedal = sqlc.sql(oneOlympian(olympian))
		targetMedal.orderBy(orderCol).show(1000)

	elif path == "Most Common Podiums (By Nation)":
		targetMedal = medals.groupBy("Gold_Nation","Silver_Nation","Bronze_Nation").count().orderBy("count",ascending=False)
		targetMedal.show(100)

	elif path == "Most Common Podiums (By Athlete/Team)":
		targetMedal = medals.groupBy("Gold_Medalist","Silver_Medalist","Bronze_Medalist").count().orderBy("count",ascending=False)
		targetMedal.show(100)

	elif path == "Manual SQL":
		print "Table Schema:"
		medals.printSchema()
		contentWithQuery="n"
		while contentWithQuery != "y":
			print "   Query Format: SELECT _____ FROM Medals _____ WHERE ______ LIKE _____"
			sqlQuery = raw_input("Type SQL Query:  ")
			ordering = raw_input("Define ordering: ")
			contentWithQuery = raw_input("Please take a moment to check your query. Are you sure the query is ready for submission? (y/n) ")
		targetMedal = sqlc.sql(sqlQuery)
		targetMedal.orderBy(ordering).show(1000)

	exitOut = raw_input("Search again (y/n)? ")
	if exitOut != "y":
		break
