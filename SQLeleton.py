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
		else:
			outQuery += queries[int(i)-1]
	return outQuery

#========BEGIN UI MAIN LOOP=============

while 1:

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
		print "\nUse parantheses, 'a', and 'o' to specify query parsing."
		print "For example, (1a2)o3 reads (1 AND 2) OR (3)."
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
	question3 = [
		inquirer.List('Ordering',
			message="Which columns would you like to sort by?",
			choices=expandSlct(selectThese)[1],
			),
		]
	orderCol = inquirer.prompt(question3)["Ordering"]

	#perform the sql search
	targetMedal = sqlc.sql("SELECT {} FROM Medals WHERE {}".format(visibleCols,finalQuery))
	targetMedal.orderBy(orderCol).show(1000)
	exitOut = raw_input("Search again (y/n)? ")
	if exitOut != "y":
		break
