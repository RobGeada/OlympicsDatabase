# OlympicsDatabase

I was frustrated that there were no easily accessible databases of Olympic results from history, so I created this tool to trawl [sports-reference](http://sports-reference.com). It generates a csv of medal results for every sport and discipline in Olympic history, not included results from the current Olympics (2016). This is a massive improvement on the extant web-based databases, simply as it greatly increases the speed and flexibility of queries.  

###Usage: Generating the Dataset
Simply place the python script and `sports.html` into a directory, then run the program. `sports.html` seeds the trawler with a list of Olympic sports as of 8/14/2016. If you just want to play with the database, you can find that [here](https://docs.google.com/spreadsheets/d/1Dsucb84gZCmRLx1LWGJy857pXL0yQwbAkayD9QCHhvs/edit?usp=sharing).

###Usage: Querying the Dataset
SQLeleton.py is a CLI that assists in generating SQL queries on the Olympic Dataset, with no restrictions on number of search parameters and parsing structure. Additionally, I've created CLI templates for some interesting searches to streamline the querying process.

##Prereqs
* PySpark
* Inquirer
