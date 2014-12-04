#!/usr/bin/env python
# -*- coding: utf-8 -*-

#### Python 32 sur poste BHO (requests, psycopg2)
#### URL Source pour WebService : http://ede.grid.unep.ch/webservices/

############################################################################
###
###
### Script pour compléter la définition es variables depuis ede.grid.unep.ch
###
### Ce script doit être exécuté après le chargement des données variables
### (script 1.unep_load_country_var.py)
####
############################################################################


import requests, sys, json
import psycopg2

######################
# Connexion BDD
######################
def connexionBDD():
	myConnexionString = 'host=localhost port=5463 user=postgres dbname=unep password=postgres';
	try:
		myConn = psycopg2.connect(myConnexionString)
	except Exception:
		sys.exit('Erreur Connexion base de données')
	return myConn;



######################
# Ajout champ table public.variable
######################
def changeStructTableVariable(conn, cur):
	sql = 'ALTER TABLE variable ADD COLUMN units character varying(255);'
	sql += 'ALTER TABLE variable ADD COLUMN definition text;'
	sql += 'ALTER TABLE variable ADD COLUMN file_name character varying(255);'
	sql += 'ALTER TABLE variable ADD COLUMN unep_priority character varying(255);'
	sql += 'ALTER TABLE variable ADD COLUMN source_url character varying(255);'
	sql += 'ALTER TABLE variable ADD COLUMN source_organization character varying(255);'
	cur.execute(sql)
	conn.commit()


######################
# Lecture URL
######################
def getJsonFromApi(url):
	#url = 'http://ede.grid.unep.ch/api/variables'
	headers  = {'content-type': 'application/json'}
	r = requests.get(url, headers = headers)
	if r.status_code != 200:
		return False
		#sys.exit('Erreur Appel URL => Code de retour : ' + str(r.status_code))
	r.encoding = 'utf-8'
	return r.json()


######################
# Parse JSON
######################
def parseJSON(conn, cur, tName, j, idVar):
	# Construction de la requete
	for d in range(len(j)):
		row = j[d]
		sql = 'UPDATE ' + tName + ' '
		sql += 'SET '
		
		tKey = []
		tVal = []
		for k in row.keys():
			tKey.append(k)
		for v in row.values():
			tVal.append(str(v))
	
		for i in range(len(tKey)):
			# Nom du champ
			sql += '"' + tKey[i] + '" = '
			# Valeur du champ
			if tVal[i] is None:
				sql += 'null,'
			else:
				if type(tVal[i]) == str:
					v = tVal[i].replace("'", "''")
					sql += '\'' + v + '\','
				else:
					sql += str(tVal[i]) + ','
		sql = sql[0:len(sql) - 1] + ' WHERE id = ' + idVar + ';'

		#print(sql)
		cur.execute(sql)
	conn.commit()
	





######################
# Main
######################

# Ouverture handle BDD
myConn = connexionBDD()
cur = myConn.cursor()

# Création des tables from scratch
changeStructTableVariable(myConn, cur)

# Chargement du détail des variables
cur.execute('SELECT id FROM public.variable ORDER BY id;')
listVar = []
rows = cur.fetchall()
for row in rows:
	listVar.append({'idV' : str(row[0]), 'table' : 'public.variable'})

for i in listVar:
	print('Traitement Var : ' + i['idV'])
	j = getJsonFromApi('http://ede.grid.unep.ch/api/variables/' + i['idV'])
	if j:
		parseJSON(myConn, cur, i['table'], j, i['idV'])

######################
# Fin
######################
sys.exit('Fin exécution sans erreur !')

