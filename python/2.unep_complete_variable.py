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
		sys.exit('Erreur Appel URL => Code de retour : ' + str(r.status_code))
	r.encoding = 'utf-8'
	return r.json()


######################
# Parse JSON
######################
def parseJSON(conn, cur, tName, j):
	# Construction de la requete
	for d in range(len(j)):
		sql = 'INSERT INTO ' + tName + ' ('
		for k in j[d].keys():
			sql += k + ','
		sql = sql[0:len(sql) - 1] + ') VALUES ('
		for v in j[d].values():
			if v is None:
				sql += 'null,'
			else:
				v = v.replace("'", "''")
				sql += '\'' + v + '\','
		sql = sql[0:len(sql) - 1] + ');'
		cur.execute(sql)
	conn.commit()

