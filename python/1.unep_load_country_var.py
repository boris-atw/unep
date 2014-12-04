#!/usr/bin/env python
# -*- coding: utf-8 -*-

#### Python 32 sur poste BHO (requests, psycopg2)
#### URL Source pour WebService : http://ede.grid.unep.ch/webservices/

############################################################################
###
###
### Script pour chargement des variables et country depuis ede.grid.unep.ch
###
### Ce script doit être exécuté avant le chargement des données de détail
### (script unep_load_data.py)
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
# Création table country et variable
######################
def createTableFromScratch(conn, cur):
	sql = 'DROP TABLE IF EXISTS public.variable;'
	sql += 'DROP TABLE IF EXISTS public.country;'
	sql += 'CREATE TABLE country(id integer NOT NULL,name character varying(120),iso_2 character varying(3),iso_3 character(3),un integer,region character varying(120),subregion character varying(120),developed integer,least_developed integer,oecd integer,subsahara integer,sids integer,arab integer,CONSTRAINT pk_country_id PRIMARY KEY (id)) WITH (OIDS=FALSE);'
	sql += 'CREATE TABLE variable (id integer NOT NULL,name text,name_short character varying(255),time_period character varying(120),CONSTRAINT pk_variable_id PRIMARY KEY (id)) WITH (OIDS=FALSE);'
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
	# Vide la table cible
	cur.execute('TRUNCATE ' + tName + ' CASCADE;');

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




######################
# Main
######################

# Ouverture handle BDD
myConn = connexionBDD()
cur = myConn.cursor()

# Création des tables from scratch
createTableFromScratch(myConn, cur)

# Chargement des tables de base (variable, country)
listAction = [{'url' : 'http://ede.grid.unep.ch/api/variables', 'table' : 'public.variable'}, {'url' : 'http://ede.grid.unep.ch/api/countries', 'table' : 'public.country'}]

for i in listAction:
	print('Traitement de la table ' + i['table'])
	j = getJsonFromApi(i['url'])
	parseJSON(myConn, cur, i['table'], j)
	print('Effectué avec succès !\n')


### Mise à jour pour le sud-soudan
cur.execute('UPDATE country SET iso_2 = \'SS\' WHERE iso_2 = \'SD\' AND iso_3 = \'SSD\';')
myConn.commit()


######################
# Fin
######################
sys.exit('Fin exécution sans erreur')


