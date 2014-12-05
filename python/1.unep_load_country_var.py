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
	text = open('f_bdd.txt', 'r')
	try:
		myConnexionString = text.read()
		myConn = psycopg2.connect(myConnexionString)
	except Exception:
		sys.exit('Erreur Connexion base de données')
	finally:
		text.close()
	return myConn;


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




######################
# Main
######################

# Ouverture handle BDD
myConn = connexionBDD()
cur = myConn.cursor()

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


