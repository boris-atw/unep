#!/usr/bin/env python
# -*- coding: utf-8 -*-

#### Python 32 sur poste BHO (requests, psycopg2)
#### URL Source pour WebService : http://ede.grid.unep.ch/webservices/

############################################################################
###
###
### Script pour chargement des valeurs de base depuis ede.grid.unep.ch
###
### Les tables country et variable doivent avoir été générées précedemment
### en utilisant le script unep_load_country_var.py
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
# Création table data_detail
######################
def createTableDetail(conn, cur):
	sql = 'DROP TABLE IF EXISTS public.data_detail;'
	sql += 'CREATE TABLE public.data_detail("iso-2" character varying(3), year integer, value numeric, id_variable integer,CONSTRAINT pk_data_detail PRIMARY KEY ("iso-2", year, id_variable)) WITH (OIDS=FALSE);'
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
def parseJSON(conn, cur, tName, j, idVar):
	# Construction de la requete
	for d in range(len(j)):
		isNullRow = False
		sql = 'INSERT INTO ' + tName + ' ('
		for k in j[d].keys():
			sql += '"' + k + '",'
		sql += '"id_variable") VALUES ('
		for v in j[d].values():
			if v is None:
				sql += 'null,'
				isNullRow = True
			else:
				if type(v) == str:
					v = v.replace("'", "''")
					sql += '\'' + v + '\','
				else:
					sql += str(v) + ','
		#sql = sql[0:len(sql) - 1] + ');'
		sql += idVar + ');'

		if isNullRow == False:
			cur.execute(sql)
	conn.commit()




######################
# Main
######################

# Ouverture handle BDD
myConn = connexionBDD()
cur = myConn.cursor()

# Création des tables de détail des valeurs
createTableDetail(myConn, cur)


# Chargement du détail des valeurs (par variable et par country)
cur.execute('SELECT id FROM public.variable WHERE variable_usable = 1 ORDER BY id;')
listVar = []
rows = cur.fetchall()
for row in rows:
	listVar.append({'idV' : str(row[0]), 'table' : 'public.data_detail'})

for i in listVar:
	print('Traitement Var : ' + i['idV'])
	j = getJsonFromApi('http://ede.grid.unep.ch/api/countries/variables/' + i['idV'])
	parseJSON(myConn, cur, i['table'], j, i['idV'])

# Création de la table data_last_year
sql = """
	CREATE TABLE public.data_last_year AS
		SELECT
			foo.iso_2
			, c.iso_3
			, c.name as country_name
			, foo.id_variable
			, v.category_id
			, foo.year as last_year_value
			, d.value
		FROM (
			SELECT
				"iso-2" as iso_2
				, id_variable
				, max(year) as year
			FROM
				public.data_detail
			GROUP BY
				"iso-2"
				, id_variable
			) AS foo
			JOIN data_detail d ON (d."iso-2" = foo.iso_2 AND d.id_variable = foo.id_variable AND d.year = foo.year) 
			JOIN country c ON foo.iso_2 = c.iso_2
			JOIN variable v ON foo.id_variable = v.id
;"""

sql += 'ALTER TABLE data_last_year ADD CONSTRAINT pk_data_last_year PRIMARY KEY(iso_2, id_variable);'
sql += 'CREATE INDEX idx_data_last_year_category_id ON data_last_year (category_id ASC NULLS LAST);'
cur.execute(sql)
myConn.commit()


# Update pour Antarctique
cur.execute('UPDATE country SET iso_2 = \'AQ\', iso_3 = \'ATA\' WHERE name = \'Antarctic\';')
cur.execute(sql)
myConn.commit()


######################
# Fin
######################
sys.exit('Fin exécution sans erreur !')


