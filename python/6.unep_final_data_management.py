#!/usr/bin/env python
# -*- coding: utf-8 -*-

#### Python 32 sur poste BHO (requests, psycopg2)
#### URL Source pour WebService : http://ede.grid.unep.ch/webservices/

############################################################################
###
###
### Script pour management final des valeurs (aggregats, ranking, etc...)
###
####
############################################################################



import sys, psycopg2


### Connexion BDD
text = open('f_bdd.txt', 'r')
try:
	myConnexionString = text.read()
finally:
	text.close()

try:
	myConn = psycopg2.connect(myConnexionString)
	cur = myConn.cursor()
except Exception:
	sys.exit('Erreur Connexion base de données')


### Suppression de la table public.data_ranking si déjà existante
sql = 'DROP TABLE IF EXISTS public.data_ranking CASCADE;'

### Création de la table public.data_ranking
sql += """
	CREATE TABLE public.data_ranking AS
		SELECT
			*
			, ntile(10) OVER (PARTITION BY id_variable ORDER BY id_variable, value) AS ranking
		FROM
			data_last_year
		ORDER BY
			id_variable, value
"""

### Création des index
sql += 'ALTER TABLE data_ranking ADD CONSTRAINT pk_data_ranking PRIMARY KEY(iso_2, id_variable);'

### Exécution de la requete et commit général
cur.execute(sql)
myConn.commit()

### Fin Exécution
sys.exit('Fin avec succès')