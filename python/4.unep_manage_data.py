#!/usr/bin/env python
# -*- coding: utf-8 -*-

#### Python 32 sur poste BHO (requests, psycopg2)
#### URL Source pour WebService : http://ede.grid.unep.ch/webservices/

############################################################################
###
###
### Script pour management des valeurs reçues depuis unep.grid.ch
###
### Ce script doit être exécuté après le chargement des données de détail
### (script unep_load_data.py)
####
############################################################################


import sys, psycopg2


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


