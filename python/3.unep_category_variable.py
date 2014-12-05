#!/usr/bin/env python
# -*- coding: utf-8 -*-

#### Python 32 sur poste BHO (requests, psycopg2)
#### URL Source pour WebService : http://ede.grid.unep.ch/webservices/

############################################################################
###
###
### Script pour compléter la définition des variables depuis ede.grid.unep.ch
### en permettant de définir les catégories de variable (unep_priority)
###
### Ce script doit être exécuté après le chargement des données variables
### (script 1.unep_load_country_var.py)
####
############################################################################


import sys, psycopg2

### Connexion BDD
myConnexionString = 'host=localhost port=5463 user=postgres dbname=unep password=postgres';
try:
	myConn = psycopg2.connect(myConnexionString)
	cur = myConn.cursor()
except Exception:
	sys.exit('Erreur Connexion base de données')

### Insertion des catégories par recherche des différents labels
sql = 'INSERT INTO public.variable_category (category_label) SELECT distinct unep_priority FROM public.variable ORDER BY unep_priority;'

### Mise à jour de l'identifiant catégorie dans la table public.variable
sql += 'UPDATE public.variable AS v SET category_id = (SELECT category_id FROM public.variable_category WHERE category_label = v.unep_priority);'

### Suppression de la colonne unep_prority, inutile dorénavant !
sql += 'ALTER TABLE public.variable DROP COLUMN unep_priority CASCADE;'

### Suppression des valeurs de category nulle
sql += 'UPDATE public.variable SET variable_usable = 0 WHERE category_id ISNULL;'
sql += 'UPDATE public.variable SET variable_usable = 0 WHERE category_id = (SELECT category_id FROM public.variable_category WHERE category_label = \'None\');'
sql += 'DELETE FROM public.variable_category WHERE category_label ISNULL;'
sql += 'DELETE FROM public.variable_category WHERE category_label = \'None\';'

### Exécution de la requete et commit général
cur.execute(sql)
myConn.commit()

### Fin Exécution
sys.exit('Fin avec succès')