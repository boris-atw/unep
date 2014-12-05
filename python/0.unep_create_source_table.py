#!/usr/bin/env python
# -*- coding: utf-8 -*-

#### Python 32 sur poste BHO (requests, psycopg2)
#### URL Source pour WebService : http://ede.grid.unep.ch/webservices/

############################################################################
###
###
### Script pour création des tables de base country, variable, data_detail
###
###
############################################################################


import sys, psycopg2

### Connexion BDD
text = open('f_bdd.txt', 'r')
try:
	myConnexionString = text.read()
finally:
	text.close()

#myConnexionString = 'host=localhost port=5463 user=postgres dbname=unep password=postgres';
try:
	myConn = psycopg2.connect(myConnexionString)
	cur = myConn.cursor()
except Exception:
	sys.exit('Erreur Connexion base de données')


### Ménage initial
sql = 'DROP TABLE IF EXISTS public.variable;'
sql += 'DROP TABLE IF EXISTS public.variable_category;'
sql += 'DROP TABLE IF EXISTS public.country;'
sql += 'DROP TABLE IF EXISTS public.data_detail;'

### Création de la table des variables
sql += """
	CREATE TABLE public.variable (
	  id integer NOT NULL,
	  name text,
	  name_short character varying(255),
	  time_period character varying(120),
	  unep_priority character varying(120),
	  units character varying(255),
	  definition text,
	  file_name character varying(255),
	  source_url character varying(255),
	  source_organization character varying(255),
	  category_id integer,
	  variable_usable integer DEFAULT 1,
	  CONSTRAINT pk_variable_id PRIMARY KEY (id)
	) WITH (OIDS=FALSE)
	;
"""
sql += 'CREATE INDEX idx_variable_category_id ON public.variable USING btree (category_id);'
sql += 'CREATE INDEX idx_variable_variable_usable ON public.variable USING btree (variable_usable);'

### Création d'une table variable_category
sql += """
	CREATE TABLE public.variable_category (
		category_id SERIAL NOT NULL
		, category_label CHARACTER VARYING(120)
		, CONSTRAINT pk_variable_category_id PRIMARY KEY (category_id)
	) WITH (OIDS=FALSE)
	;
"""

### Création de la table des pays
sql += """
	CREATE TABLE public.country(
		id integer NOT NULL
		,name character varying(120)
		,iso_2 character varying(3)
		,iso_3 character(3)
		,un integer,region character varying(120)
		,subregion character varying(120)
		,developed integer
		,least_developed integer
		,oecd integer
		,subsahara integer
		,sids integer
		,arab integer
		,CONSTRAINT pk_country_id PRIMARY KEY (id)
	) WITH (OIDS=FALSE)
	;
"""
sql += 'CREATE INDEX idx_country_iso_2 ON public.country USING btree (iso_2);'
sql += 'CREATE INDEX idx_country_iso_3 ON public.country USING btree (iso_3);'


cur.execute(sql)
myConn.commit()

######################
# Fin
######################
sys.exit('Fin exécution sans erreur')


