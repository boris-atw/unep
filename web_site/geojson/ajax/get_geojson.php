<?php
	function pgQuery($conn, $sql) {
		// Pas de traitement si la chaine est vide !
		if ($sql == '') return true;

		try {
			if (!$resultat = $conn->query($sql)) {
				$errInfo = $conn->errorInfo();
				throw new Exception("La requête ".$sql." a renvoyée l'erreur suivante :\nCode Erreur SQL => ".$errInfo[0]."\nCode Erreur PDOPGSQL => ".$errInfo[1]."\nErreur => ".$errInfo[2]."\n\nFin Anormale du Traitement !");
			}
		} catch (Exception $e) {
			echo $e->getMessage();
			die();
		}

		// Fetch le résultat et transmet l'objet résultat
		$resultat->setFetchMode(PDO::FETCH_ASSOC);
		$retour = array();
		while ($res = $resultat->fetch()) {$retour[] = $res;}
		if(count($retour) == 0) {
			return true;
		} else {
			return $retour;
		}
	}
	
	
	$config = array('host' => 'localhost', 'port' => '5463', 'db' => 'unep', 'user' => 'postgres', 'pwd' => 'postgres');

	// Méthode générale de connexion à une base de données par la couche d'abstration PDO
	$chaineConnexion = "pgsql:host=".$config['host'].";dbname=".$config['db'].";port=".$config['port'];	// Chaine de connexion à la base de données
	if (!$conn = new PDO($chaineConnexion, $config['user'], $config['pwd'])) {die('Erreur de Connexion');}
	
	$v = $_GET['v'];

	$sql = "
	SELECT
		row_to_json(fc) as fc
	 FROM
		(SELECT
			'FeatureCollection' As type,
			array_to_json(array_agg(f)) As features
		FROM (
			SELECT
				'Feature' As type, 
				lg.gid AS id,
				ST_AsGeoJSON(lg.geom, 2)::json As geometry
				, row_to_json(lp) As properties
			FROM country_geom_simplify As lg
			INNER JOIN (
				SELECT
					name
					, cgs.iso2
					, v.value
					, color as colour
					, ranking as ranking
				FROM
					country_geom_simplify AS cgs
					JOIN data_ranking v ON cgs.iso2 = v.iso_2
				WHERE
					v.id_variable = " . $v . "
				) As lp 
			ON lg.iso2 = lp.iso2)
		As f )
	As fc;
	";
	$res = pgQuery($conn, $sql);
	echo($res[0]['fc']);
?>