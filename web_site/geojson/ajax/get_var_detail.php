<?php
	function pgQuery($conn, $sql) {
		// Pas de traitement si la chaine est vide !
		if ($sql == '') return true;

		try {
			if (!$resultat = $conn->query($sql)) {
				$errInfo = $conn->errorInfo();
				throw new Exception("La requ�te ".$sql." a renvoy�e l'erreur suivante :\nCode Erreur SQL => ".$errInfo[0]."\nCode Erreur PDOPGSQL => ".$errInfo[1]."\nErreur => ".$errInfo[2]."\n\nFin Anormale du Traitement !");
			}
		} catch (Exception $e) {
			echo $e->getMessage();
			die();
		}

		// Fetch le r�sultat et transmet l'objet r�sultat
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

	// M�thode g�n�rale de connexion � une base de donn�es par la couche d'abstration PDO
	$chaineConnexion = "pgsql:host=".$config['host'].";dbname=".$config['db'].";port=".$config['port'];	// Chaine de connexion � la base de donn�es
	if (!$conn = new PDO($chaineConnexion, $config['user'], $config['pwd'])) {die('Erreur de Connexion');}
	
	$v = $_GET['v'];

	$sql = 'SELECT min(value), max(value), round(avg(value), 1) as moyenne, count(*) as nbtotal FROM public.data_ranking WHERE id_variable = ' . $v .';';
	
	$res = pgQuery($conn, $sql);
	/*var_dump($res);*/
	echo(json_encode($res[0]));