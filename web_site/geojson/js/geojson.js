// Define global variables which can be used in all functions
var map, vectors;

// Function called from body tag
function init(){
    var lon = 5;
    var lat = 15;
    var zoom = 2;

    var context = {
        getColour: function(feature) {
            return feature.attributes["colour"];
        }
    };

    var template = {
		fillOpacity: 0.9,
        strokeColor: "#555555",
        strokeWidth: 0,
		strokeOpacity : 0,
        fillColor: "${getColour}"
	};
	
	var templateFondCarte = {
		strokeColor: "#333333",
		strokeWidth: 1,
		fillOpacity: 0
	};
	

    style = new OpenLayers.Style(template, {context: context});
	styleFondCarte = new OpenLayers.Style(templateFondCarte);
    styleMap = new OpenLayers.StyleMap({'default': style});
	styleMapFondCarte = new OpenLayers.StyleMap({'default': styleFondCarte});
	var options = {
        numZoomLevels: 5,
        controls: []  // Remove all controls
    };

    // Create a new map with options defined above
    map = new OpenLayers.Map( 'olmap', options );

    // Create polygon layer as vector features
    // http://dev.openlayers.org/docs/files/OpenLayers/Layer/Vector-js.html
    fondCarte = new OpenLayers.Layer.GML( "Fond de Carte", "data/fond_carte.json", //"data/choropleth.json",
                                        { format: OpenLayers.Format.GeoJSON,
                                          styleMap: styleMapFondCarte,
                                          isBaseLayer: true,
                                          projection: new OpenLayers.Projection("EPSG:4326"),
                                          attribution: "ADN Techno" } );
    map.addLayer(fondCarte);
    map.setCenter(new OpenLayers.LonLat(lon, lat), zoom);

    // Add map controls: http://dev.openlayers.org/docs/files/OpenLayers/Control-js.html
    map.addControl(new OpenLayers.Control.LayerSwitcher());
    map.addControl(new OpenLayers.Control.MousePosition());
    map.addControl(new OpenLayers.Control.MouseDefaults());
    map.addControl(new OpenLayers.Control.PanZoomBar());
	
	// Bind l'évènement de changement de dataLayer
	$('#var').change(function() {
		var selVar = $('#selVar').val();
		if (selVar == '-1') {map.removeLayer('dataLayer'); return true;}
		loadLayer(selVar);
	});
}


function loadLayer(idVariable) {
	$.ajax({
		url : 'ajax/get_var_detail.php',
		data : {v:idVariable},
		type : 'GET',
		dataType: "json"
		
	}).done(function(json) {
		//alert(json.min);
		$('#valMin').html(json.min);
		$('#valMax').html(json.max);
		$('#valMean').html(json.moyenne);
		$('#rang_total').html(json.nbtotal);
	});
	l = map.getLayersByName('dataLayer');
	if (l.length > 0) {
		map.removeLayer(l[0]);
	}
	layer = new OpenLayers.Layer.Vector("dataLayer", {
		projection : new OpenLayers.Projection("EPSG:4326"),
		strategies: [new OpenLayers.Strategy.Fixed()],
		protocol : new OpenLayers.Protocol.HTTP({
			url : 'ajax/get_geojson.php?v=' + idVariable,
			format: new OpenLayers.Format.GeoJSON()
		}),
		styleMap: styleMap
	});
    var options = {
        hover: true,
        onSelect: serialize
    };
	map.addLayer(layer);
    var select = new OpenLayers.Control.SelectFeature(layer, options);
    map.addControl(select);
    select.activate();
}

function serialize() {
    document.getElementById("country").innerHTML = layer.selectedFeatures[0].attributes["name"] + ' - (' + layer.selectedFeatures[0].attributes['iso2'] + ')';
    document.getElementById("valeur").innerHTML = layer.selectedFeatures[0].attributes["value"];
    document.getElementById("rang").innerHTML = layer.selectedFeatures[0].attributes["ranking"];
}