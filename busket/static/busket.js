var Busket = new Object();
Busket.record = function(event, key, attributes) {
	// var busket_url = 'http://localhost:8001';
	var busket_url = 'http://analytics.lefora.com'

	var path = busket_url + '/api/js/1/record/?key=' + key + "&event=" + escape(event);
	// path = path.replace(/^https?:/, window.location.protocol);

	if (attributes) {
		for(var k in attributes) {
			path += "&" + k + "=" + escape(attributes[k]);
		}
	}

	var now = new Date();
	path += '&'+now.getTime();

	var userAgent = navigator.userAgent.toLowerCase();
	var msie = /msie/.test( userAgent ) && !(/opera/.test( userAgent ));
	if (!msie && document.getElementsByTagName && (document.createElementNS || document.createElement)) {
		var tag = (document.createElementNS) ? document.createElementNS('http://www.w3.org/1999/xhtml', 'script') : document.createElement('script');
		tag.type = 'text/javascript';
		tag.src = path;
		document.getElementsByTagName('head')[0].appendChild(tag);
	} else if (document.write) {
		document.write('<' + 'script type="text/javascript" src="' + path + '"><' + '/script>');
	}
};
Busket.record_pageview = function(key, attributes) {
	var referrer = (window.decodeURI)?window.decodeURI(document.referrer):document.referrer;
	var url = (window.decodeURI)?window.decodeURI(document.URL):document.URL;
	attributes = attributes || {};
	if (referrer)
		attributes["referrer"] = referrer;
	attributes["url"] = url;
	Busket.record("page_view", key, attributes);
};