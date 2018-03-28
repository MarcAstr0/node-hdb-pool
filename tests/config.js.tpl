var fs = require('fs');

var config = {
	host: 'example.host.com',
	port: 30015,
	user: 'username',
	password: 'secret',
	assertion: fs.readFileSync('path/to/signed_saml_assertion.xml', 'utf-8')
};

exports.config = config;
