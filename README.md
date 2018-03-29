HDB connection pool
=============
SAP HANA connection pool based on [node-hdb](https://github.com/SAP/node-hdb) driver and [node-pool](https://github.com/coopernurse/node-pool) generic pool.

The HDB connection pool is able to connect with:
* username + password
* SAML bearer assertion
* username + SAP HANA session cookie

The connection pool can be used in **standalone mode**, or with a webserver in **HTTP mode**.

## Installation

    $ npm install node-hdb-pool

## Getting started

This is a very simple example how to configure the pool:
```javascript
var hdbPool = require('node-hdb-pool');

var hpool = hdbPool.createPool({
	host: 'hostname',
	env: 'PROD',
	port: 30015,
	user: 'user',
	password: 'secret',
	maxPoolSize: 5,
	databaseName: 'DB'
});
```

And how to use it in **standalone mode**:
```javascript
if (hpool) {
    var sql = '...';    // can be either DDL or DML
    var prepArgs = [];	// prepared statement arguments
    hpool.exec(sql, prepArgs, function(err, result) {
        if (err) {
            // handle error
            return;
        }

        // use result
    }
}
```

And how to use it in **HTTP mode**:
```javascript
var express = require('express'),
    app = express();

app.get('/insertrow', function(req, res) {
	if (hpool) {
	    var sql = 'INSERT INTO ... VALUES (?, ?, ?)';  // can be either DDL or DML
        var prepArgs = ['val1', 'val2', 'val3'];	// prepared statement arguments
	    hpool.exec(sql, prepArgs, req, res, function(err) {
			if (err) {
				console.log(err);
				res.send('ERROR');
			}
			// if no error happened the result will be automatically sent back to the client. (in case of insert, the result contains the number of affected rows)
		});
	}
	else {
		console.log('No pool is available!');
		res.send('No pool is available!');
	}
});

app.listen(3000, function () {
	console.log('Example app listening on port 3000!');
});
```

And how to use it in **HTTP mode** with **streaming**:
```javascript
var express = require('express'),
    app = express();

app.get('/csvdata', function(req, res) {
	if (hpool) {
	    var sql = '...';
        var prepArgs = [];	// prepared statement arguments
	    hpool.stream(sql, prepArgs, req, [hdbPool.createCsvStringifier, res], function(err) {
			if (err) {
				console.log(err);
				res.send('ERROR');
			}
		});
	}
	else {
		console.log('No pool is available!');
		res.send('No pool is available!');
	}
});

app.listen(3000, function () {
	console.log('Example app listening on port 3000!');
});
```

And how to use it with **socket.io**:
```javascript
var express = require('express'),
    http = require('http'),
    app = express();

var HttpServer = http.createServer(app).listen(3000),
    SocketIO = require('socket.io')(HttpServer);

SocketIO.on('connection', function(sock) {
    var req = sock.request;
    
    // pass socket-events to the underlying request (hdb-pool registers events on the request like close, error)
    sock.on('disconnect', function() {
		req.emit('close');
	});
    sock.on('error', function(err) {
		req.emit('error', err);
	});

    sock.on('insertrow', function(payload) {
        if (hpool) {
            var sql = 'INSERT INTO ... VALUES (?, ?, ?)';  // can be either DDL or DML
            var prepArgs = payload;    // prepared statement arguments can be sent by the client
            
            // the 4th argument (response) needs to be null
            hpool.exec(sql, prepArgs, req, null, function(err) {
                if (err) {
                    console.log(err);
                    // emit message if the client needs to know about this error
                }
                // emit message if the client needs to know if the insert was successful
            });
        }
        else {
            console.log('No pool is available!');
            // emit message if the client needs to know about this error
        }
    });
});
```

Config parameter details:
* `host`: hostname. Required.
* `port`: port. Required.
* `env`: short symbolic name of the environment, e.g. SAND, PROD, etc. Optional, if not specified `hostname` will be used.
* `user`: username for SAP HANA Database. Optional when `assertion` is used, otherwise required.
* `password`: password for the SAP HANA user. Optional when `assertion` or `sessionCookie` is used, otherwise required.
* `assertion`: a signed SAML bearer assertion or assertion factory function. [See more](#notes-about-saml-bearer-assertion-and-hana-session-cookie).
* `sessionCookie`: an SAP HANA sessionCookie. [See more](#notes-about-saml-bearer-assertion-and-hana-session-cookie).
* `maxPoolSize`: maximum number of resources to create at any given time. Optional, default to 1.
* `minPoolSize`: minimum number of resources to keep in the pool at any given time. Optional, default to 0.
* `idleTimeoutMillis`: max milliseconds a resource can stay idle in the pool before being removed. Optional, default to 30000.
* `refreshIdle`: boolean that specifies whether idle resources at or below the min threshold should be destroyed/re-created. Optional, default to true.
* `genericPoolLog`: if verbose log of [node-pool](https://github.com/coopernurse/node-pool) package should be displayed. Optional, default to false.
* `resphPrefix`: Optional prefix concatenated to response header in case of HTTP mode.
* `logger`: Optional. False by default. In order to enable logging, reference to a configured [tracer](https://github.com/baryon/tracer) logger should be set.
* `defaultSchema`: Optional. Default schema. By default no schema is setup, so the default schema of the `user` will be used.
* `databaseName`: The name of the database when connecting to a multitenant system.

Recommended logger setup:
```
var colors = require('colors');
var logger = require('tracer').colorConsole({
	dateformat: 'HH:MM:ss',
	stackIndex: 1, // this is important to be setup in order to see a full log
	filters: {
		trace: colors.cyan,
		debug: colors.yellow,
		info: colors.green,
		warn: colors.yellow,
		error: [ colors.red ]
	}
});
```

## Standalone mode

### Pool configuration connecting with username + password
```javascript
var hdbPool = require('node-hdb-pool');

var hpool = hdbPool.createPool({
	host: 'hostname',
	env: 'PROD',
	port: 30015,
	user: 'user',
	password: 'secret',
	maxPoolSize: 5,
	idleTimeoutMillis: 30000,
	logger: logger,
	genericPoolLog: true
});
```

### Pool configuration connecting with SAML bearer assertion
```javascript
var hdbPool = require('node-hdb-pool'),
    fs = require('fs');

// fake pid to pass to HANA in order to retain sessionCookie between restarts
hdbPool.setPid(1111);

var hpool = hdbPool.createPool({
	host: 'hostname',
	env: 'PROD',
	port: 30015,
	assertion: fs.readFileSync('path/to/signed_saml_assertion.xml', 'utf-8'),
	maxPoolSize: 5,
	minPoolSize: 1, // keep at least one connection alive in order to keep HANA session cookie alive
	idleTimeoutMillis: 30000,
	refreshIdle: false, // keep alive at least _minPoolSize_ connections (if it would be true, connections are destroyed and re-created in every _idleTimeoutMillis_)
	logger: logger,
	genericPoolLog: true
});
```

### Pool configuration connecting with SAP HANA session cookie
```javascript
var hdbPool = require('node-hdb-pool');

// fake pid to pass to HANA in order to retain sessionCookie between restarts
hdbPool.setPid(1111);

var hpool = hdbPool.createPool({
	host: 'hostname',
	env: 'PROD',
	port: 30015,
	user: 'user',
	sessionCookie: 'sessionCookie'
	maxPoolSize: 5,
	minPoolSize: 1, // keep at least one connection alive in order to keep HANA session cookie alive
	idleTimeoutMillis: 30000,
	refreshIdle: false, // keep alive at least _minPoolSize_ connections (if it would be true, connections are destroyed and re-created in every _idleTimeoutMillis_)
	logger: logger,
	genericPoolLog: true
});
```

### Usage of the pool in **standalone mode**
```javascript
if (hpool) {
	hpool.exec('SELECT system_id SID, host, version FROM M_DATABASE', [], function(err, rows) {
		if (err || rows.length !== 1) {
			console.error('Error while getting system information on ' + env);
		}
		else {
		    var env = hpool.env;
			console.log('System information of ' + env + ':');
			console.log(rows[0]);
		}
	});
}
```

## HTTP mode

### Pool configuration connecting with username + password
```javascript
var hdbPool = require('node-hdb-pool');

var hpool = hdbPool.createPool({
	host: 'hostname',
	env: 'PROD',
	port: 30015,
	user: 'user',
	password: 'secret',
	maxPoolSize: 5,
	idleTimeoutMillis: 30000,
	logger: logger,
	resphPrefix: 'x-customprefix'
});
```

### Pool configuration connecting with SAML bearer assertion
```javascript
var hdbPool = require('node-hdb-pool'),
    fs = require('fs');

// fake pid to pass to HANA in order to retain sessionCookie between restarts
hdbPool.setPid(1111);

var hpool = hdbPool.createPool({
	host: 'hostname',
	env: 'PROD',
	port: 30015,
	assertion: fs.readFileSync('path/to/signed_saml_assertion.xml', 'utf-8'),
	maxPoolSize: 5,
	minPoolSize: 1, // keep at least one connection alive in order to keep HANA sessionCookie alive
	idleTimeoutMillis: 30000,
	refreshIdle: false, // keep alive at least _minPoolSize_ connections (if it would be true, connections are destroyed and recreated in every _idleTimeoutMillis_)
	logger: logger,
	resphPrefix: 'x-customprefix'
});
```

### Usage of the pool in **HTTP mode**
Default response headers:
* **query-time**: actual execution time in milliseconds (it does not include waiting time for free resource)
* **waiting-time**: time in milliseconds spent on waiting for free resource

```javascript
// setup a server
var express = require('express'),
    app = express();

var sql = 'SELECT system_id SID, host, version FROM M_DATABASE';
var prepArgs = [];	// prepared statement arguments

app.get('/csvsysinfo', function(req, res) {
	if (hpool) {
		hpool.exec(sql, prepArgs, req, res, function(err) {
			if (err) {
				console.log(err);
				res.send('ERROR');
			}
		});
	}
	else {
		console.log('No pool is available!');
		res.send('No pool is available!');
	}
});

app.listen(3000, function () {
	console.log('Example app listening on port 3000!');
});

// NOTE: in case of minPoolSize > 0, pool needs to be killed manually
setTimeout(function() {
	if (hpool && hpool.pool) {
	    hpool.pool.min = 0;
		hpool.pool.drain(function() {
			hpool.pool.destroyAllNow();
			console.log('Pool destroyed on ' + hpool.env);
			hpool = null;	// clean the reference
		});
	}
}, 1*60*60*1000);   // kill the pool periodically in every hour
```

### Usage of the pool in **HTTP mode** with **streaming**
Available streaming outputs:
* **CSV** (Comma Separated Values): hdbPool.createCsvStringifier
* **SSV** (Semi-colon Separated Values): hdbPool.createSsvStringifier
* **JSON**: hdbPool.createJSONStringifier

Default response headers:
* **query-time**: actual execution time in milliseconds (it does not include waiting time for free resource)
* **waiting-time**: time in milliseconds spent on waiting for free resource

```javascript
// setup a server
var express = require('express'),
    app = express();

var sql = 'SELECT system_id SID, host, version FROM M_DATABASE';
var prepArgs = [];	// prepared statement arguments

app.get('/csvsysinfo', function(req, res) {
	if (hpool) {
		hpool.stream(sql, prepArgs, req, [hdbPool.createCsvStringifier, res], function(err) {
			if (err) {
				console.log(err);
				res.send('ERROR');
			}
		});
	}
	else {
		console.log('No pool is available!');
		res.send('No pool is available!');
	}
});

app.listen(3000, function () {
	console.log('Example app listening on port 3000!');
});

// NOTE: in case of minPoolSize > 0, pool needs to be killed manually
setTimeout(function() {
	if (hpool && hpool.pool) {
	    hpool.pool.min = 0;
		hpool.pool.drain(function() {
			hpool.pool.destroyAllNow();
			console.log('Pool destroyed on ' + hpool.env);
			hpool = null;	// clean the reference
		});
	}
}, 1*60*60*1000);   // kill the pool periodically in every hour
```

### Configure the pool with SessionStore
In case of **HTTP mode** if `minPoolSize > 0` and SAML bearer assertion is used for connecting to SAP HANA, the acquired HANA session cookie can be stored in the **sessionStore** by simply passing the `req` as second argument.
```
var hpool = hdbPool.createPool(poolOpts, req);
```

## Notes about SAML bearer assertion and HANA session cookie
* SAML bearer assertion has to be signed otherwise SAP HANA does not accept it
  * in SAP HANA, one needs to setup the public key-pair of the private key which was used to sign the assertion
* an example SAML bearer assertion is
```xml
<saml2:Assertion ID="_2068a633d1c4b262c8fdc768a1d53346" IssueInstant="2016-01-14T09:38:49.437Z" Version="2.0" xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <saml2:Issuer></saml2:Issuer>
  <ds:Signature xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
    <ds:SignedInfo>
      <ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
      <ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>
      <ds:Reference URI="">
        <ds:Transforms>
          <ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>
          <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">
            <ec:InclusiveNamespaces PrefixList="xsd" xmlns:ec="http://www.w3.org/2001/10/xml-exc-c14n#"/>
          </ds:Transform>
        </ds:Transforms>
        <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
        <ds:DigestValue />
      </ds:Reference>
    </ds:SignedInfo>
    <ds:SignatureValue />
    <ds:KeyInfo>
      <ds:X509Data />
    </ds:KeyInfo>
  </ds:Signature>
  <saml2:Subject xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion">
    <saml2:NameID Format="urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified" NameQualifier="http://idp.example.org/" SPNameQualifier="sp1">USERNAME</saml2:NameID>
    <saml2:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">
      <saml2:SubjectConfirmationData Address="127.0.0.1" InResponseTo="_aeefedf622b60752a564" NotOnOrAfter="2016-01-14T09:43:49.487Z" Recipient="http://sp1.example.org/callback"/>
    </saml2:SubjectConfirmation>
  </saml2:Subject>
  <saml2:Conditions NotBefore="2010-01-01T00:00:00Z" NotOnOrAfter="2017-01-01T00:00:00Z" xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion">
  </saml2:Conditions>
  <saml2:AuthnStatement AuthnInstant="2016-01-14T09:37:07.687Z" SessionIndex="_eab1b512b946bcfaeb0c9b5dd85bd275" xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion">
    <saml2:SubjectLocality Address="example.org"/>
    <saml2:AuthnContext>
      <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport</saml2:AuthnContextClassRef>
    </saml2:AuthnContext>
  </saml2:AuthnStatement>
</saml2:Assertion>
```
* once connection with a valid SAML bearer assertion is done, the user gets a HANA session cookie
* HANA session cookies can be seen in _SESSION_COOKIES_ system view
* HANA session cookies without alive connections are wiped out periodically
    * quick reconnect with a previously used cookie usually works
* a HANA session cookie is valid only for a given user from a given host+PID (OS process ID)
    * if the OS process is restarted and it gets a new PID, a previously used session cookie cannot be used to connect to HANA
    * to avoid this behaviour, use the `hdbPool.setPid(XXXX);` to setup a static PID before calling `hdbPool.createPool(...)`


## Query metadata
Metadata for a query can be retrieved by `hpool.meta()`.

```javascript
if (hpool) {
    hpool.meta('SELECT system_id SID, host, version FROM M_DATABASE', [], function(err, meta) {
        if (err) {
            console.error('Error while getting metadata ' + env);
        }
        else {
            console.log(meta);
        }
    });
}

```

## Running tests
For the tests a database connection has to be established to an SAP HANA database. Therefore you need to copy the configuration `test/config.js.tpl` file into `test/config.js` and change it with your connection data. An example config file can be seen here:
```javascript
var fs = required('fs');

var config = {
	host: 'example.host.com',
	port: 30015,
	user: 'username',
	password: 'secret',
	assertion: fs.readFileSync('path/to/signed_saml_assertion.xml', 'utf-8')
};

exports.config = config;
```

To run the unit tests go to the `node-hdb-pool` folder and type:

    $ npm test

## Todo
* Implement periodic reconnection-logic if database host becomes unavailable
* Improve documentation
* Improve test coverage



## FAQ
### Calling stored procedures and sending messages
Example stored procedure to insert a user into a table:
```sql
CREATE PROCEDURE INSERTUSER (
    p_username varchar(255)
)
SQL SECURITY DEFINER AS
BEGIN SEQUENTIAL EXECUTION
    DECLARE STATUS_MESSAGE CONDITION FOR SQL_ERROR_CODE 10001;
    DECLARE EXIT HANDLER FOR STATUS_MESSAGE
        SELECT ::SQL_ERROR_CODE AS "errno", ::SQL_ERROR_MESSAGE AS "errmsg" FROM DUMMY;

    DECLARE USER_EXISTS INT = 0;

    SELECT COUNT(USER_NAME) INTO USER_EXISTS FROM USERS WHERE USER_NAME = :p_username;
	IF (:USER_EXISTS > 0) THEN
        SIGNAL STATUS_MESSAGE SET MESSAGE_TEXT = '{"status": "error", "code": 1, "message": "user already exist"}';
    ELSE
        INSERT INTO USERS VALUES (:p_username);
		SIGNAL STATUS_MESSAGE SET MESSAGE_TEXT = \'{"status": "success", "code": 0, "message": "user created"}';
	END IF;
END;
```

Example code how to call the above procedure:
```javascript
hpool.exec('CALL INSERTUSER(?)', ['newuser'], function(err, rows) {
    if (err || !rows || rows.status) {
        // handle error
    }
    else {
        if (rows.status === 'error') {
            // handle error from stored proc
        }
        else if (rows.status === 'success') {
            // handle success from stored proc
        }
    }
});
```
