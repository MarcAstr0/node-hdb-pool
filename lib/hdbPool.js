"use strict";

var hdb	= require('hdb'),
	poolModule = require('generic-pool'),
	uuid = require('node-uuid'),
	crypto = require('crypto'),
	async = require('async'),
	_ = require('underscore'),
	transformLobs = require('./transformLobs.js')
;

var hdbTypes = require('hdb/lib/protocol/common/TypeCode'),
	hdbTypeCodes = _.invert(hdbTypes);
//log('info', hdbTypeCodes);

var lobTypes = [hdbTypes.CLOB, hdbTypes.NCLOB, hdbTypes.BLOB, hdbTypes.TEXT];	// types need special handling while streaming

var DSV_DELIMITER = '\n';
var DSV_SEPARATOR = ';';
var ESCAPE_REGEXP = new RegExp('"', 'g');

var QUERYMODE = {
	EXEC: 1,
	META: 2,
	STREAM: 3
};

var DSV_QUOTE = {
	CHAR: true,
	VARCHAR: true,
	VARCHAR1: true,
	VARCHAR2: true,
	STRING: true,
	NSTRING: true,
	NCHAR: true,
	NVARCHAR: true,
	SHORTTEXT: true,
	TEXT: true
};

// optional post-processors for specific column types
var iso8601 = function(d) { return (d === '\\N') ? '\\N' : moment(d).format('YYYY-MM-DDTHH:mm:ss'); }; // ISO 8601 less tz
var DSV_FORMAT = {
	DAYDATE: iso8601,
	DATE: iso8601,
	TIMESTAMP: iso8601,
	TIMESTAMP_TZ: iso8601
};

var logger = null;

function log(level, msg, arg) {
	if (logger && logger[level]) {
		if (arg) {
			logger[level](msg, arg);
		}
		else {
			logger[level](msg);
		}
	}
}

function HDBPool(options, req) {
	var user = options.user || 'Unknown-user',
		env = options.env || options.host || 'Unknown-env'
	;

	// extract user from the assertion
	if (options.assertion && typeof options.assertion != 'function' && !options.user) {
		var m = options.assertion.match(/<([a-zA-Z0-9]+:)?NameID[^>]*>([^>]+)<\/([a-zA-Z0-9]+:)?NameID>/);
		if (m && m[2]) {
			user = m[2];
		}
		else {
			log('error', 'Assertion does not contain NameID tag');
			return;
		}
	}

	var assertionFactory;
	if (typeof options.assertion === 'function') {
		assertionFactory = options.assertion;
	} else if (typeof options.assertion === 'string') {
		assertionFactory = function(cb) {
			cb(null, options.assertion);
		};
	}

	// initialise request queue and pool for the given environment
	this.reqQueue = {};	// track requests closed by the client/requestor
	this.env = env;
	this.user = user;
	this.options = options;

	var maxPoolSize = options.maxPoolSize || 1;

	// setup special behaviour in case of min > 0 (min > 0 means that some connection needs to be always kept alive)
	if (options.minPoolSize > 0) {
		// * if minPoolSize > 0, by default generic-pool module will try to create a db-connection automatically when poolModule.Pool({...}) (constructor) is called
		// * if acquired() is called really soon after the constructor call, the automatically created db-connection will not be available yet.
		//    * but if maxPoolSize > 1, this acquire() call will try to create a new db-connection instead of wating for the automatically created one
		// the side-effects of the above behaviour:
		//  * for the first acquire() call, two db-connections would be created instead of the necessary one db-connection
		//	* if the pool is configured to connect with SAML-assertion, the same assertion would be used two times, and two HANA sessionCookies would be created for the same user
		// solution:
		//	* max-pool size is limited to 1 during the first connection attempt, and it will be set to options.maxPoolSize automatically, once the first connection is successfully created
		maxPoolSize = 1;
	}

	var pool = this.pool = poolModule.Pool({
		name: 'hdb-' + env + '-' + user,
		create: function(callback) {
			var poolFactory = this;
			var clientOpts = {
				host: options.host,
				port: options.port
			};

			var hdbclient = hdb.createClient(clientOpts);

			hdbclient.on('error', function(err) {
				log('error', user + ' CLIENT ERROR on ' + env, err);
				return callback(err, hdbclient);
			});

			var connOpts;

			if (options.user && options.password) {
				log('debug', env + ':' + user + ' connecting with username and password');
				connOpts = {user: user, password: options.password, method: 'password'};
			}
			else if (options.user && options.sessionCookie) {
				log('debug', env + ':' + user + ' hdb connecting with sessionCookie');
				connOpts = {
					user: options.user,
					// workaround to make node-hdb send SCRAMSHA256 response in addition to SAML.
					// If password is missing, invalid session cookie will result in "1033 error while parsing protocol"
					password: crypto.randomBytes(1),
					sessionCookie: new Buffer(options.sessionCookie, 'binary'),
					method: 'sessionCookie'
				};
			}
			else if (options.assertion) {
				log('debug', env + ':' + user + ' hdb connecting with SAML assertion');
			}
			else {
				log('error', user + ' no authentication method for ' + env);
				return callback('no authentication method', hdbclient);
			}

			if (!connOpts && options.assertion) {
				assertionFactory(function(err, assertion) {
					if (err) {
						return callback('SAML delegation error:' + err, hdbclient);
					}
					connOpts = {
						assertion: assertion,

						// workaround to make node-hdb send SCRAMSHA256 response in addition to SAML.
						// If missing, invalid assertion will result in "1033 error while parsing protocol"
						user: user, password: crypto.randomBytes(1),

						method: 'assertion' };
					conn();
				})
			} else {
				conn();
			}

			function conn() {

				hdbclient.connect(connOpts, function(err) {
					if (err) {
						log('info', env + ':' + user + ': error while creating hdb connection', err.message);
						if (connOpts.assertion)
							err.assertion = connOpts.assertion; // pass the assertion upstream for inspection
						return callback(err, hdbclient);
					}
					log('info', env + ':' + hdbclient.get('user') + ' connected using ' + connOpts.method);

					// store user and sessionCookie inside the pool for future connections
					user = options.user = hdbclient.get('user');
					var hdbCookie = hdbclient.get('sessionCookie');

					if (connOpts.method == 'assertion' && hdbCookie) { // HDB does not return a cookie for username/password scheme
						options.sessionCookie = hdbclient.get('sessionCookie').toString('binary');

						var pts = options.sessionCookie.split('@'); // split host
						//options.sessionCookie = pts[0];
						log('trace', 'got hdb cookie ->', options.sessionCookie, pts[1] ? pts[1] : '');

						// store sessionCookie into express session to support cube-server restart
						storeIntoSession(req, env, options.user, options.sessionCookie);
					}

					// after the first successful connection, maxPoolSize can be updated
					if (poolFactory.max != options.maxPoolSize)
						poolFactory.max = options.maxPoolSize;		// enable max pool size

					setDefaultSchema(options, hdbclient, user, env, callback);

				});
			}
		},

		validate: function(client) {
			if (client && client.readyState === 'connected') {
				// client is ready to use
				return true;
			}
			else {
				log('info', env + ':' + user + ' hdb client is not valid');
				// it will be automatically removed from the pool
				return false;
			}
		},

		destroy: function(client) {
			log('info', env + ':' + user + ' hdb client destroyed');
			client.end();
		},
		max: maxPoolSize,
		min: options.minPoolSize || 0,
		idleTimeoutMillis: options.idleTimeoutMillis || 30000, // specifies how long a resource can stay idle in pool before being removed
		log: options.genericPoolLog || false, // if true, logs via console.log - can also be a function
		refreshIdle: (_.isNull(options.refreshIdle) || _.isUndefined(options.refreshIdle)) ? true : options.refreshIdle
	});
}

// simple query
function q(sql, args, cb) {
	var prep = _.isArray(args) && args.length > 0;
	cb = cb || _.isFunction(args) ? args : function(){};
}

// streaming
HDBPool.prototype.stream  = function stream(sql, args, req, streams, cb, type) {
	query.call(this, QUERYMODE.STREAM, sql, args, req, streams, cb, type);
};

// normal exec
HDBPool.prototype.exec = function exec(sql, args, req, streams, cb, type) {
	if (_.isFunction(req)) 	// called without HTTP request/response
		query.call(this, QUERYMODE.EXEC, sql, args, null, null, req);
	else // called with HTTP request/response
		query.call(this, QUERYMODE.EXEC, sql, args, req, streams, cb);
};

// metadata
HDBPool.prototype.meta = function execute(sql, args, req, streams, cb, type) {
	if (_.isFunction(req)) 	// called without HTTP request/response
		query.call(this, QUERYMODE.META, sql, args, null, null, req);
	else // called with HTTP request/response
		query.call(this, QUERYMODE.META, sql, args, req, streams, cb);
};

// query
function query(querymode, sql, args, req, streams, cb, type) {
	var resp = (streams) ? ((_.isArray(streams)) ? _.last(streams) : streams) : null;
	var user = this.user, env = this.env, reqQueue = this.reqQueue, pool = this.pool, options = this.options, me = this;

	initRequest(this, req, resp, function(err, id) {
		if (err) return cb(err);

		var doPrep = _.isArray(args) && args.length;
		var start,	// when query was ready to start executing, but it might wait for free connection resource
			execStart	// real execution started
			;

		cb = cb ? cb : (_.isFunction(args) ? args : function(){ log('warn', 'no callback:', sql) });

		if (querymode === QUERYMODE.STREAM) {
			if (!streams || !_.isArray(streams) || streams.length < 1) return cb('streams[] is missing or empty');

			type = type || 'a'; // arraystream by default
			type = _.contains(['a', 'array'], type) ? 'createArrayStream' : 'createObjectStream';
		}

		function ex(o, sqlOrParams, exCb) {
			if (querymode === QUERYMODE.STREAM || querymode === QUERYMODE.META)
				executeQuery.call(me, querymode, o, sqlOrParams, id, req, resp, streams, type, exCb);
			else
				execQuery.call(me, o, sqlOrParams, id, req, resp, exCb);

		}

		start = +new Date();

		pool.acquire(function(err, client) {
			if (err) {	// error from factory.create
				log('info', user + ' ' + id + ' ' + env + ' - error while creating hdb connection', err.message || err);
				return cb(err);
			}

			// handle the case when client drops a request which is in the waiting queue
			if (!reqQueue[id]) {
				log('info', user + ' ' + id + ' ' + env + ' - request is already closed, no resource acquired')
				pool.release(client);
				log('trace', '# of free resources of ' + user + ' on ' + env + ': ' + pool.availableObjectsCount());
				return cb(null);	// error should not be sent back to the client since connection is already dropped
			}

			var closeCb;
			// handle the case when client drops a request which is already acquired resource
			if (req) {
				closeCb = function() {
					log('trace', user + ' ' + id + ' ' + env + ' - request closed by the client, resource released')
					pool.release(client);
					log('trace', '# of free resources of ' + user + ' on ' + env + ': ' + pool.availableObjectsCount());
					return cb(null);	// error should not be sent back to the client since connection is already dropped
				}

				req.on('close', closeCb);
			}

			log('trace', 'resource acquired for ' + user + ' ' +  id + ' ' + env);
			log('trace', '# of waiting requests of ' + user + ' on ' + env + ': ' + pool.waitingClientsCount());
			log('trace', '# of free resources of ' + user + ' on ' + env + ': ' + pool.availableObjectsCount());

			execStart = +new Date();

			// save sessionCookie into express-session
			storeIntoSession(req, env, options.user, options.sessionCookie);

			//console.log(sql);
			//console.log(args);
			//console.log('---------');

			var exCb = function(err, rows) {
				if (err) log('error', user + ' ' + id + ' ' + env + ' - Error while execution', err);
				else log('trace', user + ' ' + id + ' ' + env + ' - request served');
				pool.release(client);
				log('trace', '# of free resources of ' + user + ' on ' + env + ': ' + pool.availableObjectsCount());

				// resource is already released, so we can de-register the close-listener. This is particularly crucial in case of socket.io connection.
				if (closeCb)
					req.removeListener('close', closeCb);

				return cb(err, rows);
			};

			//setTimeout(function() {
			if (doPrep) {
				client.prepare(sql, function(err, s) {
					if (err) {
						pool.release(client);
						log('error', user + ' ' + id + ' ' + env + ' - Error while preparing statement', err);
						log('trace', '# of free resources of ' + user + ' on ' + env + ': ' + pool.availableObjectsCount());
						return cb(err);
					}
					ex(s, args, exCb);
				});
			} else {
				ex(client, sql, exCb);
			}
			//}, 20000);
		});
	});
}

function execQuery(statement, sqlOrParams, id, req, resp, exCb) {
	var user = this.user, env = this.env, options = this.options, start = this.start, execStart = this.execStart;

	log('trace', user + ' ' + id + ' ' + env +  '- start exec...');

	statement.exec(sqlOrParams, function(err, rows, dummyRows, tableRows) {
		if (err) {
			if (err) return exCb(err);
		}

		setupRespHeader(options, start, execStart, resp);

		var exception = null;
		if (dummyRows && dummyRows[0].errmsg) {
			try {
				exception = JSON.parse(dummyRows[0].errmsg);
			} catch (e) {
				exception = dummyRows;
			}

			if (exception.status === 'success') {
				rows = exception;
				exception = null;
			}
		}

		if (!exception && resp) {
			if (rows) resp.send(JSON.stringify(rows));
			else resp.send('');
		}

		return exCb(exception, rows);
	});
}

function executeQuery(querymode, statement, sqlOrParams, id, req, resp, streams, type, exCb) {
	var user = this.user, env = this.env, options = this.options, start = this.start, execStart = this.execStart;
	log('trace', user + ' ' + id + ' ' + env + ' - start executing...');

	statement.execute(sqlOrParams, function(err, rs) {
		if (err) return exCb(err);

		setupRespHeader(options, start, execStart, resp);

		if (querymode == QUERYMODE.META) {
			var metadata = rs.metadata;
			rs.close();

			if (resp) {
				resp.send(JSON.stringify(metadata))
			}

			return exCb(null, metadata);
		}

		//log('trace', user + ' ' + id + ' ' + env + ' - start streaming...');
		var stream = rs[type]();

		function onend() {
			//log('trace', user + ' ' + id + ' ' + env + ' - on end');
			if (!rs.closed) rs.close();
		}

		stream.on('error', exCb);
		stream.on('end', onend);
		stream.on('finish', function() {
			//log('trace', user + ' ' + id + ' ' + env + ' - on FINISH');
		});

		var hasLobColumn = _.find(rs.metadata, function(column) {
			return _.contains(lobTypes, column.dataType);
		});

		if (hasLobColumn) {	// insert a transformLobs stream before any other streams
			streams.unshift(transformLobs());
			// make sure that resultSet is not closed to early, so close it only when transformLobs has finished
			// otherwise if the content is more than 201 KB, resultSet is closed to early
			stream.removeListener('end', onend);
			streams[0].on('end', onend);
		}

		var s = stream;

		// handle the case when client drops the connection - make sure to close the resultset
		if (req)
			req.on('close', onend);

		_.each(streams, function(str, i) {
			if (_.isFunction(str)) {
				str = str.call(null, rs.metadata);
			}

			s = s.pipe(str);

			s.on('error', exCb);

			if (i === streams.length-1) {	// call only on the last stream (supposed to be the response stream)
				s.on('finish', exCb);
			}
		});
	});
}

function setupRespHeader(options, start, execStart, resp) {
	if (resp) {
		var headerPrefix = options.resphPrefix || '';
		resp.setHeader(headerPrefix + 'query-time', +new Date()-execStart);
		resp.setHeader(headerPrefix + 'waiting-time', execStart-start);
	}
}

function createCsvStringifier(metadata) {
	return createDsvStringifier(metadata, ',');
}

function createSsvStringifier(metadata) {
	return createDsvStringifier(metadata, ';');
}

function createDsvStringifier(metadata, sep) {
	if (!sep) sep = DSV_SEPARATOR;

	//metadata.forEach(logger.warn);

	var header = metadata.map(function getName(column) {
    	return column.columnDisplayName;
	}).join(sep) + DSV_DELIMITER;

	var functionBody = metadata.reduce(function addLine(body, column) {

		column.dataTypeName = hdbTypeCodes[column.dataType+''];

		var val = 'ifnull(row[\''+ ((column.columnDisplayName === "''") ? 'UnknownColumn' : column.columnDisplayName )+'\'])';

		if (DSV_QUOTE[column.dataTypeName]) {
			val = 'dsvquote('+val+', \''+sep+'\')';
		}

		if (DSV_FORMAT[column.dataTypeName]) {
			val = 'DSV_FORMAT.'+column.dataTypeName+'('+val+')';
		}

		body += 'line.push(' + val + ')';

		if (column.dataType === 13) {
			body += '.toString(\'hex\')';
		}
		body += '\n';
		return body;
	}, 'functionBody = function(row) { var line = [];\n') + 'return line.join("' + sep + '"); }';

	//log('debug', functionBody);

	functionBody = eval(functionBody);

	return new hdb.Stringifier({
		header: header,
		footer: '',
		seperator: DSV_DELIMITER,
		stringify: functionBody
  });
}

function createJSONStringifier() {
	return new hdb.Stringifier({
		header: '[',
		footer: ']',
		seperator: ',',
		stringify: JSON.stringify
	});
}

function transformLobs() {
	return new transformLobs();
}

// generate request id, process environment, and setup event handlers on request and setTimeout on response
function initRequest(pool, req, res, cb) {
	log('trace', 'init request');
	var env = pool.env, reqQueue = pool.reqQueue, user = pool.user, options = pool.options;

	// generate uuid for the request if it does not have any yet
	var id = (req && req.hdbPoolRequestId) ? req.hdbPoolRequestId : uuid.v1();	// reuse ID if hdbPool is called several times within one HTTP request
	log('trace', user + ' ' + id + ' ' + env + ' - new hdbPool call');

	// overwrite the default 120 sec response timeout
	// see: http://www.fingersdancing.net/2012/12/nodejs-connection-times-out-after-2.html
	if (res)
		res.setTimeout(0);

	// save sessionCookie into express-session
	// if pool is already initialized, options.sessionCookie is already defined so it can be stored
	// otherwise options.sessionCookie will be available only in the acquire() method-callback where storeIntoSession() is called again
	// the purpose of this call is to store the HANA sessionCookie as early as possible in order to ensure the smooth restart of the cube-server
	storeIntoSession(req, env, options.user, options.sessionCookie);

	reqQueue[id] = {open: true};

	if (req && !req.hdbPoolRequestId) {	// in case of new HTTP or new socket.io request
		req.hdbPoolRequestId = id;	// store the id in the request so we can distinguish new and reused requests

		// these listeners are registered only once per request
		req.on('close', function() {
			log('warn', user + ' ' + id + ' ' + env + ' - request closed by the client')
			delete reqQueue[id];
		});

		req.on('error', function(err) {
			log('error', user + ' ' + id + ' ' + env + ' - request error', err)
			delete reqQueue[id];
		});

		req.on('end', function() {
			delete reqQueue[id];
			log('trace', user + ' ' + id + ' ' + env + ' - request end')
		});
	}

	return cb(null, id);
}

// store username and HANA session cookie in the express session
function storeIntoSession(req, env, user, sessionCookie) {
	if (req && env && user && sessionCookie && req && req.session && req.session.passport && req.session.passport.user) {
		var passportUserEnv = req.session.passport.user[env] = {};
		passportUserEnv.HANAUser = user;
		passportUserEnv.HANAsessionCookie = sessionCookie;
		if (req.session) {
			req.session.save(); // persist changes
			log('trace', 'updated session');
			//log('debug', req.session);
		}
	}
}

function setDefaultSchema(options, hdbclient, user, env, callback) {
	// setup default schema if any
	if (!options.defaultSchema)
		return callback(null, hdbclient);

	hdbclient.exec('SET SCHEMA ' + options.defaultSchema, function (err) {
		if (err) {
			return callback(err, hdbclient);
		}
		log('trace', env + ':' + user + ' default schema: ' + options.defaultSchema);
		return callback(null, hdbclient);
	});
}

function dsvquote(v, sep) {
	// RFC 4180 pharagraph 6: Fields containing line breaks (CRLF), double quotes, and commas should be enclosed in double-quotes.
	if (v && (v.indexOf(sep) >= 0 || v.indexOf('"') >= 0 || v.match(/\r?\n|\r/))) {
		// RFC 4180 pharagraph 7: If double-quotes are used to enclose fields, then a double-quote appearing inside a field must be escaped by preceding it with another double quote.
		return ('"' + v.replace(ESCAPE_REGEXP, '""') + '"');
	}
	else return v;
}

function ifnull(v) {
	return v === null ? '\\N' : v;
}


exports.createPool = function createPool(options, req) {
	if (options.logger) {
		logger = options.logger;
	}

	// sanity check
	if (!options.host) {
		log('error', 'host needs to be specified');
		return;
	}
	if (!options.port) {
		log('error', 'port needs to be specified');
		return;
	}

	return new HDBPool(options, req);
};

exports.createCsvStringifier = createCsvStringifier;
exports.createSsvStringifier = createSsvStringifier;
exports.createJSONStringifier = createJSONStringifier;
exports.log = log;
exports.setPid = function(pid) {
	require('hdb/lib/util').pid = pid;
};
