/**
 * environment/named pool manager for HANA
 */

/**
 * HDB environment pools
 *
 * Environment pool is for HDB operations using technical user credentials stored in the configuration file
 *
 * general properties:
 *    * only one pool is created per environment
 *    * pool is connecting with the static username and password stored in the configuration file
 *    * max pool size per environment can be defined in the conf file
 *
 * data structure:
 *    * envPools stores a single HDBPool object per environment (e.g. envPools['HDB'])
 *
 * cleanup:
 *    * idle connections are closed after POOL_TTL_MS milliseconds of inactivity
 *
 */

/**
 * HDB user pools
 *
 * Named pool is for HDB operations using effective SAML identity of the authenticated user
 *
 * general properties:
 *    * one user pool is created per user per environment
 *    * user pools are initialized on-demand
 *    * pool is connecting first with SAML assertion, then with HANA sessionCookie
 *    * max size of the user pool is set by Conf.userpool.POOL_SIZE
 *    * min pool size should be 1 to keep at least one connection alive in order to keep HANA sessionCookie alive
 *
 * data structure:
 *    * userPools are keyed by userId
 *    * for each userId:
 *      * created date: pool registration timestamp
 *      * pools: HDBPool objects for every target environment which was visited by the user
 *      * sessions: references to express sessions in order to handle multiple express sessions (e.g. access from different devices) per user
 *	    * lastAccess: last time the user initiated a request
 *
 * cleanup:
 *    * if lastAccess of a given user is more than Conf.userpool.USER_TIMEOUT seconds ago:
 *		* all registered express sessions will be destroyed (kelas: DISABLED - this is an overkill)
 *		* user pools on all environments will be drained
 *		* user is evicted from userPools object
 *
 */

'use strict';

var hdbPool = require('./hdbPool.js'),
	WSS = require('hana-saml-wsse'),
	precise = require('precise'),
	moment = require('moment'),
	url = require('url'),
	async = require('async'),
	hdbPoolLogger = global.logger || require('tracer').colorConsole({stackIndex: 1});

var SYSFOOTPRINT = {}; // store system information like SID, HOST, VERSION

var QUERYMODE = {
	EXEC: 'exec',
	META: 'meta',
	STREAM: 'stream'
};

var userPools = {},
	envPools = {};

var wssclient;

var SESSION_EXPIRED_TOKEN = 'SESSION_EXPIRED::';

var Conf = {

	usernamePrefix: '',

	sourceSysField: 'source-sys',
	samlDelegation: {
		loglevel: 'info'
	},
	userpool: {
		CLEANUP_INTERVAL: 0,
		POOL_SIZE: 3,
		POOL_TTL_MS: 30000,
		CLIENT_PID: 777,
		USER_TIMEOUT: 10,
		POOL_LOG: null
	},
	http: {
		HEADER_PREFIX: 'x-hdb-'
	},
	db: {
		default: 'HDB',
		HDB: {
			HOST: 'hdbhost',
			PORT: 30015,
			USER: 'USER',
			PASS: '****',
			DEFAULT_SCHEMA: 'USER',
			POOL_SIZE: 3,
			POOL_TTL_MS: 30000,
			POOL_LOG: null
		}
	}
};

function configure(c) {
	// TODO config sanity check
	Conf = c;
}

// initialise environment pools
function initPools(cb) {
	if (Conf.userpool.CLIENT_PID)
		hdbPool.setPid(Conf.userpool.CLIENT_PID); // fake pid to pass to HANA in order to retain sessionCookie between restarts)

	// initialization of user- and environment-pools are independent, and can happen parallel
	async.parallel([
		function(callback) {
			initEnvPools(function(err) {
				if (err) {
					logger.error('error initializing environment pools:', err);
				}
				return callback();
			});
		},
		function(callback) {
			initUserPools(callback);
		}
	],
	function(err, results) {
		return cb && cb();
	});
}

// initialise user pools (SAML/sessionCookie-based authentication)
function initUserPools(cb) {
	logger.info('initUserPools');

	if (Conf.samlDelegation) {
		Conf.samlDelegation.log = logger[Conf.samlDelegation.loglevel];
		wssclient = new WSS.client(Conf.samlDelegation);
	}

	userPools = {};
	setInterval(cleanupUserPools, Conf.userpool.CLEANUP_INTERVAL * 1000);	// setup periodic cleanup routine

	// user pools are initialized on-demand
	if (cb) return cb();
}

// initialise environment pools (username+password auth)
function initEnvPools(cb) {
	logger.info('initEnvPools');
	envPools = {};

	var envs = _.without(_.keys(Conf.db), 'default');

	logger.debug('initializing pools', envs);

	async.each(envs, function(env, callback) {
		var c = Conf.db[env];

		var poolOpts = {
			host: c.HOST,
			port: c.PORT,
			env: env,
			user: c.USER,
			password: c.PASS,
			logger: hdbPoolLogger,
			maxPoolSize: c.POOL_SIZE,
			idleTimeoutMillis: c.POOL_TTL_MS || 30000,	// specifies how long a resource can stay idle in pool before being removed
			genericPoolLog: c.POOL_LOG,
			resphPrefix: Conf.http.HEADER_PREFIX || 'x-hdb-',
			defaultSchema: c.DEFAULT_SCHEMA || c.SCHEMA
		};

		envPools[env] = hdbPool.createPool(poolOpts);

		// get system_id, host, and version
		envPools[env].exec('SELECT system_id SID, host, version FROM M_DATABASE', [], function(err, rows) {

			if (err || rows.length !== 1) {
				return callback('Error while getting system information on ' + env);
			}

			SYSFOOTPRINT[env] = rows[0];
			logger.debug('System information updated on ' + env);
			return callback();
		});
	}, function(err) {
		if (cb)
			return cb(err);
	});
}

// function to decide if the query needs to be executed on an environment pool or on a user pool
function query(querymode, sql, args, req, streams, cb, type) {

	var env = getEnvFromRequest(req);

	if (!env || !env.match(/^[A-Za-z0-9_]+$/) || !Conf.db[env]) {
		var err = 'invalid ' + Conf.sourceSysField + ' value: ' + env;
		logger.error(err);
		return cb ? cb(err) : null;
	}

	// if we have a valid SAML session, execute the query on a user pool
	if (req && req.SAMLAuthenticated && req.session && req.session.passport && req.session.passport.user)
		return queryUserPool(querymode, env, sql, args, req, streams, cb, type);

	// execute the query on an env pool
	queryEnvPool(querymode, env, sql, args, req, streams, cb, type);
}

// try to connect
function detectSAMLerror(env, userId, assertion, cb) {
	assertion = assertion.replace("'", '', 'g'); // assertion content should be fairly safe but strip it anyway
	queryEnvPool('exec', env, "CONNECT WITH SAML ASSERTION '" + assertion + "'", [], function(err) {
		if (!err) {
			logger.error(env + ':' + userId + ': unexpected error: assertion appears valid but direct SAML authentication earlier returned an error');
			// the env pool is tainted - nuke all resources immediately
			var p = envPools[env].pool;
			logger.debug(env  + ' initiating emergency pool flush...');
			p.destroyAllNow();
		}
		cb(err);
	});
}

function queryUserPool(querymode, env, sql, args, req, streams, cb, type) {

	logger.trace('queryUserPool');

	createUserPool(req, env, function(err, userId, env) {
		if (err) return cb(err);

		var pool = userPools[userId].pools[env];

		pool[querymode](sql, args, req, streams, function(err, result) {
			// detect 10: Authentication Failed HANA error
			var isAuthFailed = (err && (err.code === 10));

			// detect the error case when user does not exist in the target environment but we have tried to connect with its username + saml assertion
			var isMissingUser = (err && err.code === 591 && err.message && err.message.indexOf('Invalid principal id for principal $principalName$') > 0);

			// no special handling required
			if (!isAuthFailed && !isMissingUser) return cb(err, result);

			// handle errors which can block resource creation in the pool, such as:
			// - HANA session timed out -> 10: authentication failed
			// - SAML assertion timed out -> 10: authentication failed
			// - invalid SAML assertion -> 10: authentication failed
			// - user is not yet created on HANA -> 591: internal error: Invalid principal id for principal $principalName$
			var _drain = function(error) {
				delete error.assertion; // don't propagate failed assertion further upstream - potentially unsafe
				var p = pool.pool;
				p.min = 0;	// prevent re-creating resources when min > 0

				var _clean = function(error) {
					userPools[userId].pools[env] = null;	// destroy the reference
					// clean the HANA session cookie
					if (req.session && req.session.passport && req.session.passport.user && req.session.passport.user[env] && req.session.passport.user[env].HANAsessionCookie) {
						req.session.passport.user[env].HANAsessionCookie = null;
						req.session.save();
					}
					return cb(error, result);
				}

				if (p.getPoolSize() === 0) return _clean(error);	// no need to drain empty pool, just clean it

				// drain and clean non-empty pool
				p.drain(function() {
					logger.warn(env + ':' + userId + ' expired pool destroyed');
					p.destroyAllNow();
					return _clean(error);
				});
			}

			if (isMissingUser) {
				logger.error(userId + ': user is not created yet on ' + env);
				_drain(err);
			}
			else if (isAuthFailed) {
				if (pool._authMethod === 'assertion' && err.assertion) {
					// try to figure out what went wrong
					detectSAMLerror(env, userId, err.assertion, _drain);
				}
				else {	// HANA session cookie expired
					logger.error(userId + ': expired HANA ' + pool._authMethod + ' on ' + env);
					_drain(err);
				}
			}
		}, type);
	});
}

function queryEnvPool(querymode, env, sql, args, req, streams, cb, type) {
	logger.trace('queryEnvPool called');
	envPools[env][querymode](sql, args, req, streams, cb, type);
}

function getAssertionFactory(masterAssertion) {

	var masterAssertionInfo = wssclient.getAssertionInfo(masterAssertion),
		expDate = masterAssertionInfo.notOnOrAfter;

	// check if delegatable assertion is still valid
	if (expDate && moment(expDate).isBefore(moment())) {
		throw new Error(SESSION_EXPIRED_TOKEN + 'master assertion no longer valid (expired ' + moment(expDate).fromNow() + ')');
	}

	return function(cb) {
		logger.info('[wsa] requesting delegated assertion');
		var timer = precise().start();
		wssclient.get('wsa', function(err, assertion, assertionInfo) {
			if (err) {
				logger.error('[wsa]', err);
			} else {
				logger.info('[wsa] rcvd delegated assertion, roundrip',
					(timer.stop().diff()/1000000000).toFixed(2) + 's,',
					assertionInfo.notOnOrAfter ? ('expires ' + moment(assertionInfo.notOnOrAfter).fromNow()) : 'no expiration date');
			}
			cb(err, assertion, assertionInfo);
		}, masterAssertion);
	}
}

function createUserPool(req, env, cb) {

	if (req && req.session && !req.session.id) {
		logger.error('empty session id');
		return cb && cb('empty session id');
	}

	logger.trace('createUserPool called');
	var userId = getUserId(req);

	if (!userId) {
		var err = 'session is not authenticated';
		logger.error(err, req.session);
		if (cb)
			return cb(err);
		return;
	}

	if (!env) {
		env = getEnvFromRequest(req);
	}

	if (!Conf.db[env]) {
		var err = 'Invalid ' + Conf.sourceSysField + ' value: ' + env;
		logger.error(err);
		if (cb)
			return cb(err);
		return;
	}

	if (!userPools[userId]) {	// if user is not registered in the userPools yet
		logger.info('registering user ' + userId + ' in userPools');
		var o = userPools[userId] = {};
		o.created = new Date();
		o.pools = {};
		o.sessions = {};	// storing express sessions
	}

	//console.log(req.session);

	if (userPools[userId] && !userPools[userId].sessions[req.session.id]) {
		logger.info(req.session.id + ' express session registered in the userPools');
		userPools[userId].sessions[req.session.id] = req.session;
	}

	if (userPools[userId])
		userPools[userId].lastAccess = new Date();

	if (userPools[userId].pools[env]) {
		return cb ? cb(null, userId, env) : null; // found existing pool, nothing to do
	}

	// user does not have a pool for the target environment

	if (!req || !req.session || !req.session.passport || !req.session.passport.user) {
		logger.error(userId + ' express session does not contain sufficient information for hdb user pool creation.');
		return cb ? cb('unsupported hdb user pool authentication mode (session is not authenticated)') : null;
	}

	logger.info(userId, 'registering hdb user pool for ' + env);

	// common pool options
	var poolOpts = {
		host: Conf.db[env].HOST,
		port: Conf.db[env].PORT,
		env: env,
		logger: hdbPoolLogger,
		minPoolSize: 1, // we keep at least one connection alive to maintain HANA sessionCookie
		maxPoolSize: Conf.userpool.POOL_SIZE || 1,
		idleTimeoutMillis: Conf.userpool.POOL_TTL_MS || 30000,	// specifies how long a resource can stay idle in pool before being removed
		genericPoolLog: Conf.db[env].POOL_LOG,
		refreshIdle: false, // keep alive at least _minPoolSize_ connections (if true, connections are destroyed and recreated every _idleTimeoutMillis_)
		resphPrefix: Conf.http.HEADER_PREFIX || 'x-hdb-',
		defaultSchema: Conf.db[env].DEFAULT_SCHEMA || Conf.db[env].SCHEMA
	};

	var passportUser = req.session.passport.user,
		passportUserEnv = passportUser[env],
		poolAuthOpts = {},
		_trc  = env + ':' + userId + ':';

	// sessionCookie takes precedence if present
	if (passportUserEnv && passportUserEnv.HANAUser && passportUserEnv.HANAsessionCookie) {
		poolAuthOpts = {
			method: 'sessionCookie',
			user: passportUserEnv.HANAUser,
			sessionCookie: passportUserEnv.HANAsessionCookie
		};
	} else if (passportUser.assertion) {

		var assertionOrAssertionFactory;

		try {
			assertionOrAssertionFactory = wssclient
				? getAssertionFactory(passportUser.assertion)
				: passportUser.assertion;
		} catch (e) {
			return cb(e);
		}

		poolAuthOpts = {
			method: 'assertion',
			user: userId,
			assertion: assertionOrAssertionFactory
		};
	} else {
		logger.error(userId + ' neither assertion nor sessionCookie was found in the express session, unable to create user pool');
		return cb ? cb('unsupported hdb user pool authentication mode') : null;
	}

	poolOpts = _.extend(poolOpts, poolAuthOpts);
	userPools[userId].pools[env] = hdbPool.createPool(poolOpts, req);

	// store auth type so that later on we can try to detect assertion-specific error
	userPools[userId].pools[env]._authMethod = poolAuthOpts.method;

	logger.info(_trc, 'user pool created using', poolAuthOpts.method, 'auth');

	if (cb)
		return cb(null, userId, env);

}

// comma-separated values
function csv(sql, args, req, outstream, cb) {
	logger.trace('csv called');
	query(QUERYMODE.STREAM, sql, args, req, [hdbPool.createCsvStringifier, outstream], cb);
}

// semicolon-separated values
function ssv(sql, args, req, outstream, cb) {
	logger.trace('ssv called');
	query(QUERYMODE.STREAM, sql, args, req, [hdbPool.createSsvStringifier, outstream], cb);
}

function json(sql, args, req, outstream, cb) {
	logger.trace('json called');
	query(QUERYMODE.STREAM, sql, args, req, [hdbPool.createJSONStringifier, outstream], cb);
}

function exec(sql, args, req, outstream, cb) {
	logger.trace('exec called');
	query(QUERYMODE.EXEC, sql, args, req, outstream, cb);
}

function meta(sql, args, req, outstream, cb) {
	logger.trace('meta called');
	query(QUERYMODE.META, sql, args, req, outstream, cb);
}

// check if Conf.sourceSysField is present or not
function hasEnv(req) {
	// check header
	if (req.headers[Conf.sourceSysField])
		return true;

	// check GET params
	if (req.query[Conf.sourceSysField])
		return true;

	if (req.session && req.session.returnTo) {
		var parsed = url.parse(req.session.returnTo, true);
		if (parsed.query && parsed.query[Conf.sourceSysField])
			return true;
	}

	return false;
}

function getEnvFromRequest(req) {

	// set default
	var env = Conf.db.default || 'HDB';

	// check in session
	if (req && req.session && req.session[Conf.sourceSysField])
		env = req.session[Conf.sourceSysField];

	// check in header
	if (req && req.headers && req.headers[Conf.sourceSysField])
		env = req.headers[Conf.sourceSysField];

	// check in GET params
	if (req && req.query && req.query[Conf.sourceSysField])
		env = req.query[Conf.sourceSysField];
	else if (req && req.session && req.session.returnTo) {
		var parsed = url.parse(req.session.returnTo, true);
		if (parsed.query && parsed.query[Conf.sourceSysField])
			env = parsed.query[Conf.sourceSysField];
	}

	// check in POST params
	if (req && req.body && req.body[Conf.sourceSysField]) env = req.body[Conf.sourceSysField];

	return env;
}

function getSystemFootprint(req) {
	var env = getEnvFromRequest(req),
		sysft = SYSFOOTPRINT[env] || {};

	if (!sysft.SID) sysft.SID = 'NaN';
	if (!sysft.HOST) sysft.HOST = 'NaN';
	if (!sysft.VERSION) sysft.VERSION = 'NaN';

	return sysft;
}

// checks if the user already has a pool in the given environment
function hasUserPool(req) {
	var userId = getUserId(req),
		env = getEnvFromRequest(req);

	// check if the pool is already initialised and stored in userPools object
	if (userPools && userPools[userId] && userPools[userId].pools && userPools[userId].pools[env])
		return true;

	// check if the sessionCookie is stored in the express session for the given environment
	if (req && req.session && req.session.passport && req.session.passport.user
		&& req.session.passport.user[env] && req.session.passport.user[env].HANAsessionCookie)
		return true;

	return false;
}

function logout(req, next) {

	if (!req || !req.session || !req.session.passport || !req.session.passport.user)
		return next();

	var sessionId = req.session.id,
		userId = getUserId(req);

	// kill express session
	req.session.destroy(function() {

		logger.info(userId + ': destroyed session ' + sessionId);

		var poolObj = userPools[userId];

		if (!poolObj) {
			logger.info(userId + ': no hdb user pool to destroy');
			return next();
		}

		if (!poolObj.sessions) {
			logger.info(userId + ': no express sessions stored in meta pool');
			// FIXME should be safe to call `drainUserPool` here?
			return next();
		}

		// check if the user has any additional active Express sessions
		async.detect(_.keys(poolObj.sessions),
			function(sessId, cb) {
				req.sessionStore.get(sessId, function(err, session) {
					return err ? cb(false) : cb(session); // if session is not active, session will be `undefined`
				});
			}, function(activeSession) {
				if (activeSession) {
					logger.trace(userId + ': has additional active express sessions, retaining hdb user pool');
					return next();
				}

				// destroy user pool
				logger.trace(userId + ': no more active express sessions, destroying hdb user pool');
				return drainUserPool(userId, next);
		});
	});
}

// private methods
// -------------------------------------------------------------------
function getUserId(req) {
	// get userId
	var userId = 'UNAUTHENTICATED'; // TODO handle unathenticated case
	var usernameSessionKey = Conf.usernameSessionKey || 'nameID';
	if (req && req.session && req.session.passport && req.session.passport.user) {
		userId = req.session.passport.user[usernameSessionKey] || req.session.passport.user.nameID;
	} else {
		logger.error('no user id is found in the session (' + usernameSessionKey + ' = ?)');
		return null;
	}
	return Conf.usernamePrefix + userId;
}

function cleanupUserPools() {
	var timeoutThreshold = new Date().getTime() - Conf.userpool.USER_TIMEOUT * 1000;

	_.each(userPools, function(o, userId) {
		if (o.lastAccess < timeoutThreshold) {

			drainUserPool(userId, function() {
				logger.info('all hdb pools for', userId, 'are drained');
			});

			// destroy all Express sessions of the given user,
			// e.g. if logged in from different browsers/devices

			async.each(_.values(o.sessions), function(session, callback) {
				logger.info(userId + ' ' + session.id + ' express session destroyed');

				var envs = _.without(_.keys(Conf.db), 'default');

				envs.forEach(function(env) {
					if (session.passport.user[env]) {
						logger.info('clearing HDB sessionCookie for', env + ':' + userId + ' -> ' + session.passport.user[env].HANAsessionCookie);
						delete session.passport.user[env];
					}
				});

				callback();
				// overkill
				//session.destroy(callback);
				//
				//
				if (session.save) {
					session.save();
					logger.debug('updated session');
				}

			}, function(err) {
				if (err)
					logger.error(userId + ' error while destroying express sessions', err);

				//drainUserPool(userId);
			});

		}
	});
}

function drainUserPool(userId, cb) {
	if (!userPools[userId] && !userPools[userId].pools) {
		// unregister user from meta-pools
		delete userPools[userId];
		logger.info(userId + ' does not have a registered hdb pool');
		return cb ? cb() : null;
	}

	var pools = userPools[userId].pools;

	// unregister user from meta pools
	delete userPools[userId];

	// drain all pools
	async.forEachOf(pools, function(pool, env, callback) {
		if (!pool || !pool.pool)
			// pool is already killed because e.g. session timeout was detected
			return callback();

		pool.pool.min = 0;	// prevent re-creating resources when min > 0
		pool.pool.drain(function() {
			logger.info(userId + ' pool destroyed on ' + env);
			pool.pool.destroyAllNow();
			return callback();
		});
	}, function(err) {
		if (cb) return cb();
	});
}

exports.configure = configure;
exports.initPools = initPools;
exports.createUserPool = createUserPool;
exports.hasEnv = hasEnv;
exports.hasUserPool = hasUserPool;
exports.getEnv = getEnvFromRequest;
exports.csv = csv;
exports.ssv = ssv;
exports.json = json;
exports.exec = exec;
exports.meta = meta;
exports.getSystemFootprint = getSystemFootprint;
exports.getAssertionFactory = getAssertionFactory;
exports.q = queryEnvPool;
exports.logout = logout;
