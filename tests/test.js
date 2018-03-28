var should = require('should'),
	_ = require('underscore'),
	async = require('async'),
	hdbPool = require('../lib'),
	config = require('./config.js').config,
	express = require('express'),
	http = require('http'),
	d3 = require('d3'),
	loremIpsum = require('lorem-ipsum'),
	os = require('os')
	;

var hdbTypeCodes = _.invert(require('../node_modules/hdb/lib/protocol/common/TypeCode.js'));

// for testing
var colors = require('colors');
var logger = require('tracer').colorConsole({
	//level: 'error',
	dateformat: 'HH:MM:ss',
	stackIndex: 1,
	filters: {
		trace: colors.cyan,
		debug: colors.yellow,
		info: colors.green,
		warn: colors.yellow,
		error: [ colors.red ]
	}
});

var poolOpts;

before(function() {
	poolOpts = {
		host: config.host,
		port: config.port,
		//logger: logger
	};
});

describe('HDB pool /', function() {
	var sqls = {
		table: {
			create: { sql: 'create table HDBPOOLTEST (a int, b varchar(16))', params: [] },
			select: { sql: 'select * from HDBPOOLTEST', params: [] },
			insert: { sql: 'insert into HDBPOOLTEST values (?, ?)', params: [1, 'VALUE1'] },
			update: { sql: 'update HDBPOOLTEST set B = ? WHERE A = ?', params: ['NEWVALUE1', 1] },
			delete: { sql: 'delete from HDBPOOLTEST WHERE A = ? AND B = ?', params: [1, 'NEWVALUE1'] },
			drop: { sql: 'drop table HDBPOOLTEST', params: [] }
		},
		procedure: {
			create: {
				sql: '\
					create procedure HDBPOOLPROC (\
						IN p_id INTEGER, p_value varchar(16)\
					)\
					SQL SECURITY DEFINER AS\
					BEGIN SEQUENTIAL EXECUTION\
						DECLARE STATUS_MESSAGE CONDITION FOR SQL_ERROR_CODE 10001;\
						DECLARE EXIT HANDLER FOR STATUS_MESSAGE\
						SELECT ::SQL_ERROR_CODE AS "errno", ::SQL_ERROR_MESSAGE AS "errmsg" FROM DUMMY;\
						\
						SIGNAL STATUS_MESSAGE SET MESSAGE_TEXT = \'{"status": "success", "code": 0, "message": "\' || :p_id || \' \' || :p_value || \'"}\';\
					END;\
				', params: [] },
			call: { sql: 'call HDBPOOLPROC(?, ?)', params: [2, 'PROCVALUE'] },
			drop: { sql: 'drop procedure HDBPOOLPROC', params: [] }
		},
		function: {
			create: {
				sql: '\
					create function HDBPOOLFUNC (\
						IN p_id INTEGER, p_value varchar(16)\
					)\
					RETURNS TABLE(PID INTEGER, PVALUE NVARCHAR(16))\
					AS\
					BEGIN\
						RETURN SELECT :p_id PID, :p_value PVALUE FROM DUMMY;\
					END;\
				', params: [] },
			select: { sql: 'SELECT * FROM HDBPOOLFUNC(?, ?)', params: [3, 'FUNCVALUE'] },
			drop: { sql: 'drop function HDBPOOLFUNC', params: [] }
		}
	};


	describe('Pool in standalone mode /', function() {
		it('should not create a pool without host', function() {
			var hpool = hdbPool.createPool(_.omit(poolOpts, 'host'));
			should(hpool).not.be.ok();
		});

		it('should not create a pool without port', function() {
			var hpool = hdbPool.createPool(_.omit(poolOpts, 'port'));
			should(hpool).not.be.ok();
		});

		it('should not connect with invalid connection options', function(done) {
			// e.g. password is missing
			var hpool = hdbPool.createPool(_.extend(_.clone(poolOpts), {user: config.user}));
			checkPool(hpool);
			hpool.pool.acquire(function(err, client) {
				should.exist(client);
				should.exist(err);
				err.should.equal('no authentication method');
				drainPool(hpool, done);
			});
		});

		var optsUserPass;
		before(function() {
			optsUserPass = _.extend(_.clone(poolOpts), {user: config.user, password: config.password});
		});

		describe('Connect with username + password /', function() {
			before(function() {
				//this.skip();
			});

			it('should not connect with invalid username or password', function(done) {
				var opts = _.extend(_.clone(poolOpts), {user: config.user, password: 'invalidpassword'});

				var hpool = hdbPool.createPool(opts);
				checkPool(hpool);
				hpool.pool.acquire(function(err, client) {
					should.exist(client);
					should.exist(err);
					should.exist(err.message);
					err.message.should.equal('authentication failed');
					should.exist(err.code);
					err.code.should.equal(10);
					hpool.pool.availableObjectsCount().should.equal(0);
					drainPool(hpool, done);
				});
			});

			var hpool;
			beforeEach(function() {
				// create pool
				hpool = hdbPool.createPool(_.extend(optsUserPass, {maxPoolSize: 1}));
				checkPool(hpool);
			});

			afterEach(function(done) {
				drainPool(hpool, done);
			});

			it('should connect with valid username and password', function(done) {
				connect(hpool, config.user, done);
			});

			it('should connect with valid username and password and setup a default schema', function(done) {
				var hpoolWithSchema = hdbPool.createPool(_.extend(_.clone(optsUserPass), {maxPoolSize: 1, defaultSchema: 'SYS'}))

				hpoolWithSchema.pool.acquire(function(err, client) {
					should.exist(client);
					should.not.exist(err);
					client.get('user').should.equal(config.user);
					client.readyState.should.equal('connected');

					client.exec('SELECT CURRENT_SCHEMA FROM DUMMY', function(err, rows) {
						should.not.exist(err);
						should.exist(rows);
						rows.length.should.equal(1);
						should.exist(rows[0])
						should.exist(rows[0].CURRENT_SCHEMA)
						rows[0].CURRENT_SCHEMA.should.equal('SYS')
						hpool.pool.release(client);
						done(err, client);
					});
				});
			});

			it('should connect with username + password, and release the client', function(done) {
				connectRelease(hpool, done);
			});

			it('should connect with username + password, query and release the client', function(done) {
				connectQueryRelease(hpool, done);
			});
		});

		var optsAssertion;
		before(function() {
			optsAssertion = _.extend(_.clone(poolOpts), {
				assertion: config.assertion,
				maxPoolSize: 1
			});
		});

		describe('Connect with SAML assertion /', function() {
			before(function() {
				//this.skip();
			});

			it('should not initialize the pool if NameID is missing from the assertion', function() {
				var optsTmp = _.clone(optsAssertion);
				// modify nameID to be invalid
				optsTmp.assertion = optsTmp.assertion.replace(/<([a-zA-Z0-9]+:)?NameID/, '<$1NameI');

				var hpool = hdbPool.createPool(optsTmp);
				should(hpool).be.ok();
				should(hpool.pool).not.be.ok();
			});

			it('should not connect with invalid SAML assertion', function(done) {
				var optsTmp = _.clone(optsAssertion);
				// modify the signature (remove its last character) to make the assertion invalid
				optsTmp.assertion = optsTmp.assertion.replace(/(.){1}<\/([a-zA-Z0-9]+:)?SignatureValue/, '<\/$2SignatureValue');

				var hpool = hdbPool.createPool(optsTmp);
				checkPool(hpool);
				hpool.pool.acquire(function(err, client) {
					should.exist(client);
					should.exist(err);
					should.exist(err.message);
					err.message.should.equal('authentication failed');
					should.exist(err.code);
					err.code.should.equal(10);
					hpool.pool.availableObjectsCount().should.equal(0);
					drainPool(hpool, done);
				});
			});

			describe('Connect with SAML assertion when minPoolSize = 0 /', function() {
				before(function() {
					//this.skip();
				});

				var hpool;
				function createPool() {
					// create pool
					hpool = hdbPool.createPool(_.clone(optsAssertion));
					checkPool(hpool);
				}

				function closePool(done) {
					drainPool(hpool, done);
				}

				it('should connect with valid SAML assertion and get HANA sessionCookie', function(done) {
					var m = optsAssertion.assertion.match(/<([a-zA-Z0-9]+:)?NameID[^>]*>([^>]+)<\/([a-zA-Z0-9]+:)?NameID>/);
					should.exist(m);
					should.exist(m[2]);
					var userID = m[2];

					createPool();
					connect(hpool, userID, function(err, client) {
						should.exist(client.get('sessionCookie'));
						closePool(done);
					});
				});

				it('should connect with valid SAML assertion, and release the client', function(done) {
					createPool();
					connectRelease(hpool, function() {
						closePool(done);
					});
				});

				it('should connect with valid SAML assertion, query and release the client', function(done) {
					createPool();
					connectQueryRelease(hpool, function() {
						closePool(done);
					});
				});

				it('should connect with valid SAML assertion with static pid', function(done) {
					this.timeout(5000);
					var randomPid = Math.floor(Math.random()*9000000) + 1000000; // generate 7-digit random number
					hdbPool.setPid(randomPid);

					createPool();
					hpool.pool.acquire(function(err, client) {
						should.exist(client);
						should.not.exist(err);
						client.readyState.should.equal('connected');
						should.exist(client.get('sessionCookie'));
						should.exist(client.get('user'));
						var username = client.get('user');

						client.prepare('SELECT COUNT(*) CNT FROM SESSION_COOKIES WHERE USER_NAME = ? AND CLIENT_HOST = ? AND CLIENT_PID = ?', function (err, statement) {
							should.not.exist(err);
							// make sure that session cookie is created with the default pid (process.pid)
							statement.exec([username, os.hostname(), randomPid], function(err, rows) {
								hpool.pool.release(client);
								should.not.exist(err);
								should.exist(rows);
								rows.length.should.equal(1);
								should.exist(rows[0]);
								should.exist(rows[0].CNT);
								rows[0].CNT.should.equal(1);
								hdbPool.setPid(process.pid);	// set pid back to the default
								closePool(done);
							});
						});
					});
				});
			});

			// when minPoolSize = 1
			//	1. db-connection needs to be established right on pool-creation,
			// 	2. 1 db-connection should be always kept
			describe('Connect with SAML assertion when minPoolSize = 1 /', function() {
				before(function() {
					//this.skip();
				});

				beforeEach(function() {
				});

				//afterEach(function(done) {
				//});

				it('should connect with SAML assertion right on pool creation (without calling acquire() explicitly)', function(done) {
					this.timeout(5000);

					var hpool = hdbPool.createPool(_.extend(_.clone(optsAssertion), {minPoolSize: 1}));
					checkPool(hpool);

					// unfortunately, generic-pool does not provide a callback in this case, so we setup some timeout
					setTimeout(function() {
						hpool.pool.availableObjectsCount().should.equal(1);
						drainPool(hpool, done);
					}, 3000);
				});

				it('should have a connection even after idleTimeoutMillis', function(done) {
					this.timeout(7000);

					var hpool = hdbPool.createPool(_.extend(_.clone(optsAssertion), {minPoolSize: 1, idleTimeoutMillis: 2000, refreshIdle: false}));
					checkPool(hpool);

					// TODO test somehow if refreshIdle=false works as expected
					//setTimeout(function() {
					//	hpool.pool.availableObjectsCount().should.equal(1);
					//	console.log(hpool.pool.availableObjectsCount());
					//}, 2900);

					// check if after idleTimeoutMillis, we still have an available resource
					setTimeout(function() {
						hpool.pool.availableObjectsCount().should.equal(1);
						drainPool(hpool, done);
					}, 5000);
				});
			});


			describe('Connect with SAML assertion when maxPoolSize > 1 /', function() {
				it('should connect only once with the SAML assertion even if in case of two parallel query', function(done) {
					//this.skip();

					this.timeout(5000);
					var hpool = hdbPool.createPool(_.extend(_.clone(optsAssertion), {minPoolSize: 1, maxPoolSize: 2, refreshIdle: false}));
					checkPool(hpool);

					async.parallel([
						function(cb) {
							connectRelease(hpool, function(err, client) {
								should.exist(client.get('sessionCookie'));
								cb(null, client.get('sessionCookie'));
							});
						},
						function(cb) {
							connectRelease(hpool, function(err, client) {
								should.exist(client.get('sessionCookie'));
								cb(null, client.get('sessionCookie'));
							});
						}
					], function(err, results) {
						should.not.exist(err);
						should.exist(results);
						results.length.should.equal(2);
						results[0].should.equal(results[1]);
						// in case of the above parallel quieries only one creates the assertion, the other is queued for a while and gets executed only when connection with the assertion is ready
						hpool.pool.availableObjectsCount().should.equal(1);
						done();
					});
				});

				it('should connect only once with the SAML assertion even if in case of two parallel query, but for later queries maxPoolSize should be enabled', function(done) {
					this.timeout(10000);
					var hpool = hdbPool.createPool(_.extend(_.clone(optsAssertion), {minPoolSize: 1, maxPoolSize: 2, refreshIdle: false}));
					checkPool(hpool);

					async.parallel([
						function(cb) {
							connectRelease(hpool, function(err, client) {
								should.exist(client.get('sessionCookie'));
								cb(null, client.get('sessionCookie'));
							});
						},
						function(cb) {
							connectRelease(hpool, function(err, client) {
								should.exist(client.get('sessionCookie'));
								cb(null, client.get('sessionCookie'));
							});
						}
					], function(err, results) {
						should.not.exist(err);
						should.exist(results);
						results.length.should.equal(2);
						results[0].should.equal(results[1]);
						// in case of the above parallel quieries only one creates the assertion, the other is queued for a while and gets executed only when connection with the assertion is ready
						hpool.pool.availableObjectsCount().should.equal(1);
						//done();
						async.parallel([
							function(cb) {
								// release the resource only 2000ms later
								connectQueryRelease(hpool, cb, 2000);
							},
							function(cb) {
								connectQueryRelease(hpool, cb);
							}
						], function(err, results) {
							should.not.exist(err);
							should.exist(results);
							results.length.should.equal(2);
							hpool.pool.availableObjectsCount().should.equal(2);
							done();
						});
					});
				});
			});
		});

		describe('Connect with username + HANA session cookie /', function() {
			var username, sessionCookie;
			before(function(done) {
				//this.skip();

				// first we need to connect with SAML assertion in order to get HANA session cookie
				var hpool = hdbPool.createPool(_.clone(optsAssertion));
				checkPool(hpool);
				// make connection and kill the pool
				connectRelease(hpool, function(err, client) {
					should.exist(client.get('sessionCookie'));
					should.exist(client.get('user'));
					username = client.get('user');
					sessionCookie = client.get('sessionCookie');
					sessionCookie = sessionCookie.slice(0, 32);	// keep only the session cookie part (first 32 bytes), drop the PID@HOSTNAME part

					drainPool(hpool, done);
				});
			});

			var hpool;
			beforeEach(function() {
				// create pool with session cookie
				// NOTE: it might happen that after the above pool is destroyed, HANA wipes out the session cookie immediately, before we could reconnect with the cookie
				hpool = hdbPool.createPool(_.extend(_.clone(poolOpts), {user: username, sessionCookie: sessionCookie, maxPoolSize: 2}));
				checkPool(hpool);
			});

			afterEach(function(done) {
				drainPool(hpool, done);
			});

			it('should connect with valid username and HANA session cookie', function(done) {
				connect(hpool, username, done);
			});

			it('should connect with username + HANA session cookie, and release the client', function(done) {
				connectRelease(hpool, done);
			});

			it('should connect with username + HANA session cookie, query and release the client', function(done) {
				connectQueryRelease(hpool, done);
			});

			it('should be able to create more connection with the same username + HANA session cookie', function(done) {
				this.timeout(4000);

				async.parallel([
					function(cb) {
						hpool.pool.acquire(function(err, client) {
							should.exist(client);
							should.not.exist(err);
							client.readyState.should.equal('connected');
							setTimeout(function() {
								hpool.pool.release(client);
								cb();
							}, 2000);
						});
					},
					function(cb) {
						hpool.pool.acquire(function(err, client) {
							should.exist(client);
							should.not.exist(err);
							client.readyState.should.equal('connected');
							hpool.pool.release(client);
							cb();
						});
					}
				], function(err) {
					should.not.exist(err);
					hpool.pool.availableObjectsCount().should.equal(2);
					done();
				});
			});

			it('should throw error if HANA session cookie is expired', function(done) {
				// simulate session cookie expiration with modifying the PID

				hdbPool.setPid(1234);
				var hpool = hdbPool.createPool(_.extend(_.clone(poolOpts), {user: username, sessionCookie: sessionCookie}));
				checkPool(hpool);
				hpool.pool.acquire(function(err, client) {
					should.exist(client);
					should.exist(err);
					should.exist(err.message);
					err.message.should.equal('authentication failed');
					should.exist(err.code);
					err.code.should.equal(10);
					hpool.pool.availableObjectsCount().should.equal(0);
					hdbPool.setPid(process.pid);	// set pid back to the default
					drainPool(hpool, done);
				});
			});
		});

		describe('Execute DDLs/DMLs /', function() {
			before(function() {
				//this.skip();
			});

			var hpool;

			beforeEach(function() {
				// create pool
				hpool = hdbPool.createPool(_.extend(optsUserPass, {maxPoolSize: 3}));
				checkPool(hpool);
			});

			afterEach(function(done) {
				drainPool(hpool, done);
			});

			it('should create table', function(done) {
				hpool.exec(sqls.table.create.sql, sqls.table.create.params, function(err, rows) {
					should.not.exist(err);
					should.not.exist(rows);
					done();
				});
			});

			it('should insert row', function(done) {
				hpool.exec(sqls.table.insert.sql, sqls.table.insert.params, function(err, affectedRows) {
					should.not.exist(err);
					should.exist(affectedRows);
					affectedRows.should.equal(1);
					done();
				});
			});

			it('should update row', function(done) {
				hpool.exec(sqls.table.update.sql, sqls.table.update.params, function(err, affectedRows) {
					should.not.exist(err);
					should.exist(affectedRows);
					affectedRows.should.equal(1);
					done();
				});
			});

			it('should delete row', function(done) {
				hpool.exec(sqls.table.delete.sql, sqls.table.delete.params, function(err, affectedRows) {
					should.not.exist(err);
					should.exist(affectedRows);
					affectedRows.should.equal(1);
					done();
				});
			});

			it('should drop table', function(done) {
				hpool.exec(sqls.table.drop.sql, sqls.table.drop.params, function(err, rows) {
					should.not.exist(err);
					should.not.exist(rows);
					done();
				});
			});

			it('should create procedure', function(done) {
				hpool.exec(sqls.procedure.create.sql, sqls.procedure.create.params, function(err, rows) {
					should.not.exist(err);
					should.not.exist(rows);
					done();
				});
			});

			it('should call procedure', function(done) {
				hpool.exec(sqls.procedure.call.sql, sqls.procedure.call.params, function(err, rows) {
					should.not.exist(err);
					should.exist(rows);
					should.exist(rows.status);
					should.exist(rows.message);
					rows.status.should.equal('success');
					rows.message.should.equal('2 PROCVALUE');
					done();
				});
			});

			it('should drop procedure', function(done) {
				hpool.exec(sqls.procedure.drop.sql, sqls.procedure.drop.params, function(err, rows) {
					should.not.exist(err);
					should.not.exist(rows);
					done();
				});
			});

			it('should create function', function(done) {
				hpool.exec(sqls.function.create.sql, sqls.function.create.params, function(err, rows) {
					should.not.exist(err);
					should.not.exist(rows);
					done();
				});
			});

			it('should select from function', function(done) {
				hpool.exec(sqls.function.select.sql, sqls.function.select.params, function(err, rows) {
					should.not.exist(err);
					should.exist(rows);
					should.exist(rows.length);
					rows.length.should.equal(1);
					should.exist(rows[0]);
					should.exist(rows[0].PID);
					should.exist(rows[0].PVALUE);
					rows[0].PID.should.equal(3);
					rows[0].PVALUE.should.equal('FUNCVALUE');
					done();
				});
			});

			it('should drop function', function(done) {
				hpool.exec(sqls.function.drop.sql, sqls.function.drop.params, function(err, rows) {
					should.not.exist(err);
					should.not.exist(rows);
					done();
				});
			});
		});

		describe('Get metadata /', function() {
			before(function() {
				//this.skip();
			});

			var hpool;

			beforeEach(function() {
				// create pool
				hpool = hdbPool.createPool(_.extend(optsUserPass, {maxPoolSize: 3}));
				checkPool(hpool);
			});

			afterEach(function(done) {
				drainPool(hpool, done);
			});

			it('should get metadata', function(done) {
				// first create a table
				hpool.exec(sqls.table.create.sql, sqls.table.create.params, function(err, rows) {
					should.not.exist(err);
					should.not.exist(rows);

					// query metadata
					hpool.meta(sqls.table.select.sql, sqls.table.select.params, function(err, meta) {
						try {
							checkMetaData(err, meta);
							dropTable(done);
						} catch(e) {
							dropTable(function() {
								throw new Error(e);
							});
						}


						function dropTable(cb) {
							// drop table
							hpool.exec(sqls.table.drop.sql, sqls.table.drop.params, function(err, rows) {
								should.not.exist(err);
								should.not.exist(rows);
								cb && cb();
							});
						}
					});
				});
			});
		});

		describe('Stresstest /', function() {
			before(function() {
				//this.skip();
			});

			var hpool;
			beforeEach(function() {
				// create pool
				hpool = hdbPool.createPool(_.extend(optsUserPass, {maxPoolSize: 3}));
				checkPool(hpool);
			});

			afterEach(function(done) {
				drainPool(hpool, done);
			});

			it('should keep the number of acquired resource according to maxPoolSize', function(done) {
				this.timeout(8000);

				function acquire(timeout, cb) {
					hpool.pool.acquire(function(err, client) {
						should.exist(client);
						should.not.exist(err);
						client.readyState.should.equal('connected');

						if (timeout) {
							setTimeout(function() {
								hpool.pool.release(client);
								cb(err);
							}, timeout);
						}
						else {
							hpool.pool.release(client);
							cb(err);
						}
					});
				}

				async.parallel([
					function(cb) {
						acquire(4000, cb);
					},
					function(cb) {
						acquire(4000, cb);
					},
					function(cb) {
						acquire(4000, cb);
					},
					function(cb) {
						acquire(0, cb);
					},
					function(cb) {
						acquire(0, cb);
					},
					function(cb) {
						acquire(0, cb);
					},
					function(cb) {
						acquire(0, cb);
					}
				], function(err) {
					should.not.exist(err);
					hpool.pool.availableObjectsCount().should.equal(3);
					done();
				});

				// check the state after 2000 ms
				setTimeout(function() {
					should.exist(hpool.pool);
					hpool.pool.availableObjectsCount().should.equal(0);
					hpool.pool.inUseObjectsCount().should.equal(3);
					hpool.pool.waitingClientsCount().should.equal(4);

				}, 2000);
			});

			// TODO add more pool-related tests
		});
	});

	describe('Pool in HTTP mode /', function() {
		var app, port, hpool, srv, srvOpts;

		describe('HTTP DDLs/DMLs /', function() {
			var exec;

			before(function(done) {
				app = express();
				port = 3000;

				// create pool
				hpool = hdbPool.createPool(_.extend(_.clone(poolOpts), {user: config.user, password: config.password, maxPoolSize: 5, resphPrefix: 'test-'}));
				checkPool(hpool);

				exec = function(sql, prepArgs, req, res) {
					if (hpool && hpool.pool) {
						hpool.exec(sql, prepArgs, req, res, function(err) {
							if (err)
								res.send(err);
						});
					}
					else {
						res.send('No pool is available!');
					}
				};

				app.get('/normal/:object/:action', function(req, res) {
					var p = req.params,
						sqlObj = sqls[p.object][p.action];

					exec(sqlObj.sql, sqlObj.params, req, res);
				});

				srv = app.listen(3000, done);
				srvOpts = {host: 'localhost', port: port};
			});

			after(function(done) {
				srv.close();
				drainPool(hpool, done);
			});

			it('should create table /', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/table/create'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('');
						done();
					});
				});
			});

			it('should insert row', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/table/insert'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('1');
						done();
					});
				});
			});

			it('should update row', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/table/update'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('1');
						done();
					});
				});
			});

			it('should delete row', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/table/delete'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('1');
						done();
					});
				});
			});

			it('should drop table /', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/table/drop'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('');
						done();
					});
				});
			});

			it('should create procedure /', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/procedure/create'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('');
						done();
					});
				});
			});

			it('should call procedure /', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/procedure/call'}), function(res) {
					getResponseAsString(res, function(respStr) {
						var json = JSON.parse(respStr);
						should.exist(json);
						should.exist(json.status);
						should.exist(json.message);
						json.status.should.equal('success');
						json.message.should.equal('2 PROCVALUE');
						done();
					});
				});
			});

			it('should drop procedure /', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/procedure/drop'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('');
						done();
					});
				});
			});

			it('should create function /', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/function/create'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('');
						done();
					});
				});
			});

			it('should select from function /', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/function/select'}), function(res) {
					getResponseAsString(res, function(respStr) {
						var json = JSON.parse(respStr);
						should.exist(json);
						should.exist(json.length);
						json.length.should.equal(1);
						should.exist(json[0]);
						should.exist(json[0].PID);
						should.exist(json[0].PVALUE);
						json[0].PID.should.equal(3);
						json[0].PVALUE.should.equal('FUNCVALUE');
						done();
					});
				});
			});

			it('should drop function /', function(done) {
				http.get(_.extend(srvOpts, {path: '/normal/function/drop'}), function(res) {
					getResponseAsString(res, function(respStr) {
						respStr.should.equal('');
						done();
					});
				});
			});
		});

		describe('HTTP get metadata /', function() {
			var meta;

			before(function(done) {
				app = express();
				port = 3000;

				// create pool
				hpool = hdbPool.createPool(_.extend(_.clone(poolOpts), {user: config.user, password: config.password, maxPoolSize: 5, resphPrefix: 'test-'}));
				checkPool(hpool);

				meta = function(sql, prepArgs, req, res) {
					if (hpool && hpool.pool) {
						hpool.meta(sql, prepArgs, req, res, function(err) {
							if (err)
								res.send(err);
						});
					}
					else {
						res.send('No pool is available!');
					}
				};

				app.get('/normal/:object/:action', function(req, res) {
					var p = req.params,
						sqlObj = sqls[p.object][p.action];

					meta(sqlObj.sql, sqlObj.params, req, res);
				});

				srv = app.listen(3000, done);
				srvOpts = {host: 'localhost', port: port};
			});

			after(function(done) {
				srv.close();
				drainPool(hpool, done);
			});

			it('should get metadata /', function(done) {
				// first create a table
				hpool.exec(sqls.table.create.sql, sqls.table.create.params, function(err, rows) {
					should.not.exist(err);
					should.not.exist(rows);
					// query metadata
					http.get(_.extend(srvOpts, {path: '/normal/table/select'}), function(res) {
						getResponseAsString(res, function(respStr) {
							try {
								var json = JSON.parse(respStr);
								checkMetaData(err, json);
								dropTable(done);
							} catch(e) {
								dropTable(function() {
									throw new Error(e);
								});
							}

							function dropTable(cb) {
								// drop table
								hpool.exec(sqls.table.drop.sql, sqls.table.drop.params, function(err, rows) {
									should.not.exist(err);
									should.not.exist(rows);
									cb && cb();
								});
							}
						});
					});
				});
			});
		});

		describe('HTTP Streaming /', function() {
			var loremIpsumOpts = {count: 1500, units: 'words'};
			var stream,
				longtexts = [loremIpsum(loremIpsumOpts), loremIpsum(loremIpsumOpts)];

			before(function(done) {
				app = express();
				port = 3000;

				// create pool
				hpool = hdbPool.createPool(_.extend(_.clone(poolOpts), {user: config.user, password: config.password, maxPoolSize: 5, resphPrefix: 'test-'}));
				checkPool(hpool);

				stream = function(sql, prepArgs, req, res, stringifier) {
					if (hpool && hpool.pool) {
						hpool.stream(sql, prepArgs, req, [stringifier, res], function(err) {
							if (err) {
								res.send('ERROR');
							}
						});
					}
					else {
						res.send('No pool is available!');
					}
				};

				app.get('/stream/jsontest', function(req, res) {
					var sql = 'SELECT \'c1r1\' AS COL1, \'c2r1\' AS COL2  FROM DUMMY \
								UNION \
								SELECT \'c1r2\' AS COL1, \'c2r2\' AS COL2  FROM DUMMY';
					stream(sql, [], req, res, hdbPool.createJSONStringifier);
				});

				app.get('/stream/jsontest-lob', function(req, res) {
					var sql = 'SELECT \'c1r1\' COL1, CAST(\'' + longtexts[0] + '\' AS NCLOB) COL2  FROM DUMMY \
								UNION ALL \
								SELECT \'c1r2\' COL1, CAST(\'' + longtexts[1] + '\' AS NCLOB) COL2  FROM DUMMY';
					stream(sql, [], req, res, hdbPool.createJSONStringifier);
				});

				app.get('/stream/jsontest-prepared', function(req, res) {
					var sql = 'SELECT * FROM (SELECT \'c1r1\' AS COL1, \'c2r1\' AS COL2  FROM DUMMY \
								UNION \
								SELECT \'c1r2\' AS COL1, \'c2r2\' AS COL2  FROM DUMMY) WHERE COL1 = ?';
					stream(sql, ['c1r2'], req, res, hdbPool.createJSONStringifier);
				});

				app.get('/stream/headertest', function(req, res) {
					var sql = 'SELECT \'c1r1\' AS COL1, \'c2r1\' AS COL2  FROM DUMMY';
					stream(sql, [], req, res, hdbPool.createJSONStringifier);
				});

				app.get('/stream/csvtest-rfc4180', function(req, res) {
					var sql = 'SELECT \'value,value\' AS COL1, \'value"value\' AS COL2, \'value\nvalue\' AS COL3 FROM DUMMY';
					//var sql = 'SELECT \'c1r1\' AS COL1, \'c2r1\' AS COL2  FROM DUMMY';
					stream(sql, [], req, res, hdbPool.createCsvStringifier);
				});

				app.get('/stream/csvtest', function(req, res) {
					var sql = 'SELECT \'c1r1\' AS COL1, \'c2r1\' AS COL2  FROM DUMMY \
								UNION \
								SELECT \'c1r2\' AS COL1, \'c2r2\' AS COL2  FROM DUMMY';
					stream(sql, [], req, res, hdbPool.createCsvStringifier);
				});

				app.get('/stream/csvtest-lob', function(req, res) {
					var sql = 'SELECT \'c1r1\' COL1, CAST(\'' + longtexts[0] + '\' AS NCLOB) COL2  FROM DUMMY \
								UNION ALL \
								SELECT \'c1r2\' COL1, CAST(\'' + longtexts[1] + '\' AS NCLOB) COL2  FROM DUMMY';
					stream(sql, [], req, res, hdbPool.createCsvStringifier);
				});

				app.get('/stream/csvtest-prepared', function(req, res) {
					var sql = 'SELECT * FROM (SELECT \'c1r1\' AS COL1, \'c2r1\' AS COL2  FROM DUMMY \
								UNION \
								SELECT \'c1r2\' AS COL1, \'c2r2\' AS COL2  FROM DUMMY) WHERE COL1 = ?';
					stream(sql, ['c1r2'], req, res, hdbPool.createCsvStringifier);
				});

				srv = app.listen(3000, done);
			});

			after(function(done) {
				srv.close();
				drainPool(hpool, done);
			});

			it('should stream query as JSON /', function(done) {
				http.get({host: 'localhost', port: port, path: '/stream/jsontest'}, function(res) {
					getResponseAsString(res, function(respStr) {
						var json = JSON.parse(respStr);
						should.exist(json);
						json.length.should.equal(2);
						should.exist(json[0].COL1);
						should.exist(json[1].COL2);

						json[0].COL1.should.equal('c1r1');
						json[1].COL2.should.equal('c2r2');

						done();
					});
				});
			});

			it('should stream LOB-query as JSON /', function(done) {
				http.get({host: 'localhost', port: port, path: '/stream/jsontest-lob'}, function(res) {
					getResponseAsString(res, function(respStr) {
						var json = JSON.parse(respStr);
						should.exist(json);
						json.length.should.equal(2);
						should.exist(json[0].COL1);
						should.exist(json[1].COL2);

						json[0].COL1.should.equal('c1r1');
						json[0].COL2.should.equal(longtexts[0]);
						json[1].COL1.should.equal('c1r2');
						json[1].COL2.should.equal(longtexts[1]);

						done();
					});
				});
			});

			it('should stream preparedStatement query as JSON /', function(done) {
				http.get({host: 'localhost', port: port, path: '/stream/jsontest-prepared'}, function(res) {
					getResponseAsString(res, function(respStr) {
						var json = JSON.parse(respStr);
						should.exist(json);
						json.length.should.equal(1);
						should.exist(json[0].COL1);

						json[0].COL1.should.equal('c1r2');

						done();
					});
				});
			});

			it('should have default headers with custom prefix if configured /', function(done) {
				http.get({host: 'localhost', port: port, path: '/stream/headertest'}, function(res) {
					//console.log(res.headers);
					should.exist(res);
					should.exist(res.headers);
					should.exist(res.headers['test-query-time']);
					should.exist(res.headers['test-waiting-time']);
					done();
				});
			});

			it('should stream CSV according to RFC 4180 /', function(done) {
				// RFC 4180 pharagraph 6: Fields containing line breaks (CRLF), double quotes, and commas should be enclosed in double-quotes.
				http.get({host: 'localhost', port: port, path: '/stream/csvtest-rfc4180'}, function(res) {
					getResponseAsString(res, function(respStr) {
						var rows = d3.csv.parse(respStr);	// d3.csv parses according to RFC 4180
						should.exist(rows);
						rows.length.should.equal(1);
						should.exist(rows[0].COL1);
						should.exist(rows[0].COL2);
						should.exist(rows[0].COL3);
						rows[0].COL1.should.equal('value,value');
						rows[0].COL2.should.equal('value"value');
						rows[0].COL3.should.equal('value\nvalue');
						done();
					});
				});
			});

			it('should stream query as CSV /', function(done) {
				http.get({host: 'localhost', port: port, path: '/stream/csvtest'}, function(res) {
					getResponseAsString(res, function(respStr) {
						var rows = d3.csv.parse(respStr);
						should.exist(rows);
						rows.length.should.equal(2);
						should.exist(rows[0].COL1);
						should.exist(rows[1].COL2);
						rows[0].COL1.should.equal('c1r1');
						rows[1].COL2.should.equal('c2r2');

						done();
					});
				});
			});

			it('should stream LOB-query as CSV /', function(done) {
				http.get({host: 'localhost', port: port, path: '/stream/csvtest-lob'}, function(res) {
					getResponseAsString(res, function(respStr) {
						var rows = d3.csv.parse(respStr);
						should.exist(rows);
						rows.length.should.equal(2);
						should.exist(rows[0].COL1);
						should.exist(rows[1].COL2);

						rows[0].COL1.should.equal('c1r1');
						rows[0].COL2.should.equal(longtexts[0]);
						rows[1].COL1.should.equal('c1r2');
						rows[1].COL2.should.equal(longtexts[1]);

						done();
					});
				});
			});

			it('should stream preparedStatement query as CSV /', function(done) {
				http.get({host: 'localhost', port: port, path: '/stream/csvtest-prepared'}, function(res) {
					getResponseAsString(res, function(respStr) {
						var rows = d3.csv.parse(respStr);
						should.exist(rows);
						rows.length.should.equal(1);
						should.exist(rows[0].COL1);

						rows[0].COL1.should.equal('c1r2');

						done();
					});
				});
			});

			// TODO add more test focusing on unexpectedly closed HTTP connection
		});
	});

});


// helper functions

function connect(hpool, user, done) {
	hpool.pool.acquire(function(err, client) {
		should.exist(client);
		hpool.pool.release(client);
		should.not.exist(err);
		client.get('user').should.equal(user);
		client.readyState.should.equal('connected');
		done(err, client);
	});
}

function connectRelease(hpool, done) {
	hpool.pool.acquire(function(err, client) {
		should.exist(client);
		hpool.pool.release(client);
		should.not.exist(err);
		client.readyState.should.equal('connected');
		done(err, client);
	});
}

function connectQueryRelease(hpool, done, releaseTimeout) {
	hpool.pool.acquire(function(err, client) {
		should.exist(client);
		should.not.exist(err);
		client.readyState.should.equal('connected');
		client.exec('SELECT \'col1\' AS COL1, \'col2\' AS COL2  FROM DUMMY', function(err, rows) {
			function doIt() {
				hpool.pool.release(client);
				should.not.exist(err);
				should.exist(rows);
				rows.length.should.equal(1);
				rows[0].should.ok();
				rows[0].COL1.should.ok();
				rows[0].COL1.should.equal('col1');
				rows[0].COL2.should.ok();
				rows[0].COL2.should.equal('col2');
				done(err, client);
			}
			if (releaseTimeout) setTimeout(doIt, releaseTimeout);
			else doIt();

		});
	});
}

function checkPool(hpool) {
	should(hpool).be.ok();
	should(hpool.pool).be.ok();
}

function drainPool(hpool, done) {
	should(hpool).be.ok();
	should(hpool.pool).be.ok();
	// destroy the pool
	hpool.pool.drain(function() {
		hpool.pool.destroyAllNow();
		done();
	});
}

function getResponseAsString(res, cb) {
	should.exist(res);
	var resp = '';
	res.on('data', function(chunk) {
		should.exist(chunk);
		resp += chunk.toString('utf8');
	});

	res.on('end', function() {
		should.exist(resp);
		return cb(resp);
	});
}

function checkMetaData(err, meta) {
	should.not.exist(err);
	should.exist(meta);
	should.exist(meta.length);
	meta.length.should.equal(2);
	should.exist(meta[0].dataType);
	should.exist(meta[0].length);
	should.exist(meta[0].tableName);
	should.exist(meta[0].columnName);
	should.exist(meta[0].columnDisplayName);
	hdbTypeCodes[meta[0].dataType].should.equal('INT');
	meta[0].length.should.equal(10);
	meta[0].tableName.should.equal('HDBPOOLTEST');
	meta[0].columnName.should.equal('A');
	meta[0].columnDisplayName.should.equal('A');

	should.exist(meta[1].dataType);
	should.exist(meta[1].length);
	should.exist(meta[1].tableName);
	should.exist(meta[1].columnName);
	should.exist(meta[1].columnDisplayName);
	hdbTypeCodes[meta[1].dataType].should.equal('VARCHAR1');
	meta[1].length.should.equal(16);
	meta[1].tableName.should.equal('HDBPOOLTEST');
	meta[1].columnName.should.equal('B');
	meta[1].columnDisplayName.should.equal('B');
}
