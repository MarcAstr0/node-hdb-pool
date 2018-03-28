// Transform stream for streaming LOBs properly

var stream = require('stream'),
	async = require('async'),
	_ = require('underscore')
;

var Lob = require('hdb/lib/protocol/Lob'),
	hdbPool = require('./hdbPool.js')
;

function TransformLobs() {
	var transformLobs = new stream.Transform({objectMode: true});

	transformLobs._transform = function _transform(chunk, encoding, done) {
		var me = this;

		if (_.isArray(chunk) && chunk.length) {
			hdbPool.log('trace', 'Processing array chunk');
			async.map(
				chunk,
				function(item, callback) {
					transform(item, encoding, callback);
				},
				function(err, results) {
					me.push(results);
					done();
				}
			);
		}
		else {
			hdbPool.log('trace', 'Processing non-array chunk');
			transform(chunk, encoding, function(err, result) {
				me.push(result);
				done();
			});
		}
	}

	function transform(data, encoding, callback) {
		if (!data) return callback('Data is not an object!');

		async.forEachOf(data, function(value, column, cb) {
			hdbPool.log('trace', 'Reading ' + column);
			// handle Lobs
			if (value && value instanceof Lob) {
				hdbPool.log('trace', 'Processing ' + column + ' as Lob');
				var lob = value.createReadStream(),
					lobChunks = [];

				lob.on('readable', function () {
					var lobChunk = lob.read();
					if (lobChunk) lobChunks.push(lobChunk);
				});

				lob.on('error', function(err) {
					data[column] = 'Error while reading Lob!'
					hdbPool.log('error', 'Error while reading Lob. Column: ' + column);
					return cb();	// do not return with error - it would stop transforming the rest of the columns
				});

				lob.once('end', function() {
					data[column] = Buffer.concat(lobChunks).toString(encoding);
					hdbPool.log('trace', column + ' has been transformed.');
					return cb();
				});
			}
			else {
				// we don't need to touch the value
				hdbPool.log('trace', column + ' has been kept as it was.');
				return cb();
			}
		}, function(err) {
			return callback(err, data);
		});
	}

	return transformLobs;
}

module.exports = TransformLobs;
