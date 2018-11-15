/**
 * ElasticSearch Emitter. Store Request/Response records in Elasticsearch
 */
const elasticsearch = require('elasticsearch');
const AWS = require('aws-sdk');

const debug = require('debug')('sws:elastic');
const swsUtil = require('./swsUtil');
const moment = require('moment');
const shortId = require('shortid');

const indexTemplate = require('../schema/elasticsearch/api_index_template.json');

const ES_MAX_BUFF = 50;

let esClient;

// ElasticSearch Emitter. Store Request/Response records in Elasticsearch
function swsElasticEmitter() {
  // Options
  this.options = null;

  this.indexBuffer = '';
  this.bufferCount = 0;
  this.lastFlush = 0;

  this.elasticURL = null;
  this.elasticURLBulk = null;
  this.elasticProto = null;
  this.elasticHostname = null;
  this.elasticPort = null;

  this.indexPrefix = 'api-';

  this.enabled = false;
}

// Initialize
swsElasticEmitter.prototype.initialize = function(swsOptions) {
  if (typeof swsOptions === 'undefined') return;
  if (!swsOptions) return;

  this.options = swsOptions;

  if (swsOptions[swsUtil.supportedOptions.aws]) {
    AWS.config.update(swsOptions[swsUtil.supportedOptions.aws]);

    esClient = new elasticsearch.Client({
      host: swsOptions[swsUtil.supportedOptions.elasticsearch],
      log: 'error',
      connectionClass: require('http-aws-es')
    });
  } else {
    esClient = new elasticsearch.Client({
      host: swsOptions[swsUtil.supportedOptions.elasticsearch],
      log: 'error'
    });
  }

  // Set or detect hostname
  if (!(swsUtil.supportedOptions.elasticsearch in swsOptions)) {
    debug('Elasticsearch is disabled');
    return;
  }

  this.elasticURL = swsOptions[swsUtil.supportedOptions.elasticsearch];

  if (!this.elasticURL) {
    debug('Elasticsearch url is invalid');
    return;
  }

  this.elasticURLBulk = `${this.elasticURL}/_bulk`;

  if (swsUtil.supportedOptions.elasticsearchIndexPrefix in swsOptions) {
    this.indexPrefix = swsOptions[swsUtil.supportedOptions.elasticsearchIndexPrefix];
  }

  // Check / Initialize schema
  this.initTemplate();

  this.enabled = true;
};

// initialize index template
swsElasticEmitter.prototype.initTemplate = function(rrr) {
  const that = this;

  const requiredTemplateVersion = indexTemplate.version;

  // Check if there is a template
  const templateURL = `${this.elasticURL}/_template/template_api`;
  esClient.indices.get({ index: 'api-*' }).then(
    (res) => {
      // do nothing
    },
    (err) => {
      esClient.indices.create({ index: 'api-*', indexTemplate }).then(
        () => {
          // do nothing
        },
        (err) => {
          console.log(err);
          // cb();
        }
      );
    }
  );

  // request.get({ url: templateURL, json: true }, (error, response, body) => {
  //   if (error) {
  //     debug('Error querying template:', JSON.stringify(error));
  //   } else {
  //     let initializeNeeded = false;

  //     if (response.statusCode === 404) {
  //       initializeNeeded = true;
  //     } else if (response.statusCode === 200) {
  //       if ('template_api' in body) {
  //         if (
  //           !('version' in body.template_api) ||
  //           body.template_api.version < requiredTemplateVersion
  //         ) {
  //           initializeNeeded = true;
  //         }
  //       }
  //     }

  //     if (initializeNeeded) {
  //       request.put({ url: templateURL, json: indexTemplate }, (error, response, body) => {
  //         if (error) {
  //           debug('Failed to update template:', JSON.stringify(error));
  //         }
  //       });
  //     }
  //   }
  // });
};

// Update timeline and stats per tick
swsElasticEmitter.prototype.tick = function(ts, totalElapsedSec) {
  // Flush if buffer is not empty and not flushed in more than 1 second
  if (this.bufferCount > 0 && ts - this.lastFlush >= 1000) {
    this.flush();
  }
};

// Pre-process RRR
swsElasticEmitter.prototype.preProcessRecord = function(rrr) {
  // handle custom attributes
  if ('attrs' in rrr) {
    const attrs = rrr.attrs;
    for (const attrname in attrs) {
      attrs[attrname] = swsUtil.swsStringValue(attrs[attrname]);
    }
  }

  if ('attrsint' in rrr) {
    const intattrs = rrr.attrsint;
    for (const intattrname in intattrs) {
      intattrs[intattrname] = swsUtil.swsNumValue(intattrs[intattrname]);
    }
  }
};

// Index Request Response Record
swsElasticEmitter.prototype.processRecord = function(rrr) {
  if (!this.enabled) {
    return;
  }

  this.preProcessRecord(rrr);

  // Create metadata
  const indexName =
    this.indexPrefix +
    moment(rrr['@timestamp'])
      .utc()
      .format('YYYY.MM.DD');
  const meta = { index: { _index: indexName, _type: 'api', _id: rrr.id } };

  // Add to buffer
  this.indexBuffer += `${JSON.stringify(meta)}\n`;
  this.indexBuffer += `${JSON.stringify(rrr)}\n`;

  this.bufferCount += 1;

  if (this.bufferCount >= ES_MAX_BUFF) {
    this.flush();
  }
};

swsElasticEmitter.prototype.flush = function() {
  if (!this.enabled) {
    return;
  }

  this.lastFlush = Date.now();

  // const options = {
  //   url: this.elasticURLBulk,
  //   headers: {
  //     'Content-Type': 'application/x-ndjson'
  //   },
  //   body: this.indexBuffer
  // };

  esClient
    .index({
      index: 'api-*',
      type: 'api',
      body: this.indexBuffer,
      id: shortId.generate(),
      refresh: true
    })
    .then(
      (resp) => {},
      (err) => {
        console.error('error from elasticsearch', err);
      }
    );

  // request.post(options, (error, response, body) => {
  //   if (error) {
  //     debug('Indexing Error:', JSON.stringify(error));
  //   }
  //   if (response && 'statusCode' in response && response.statusCode !== 200) {
  //     debug('Indexing Error: %d %s', response.statusCode, response.message);
  //   }
  // });

  this.indexBuffer = '';
  this.bufferCount = 0;
};

module.exports = swsElasticEmitter;
