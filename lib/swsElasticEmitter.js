/**
 * ElasticSearch Emitter. Store Request/Response records in Elasticsearch
 */
const elasticsearch = require('elasticsearch');
const AWS = require('aws-sdk');

const debug = require('debug')('sws:elastic');
const swsUtil = require('./swsUtil');
const moment = require('moment');

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
      hosts: [swsOptions[swsUtil.supportedOptions.elasticsearch]],
      log: 'error',
      connectionClass: require('http-aws-es')
    });
  } else {
    esClient = new elasticsearch.Client({
      hosts: [swsOptions[swsUtil.supportedOptions.elasticsearch]],
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
  esClient.indices.get({ index: 'api' }).then(
    (res) => {
      // index found nothing to do
    },
    (err) => {
      esClient.indices.putTemplate({ body: indexTemplate, name: 'api-*' }).then(
        () => {
          // do nothing
        },
        (err) => {
          console.error(err);
        }
      );
    }
  );
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

  esClient
    .bulk({
      body: this.indexBuffer
    })
    .then(
      (resp) => {},
      (err) => {
        console.error('error from elasticsearch', err);
      }
    );

  this.indexBuffer = '';
  this.bufferCount = 0;
};

module.exports = swsElasticEmitter;
