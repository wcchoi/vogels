'use strict';

var _            = require('lodash'),
    util         = require('util'),
    AWS          = require('aws-sdk'),
    DOC          = require('dynamodb-doc'),
    Table        = require('./table'),
    Schema       = require('./schema'),
    serializer   = require('./serializer'),
    batch        = require('./batch'),
    Item         = require('./item'),
    createTables = require('./createTables'),
    updateTables = require('./updateTables');

var vogels = module.exports;

vogels.AWS = AWS;

var internals = {};

vogels.dynamoDriver = internals.dynamoDriver = function (driver) {
  if(driver) {
    internals.dynamodb = driver;

    var docClient = internals.loadDocClient(driver);
    internals.updateDynamoDBDocClientForAllModels(docClient);
  } else {
    internals.dynamodb = internals.dynamodb || new vogels.AWS.DynamoDB({apiVersion: '2012-08-10'});
  }

  return internals.dynamodb;
};

internals.updateDynamoDBDocClientForAllModels = function (docClient) {
  _.each(vogels.models, function (modelVersions) {
      _.each(modelVersions, function(model) {
        model.config({docClient: docClient});
      });
  });
};

internals.loadDocClient = function (driver) {
  if(driver) {
    internals.docClient = new DOC.DynamoDB(driver);
  } else {
    internals.docClient = internals.docClient || new DOC.DynamoDB(internals.dynamoDriver());
  }

  return internals.docClient;
};

internals.compileModel = function (name, schema, internal) {

  // extremly simple table names
  var tableName = name.toLowerCase() + 's';

  var table = new Table(tableName, schema, serializer, internals.loadDocClient());

  var Model = function (attrs) {
    Item.call(this, attrs, table);
  };

  util.inherits(Model, Item);

  Model.get          = _.bind(table.get, table);
  Model.create       = _.bind(table.create, table);
  Model.update       = _.bind(table.update, table);
  Model.destroy      = _.bind(table.destroy, table);
  Model.query        = _.bind(table.query, table);
  Model.scan         = _.bind(table.scan, table);
  Model.parallelScan = _.bind(table.parallelScan, table);

  Model.getItems = batch(table, serializer).getItems;
  Model.batchGetItems = batch(table, serializer).getItems;

  // table ddl methods
  Model.createTable   = _.bind(table.createTable, table);
  Model.updateTable   = _.bind(table.updateTable, table);
  Model.describeTable = _.bind(table.describeTable, table);
  Model.deleteTable   = _.bind(table.deleteTable, table);
  Model.tableName     = _.bind(table.tableName, table);
  Model.version       = table.schema.version;
  Model.getConfig     = function() { return table.schema.config };

  table.itemFactory = Model;

  // hooks
  Model.after  = _.bind(table.after, table);
  Model.before = _.bind(table.before, table);

  /* jshint camelcase:false */
  Model.__defineGetter__('docClient', function(){
    return table.docClient;
  });

  Model.config = function(config) {
    config = config || {};

    if(config.tableName) {
      table.config.name = config.tableName;
    }

    if (config.docClient) {
      table.docClient = config.docClient;
    } else if (config.dynamodb) {
      table.docClient = new DOC.DynamoDB(config.dynamodb);
    }

    return table.config;
  };

  if(internal) {
    return vogels.internalModel(name, Model);
  } else {
    return vogels.model(name, Model.version, Model);
  }
};

internals.addModel = function (name, model, version) {
  if(version === undefined) version = model.version;
  vogels.models[name] = vogels.models[name] || [];
  vogels.models[name][version] = model

  return vogels.models[name][version];
};

vogels.reset = function () {
  vogels.models = {};
  vogels.internalModels = {};
};

vogels.Set = function () {
  return internals.docClient.Set.apply(internals.docClient, arguments);
};

vogels.define = function (modelName, config, internal) {
  if(internal === undefined) internal = false;
  if(_.isFunction(config)) {
    throw new Error('define no longer accepts schema callback, migrate to new api');
  }

  var schema = new Schema(config);

  var compiledTable = internals.compileModel(modelName, schema, internal);

  return compiledTable;
};

vogels.model = function(name, version, model) {
  if(model) {
    internals.addModel(name, model, version);
  }
  if(version === undefined) version = vogels.models[name].length - 1

  return vogels.models[name][version] || null;
};

vogels.latestVersionModels = function() {
    return _.map(_.keys(vogels.models), function(modelName) {
        var modelVersions =  vogels.models[modelName];
        return modelVersions[modelVersions.length - 1];
    })
}

internals.addInternalModel = function(name, model) {
  vogels.internalModels[name] = model;

  return vogels.internalModels[name];
};

vogels.internalModel = function(name, model) {
  if(model) {
    internals.addInternalModel(name, model);
  }

  return vogels.internalModels[name] || null;
};

vogels.updateTables = function (options, callback) {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  return updateTables(vogels.latestVersionModels(), options, callback);
};

vogels.createTables = function (options, callback) {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  return createTables(vogels.latestVersionModels(), options, callback);
};

vogels.types = Schema.types;

vogels.reset();
