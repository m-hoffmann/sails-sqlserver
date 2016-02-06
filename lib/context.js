var _ = require('lodash');
_.str = require('underscore.string');
var utils = require('./utils');


var mssql = require("mssql");

var SqlContext = function (schema) {
  this._schema = _.clone(schema);
  this._parameters = [];
};

SqlContext.prototype = {

  getParameterTypeByValue: function (value) {

    switch (typeof value) {

      case "boolean":
        return mssql.Bit;

      case "string":
        return mssql.NVarChar(mssql.MAX);

      case "number":
        return mssql.Integer;

      case "object":
      case "symbol":
        return mssql.NVarChar(mssql.MAX);

      default:
        return mssql.NVarChar(mssql.MAX);

    }

  },

  getParameterTypeByAttribute: function (attr) {

    var size;

    switch (attr.type) {

      case 'binary':
        return mssql.VarBinary;

      case 'string':
        size = _.isNumber(attr.size) ? attr.size.toString() : "MAX";
        return mssql.NVarChar(size);

      case 'array':
      case 'json':
      case 'text':
        return mssql.NVarChar(mssql.MAX);

      case 'boolean':
        return mssql.Bit;

      case 'int':
      case 'integer':
        return mssql.Int;

      case 'float':
      case 'double':
        return mssql.Float;

      case 'date':
        return mssql.Date;

      case 'time':
        return mssql.Time;

      case 'datetime':
        return mssql.DateTime;

      default:
        console.error("Unregistered type given: " + attr.type);
        return mssql.NVarChar(mssql.MAX);
    }

  },

  getParameterType: function (collectionName, value, attrName) {
    var attr = this._schema[attrName];
    return attr ? this.getParameterTypeByAttribute(attr) : this.getParameterTypeByValue(value);
  },

  addParameter: function (collectionName, value, attrName) {

    var name = "p" + this._parameters.length;

    this._parameters.push({
      name: name,
      value: value,
      type: this.getParameterType(collectionName, value, attrName)
    });

    return "@" + name;
  },

  getParameters: function () {
    return this._parameters;
  },

  escapeId: function (val) {
    return "[" + val.replace(/'/g, "''") + "]";
  },

  escape: function (val, stringifyObjects, timeZone) {

    if (val === undefined || val === null) {
      return 'NULL';
    }

    switch (typeof val) {
      case 'boolean':
        return (val) ? '1' : '0';
      case 'number':
        return val + '';
    }

    if (typeof val === 'object') {
      val = val.toString();
    }

    val = val.replace(/[\']/g, function (s) {
      switch (s) {
        case "\'":
          return "''";
        default:
          return " ";
      }
    });
    if (/[^\u0000-\u00ff]/.test(val)) {
      return "N'" + val + "'";
    }
    return "'" + val + "'";
  },

  normalizeSchema: function (schema) {
    return _.reduce(schema, function (memo, field) {

      // Marshal mssql DESCRIBE to waterline collection semantics
      var attrName = field.ColumnName;
      var type = field.TypeName;

      memo[attrName] = {
        type: type
        //defaultsTo: field.Default
      };

      memo[attrName].autoIncrement = field.AutoIncrement;
      memo[attrName].primaryKey = field.PrimaryKey;
      memo[attrName].unique = field.Unique;
      memo[attrName].indexed = field.Indexed;
      memo[attrName].nullable = field.Nullable;

      return memo;
    }, {});
  },

  // @returns ALTER query for adding a column
  addColumn: function (collectionName, attrName, attrDef) {
    var tableName = collectionName;
    var columnDefinition = this._schema(collectionName, attrDef, attrName);
    return 'ALTER TABLE ' + tableName + ' ADD ' + columnDefinition;
  },

  // @returns ALTER query for dropping a column
  removeColumn: function (collectionName, attrName) {
    var tableName = collectionName;
    attrName = attrName;
    return 'ALTER TABLE ' + tableName + ' DROP COLUMN ' + attrName;
  },

  selectQuery: function (collectionName, options) {
    var query = utils.buildSelectStatement(options, collectionName);
    query += this.serializeOptions(collectionName, options);
    if (options.skip) {
      var outerOffsetQuery = 'SELECT ';
      if (options.limit) {
        outerOffsetQuery += 'TOP ' + options.limit + ' ';
      }
      outerOffsetQuery += '* FROM (' + query + ') __outeroffset__ WHERE __outeroffset__.__rownum__ > ' + options.skip + ' ';
      query = outerOffsetQuery;
    }
    return query;
  },

  insertQuery: function (collectionName, data) {
    var tableName = collectionName;
    return 'INSERT INTO ' + tableName + ' (' + this.attributes(collectionName, data) + ')' + ' VALUES (' + this.values(collectionName, data) + '); SELECT @@IDENTITY AS [id]';
  },

  // Create a schema csv for a DDL query
  schema: function (collectionName, attributes) {
    return this.build(collectionName, attributes, this._schema);
  },

  _schema: function (collectionName, attribute, attrName) {

    attrName = '[' + attrName + ']';

    var type = sqlTypeCast(attribute);

    if (attribute.primaryKey) {

      // If type is an integer, set auto increment
      if (type === 'INT') {
        return attrName + ' INT IDENTITY PRIMARY KEY';
      }

      // Just set NOT NULL on other types
      return attrName + ' VARCHAR(255) NOT NULL PRIMARY KEY';
    }

    // Process UNIQUE field
    if (attribute.unique) {
      return attrName + ' ' + type + ' UNIQUE';
    }

    return attrName + ' ' + type + ' NULL';
  },

  // Create an attribute csv for a DQL query
  attributes: function (collectionName, attributes) {
    return this.build(collectionName, attributes, this.prepareAttribute);
  },

  // Create a value csv for a DQL query
  // key => optional, overrides the keys in the dictionary
  values: function (collectionName, values, key) {
    return this.build(collectionName, values, this.prepareValue.bind(this), ', ', key);
  },

  updateCriteria: function (collectionName, values) {
    var query = this.build(collectionName, values, this.prepareCriterion.bind(this));
    query = query.replace(/IS NULL/g, '=NULL');
    return query;
  },

  prepareCriterion: function (collectionName, value, key, parentKey) {

    if (validSubAttrCriteria(value)) {
      return this.where(collectionName, value, null, key);
    }

    // Build escaped attr and value strings using either the key,
    // or if one exists, the parent key
    var attrStr, valueStr;


    // Special comparator case
    if (parentKey) {

      attrStr = this.prepareAttribute(collectionName, value, parentKey);

      if (key === '<' || key === 'lessThan') {
        return attrStr + '<' + this.prepareValue(collectionName, value, parentKey);
      }
      else if (key === '<=' || key === 'lessThanOrEqual') {
        return attrStr + '<=' + this.prepareValue(collectionName, value, parentKey);
      }
      else if (key === '>' || key === 'greaterThan') {
        return attrStr + '>' + this.prepareValue(collectionName, value, parentKey);
      }
      else if (key === '>=' || key === 'greaterThanOrEqual') {
        return attrStr + '>=' + this.prepareValue(collectionName, value, parentKey);
      }
      else if (key === '!' || key === 'not') {
        if (value === null) {
          return attrStr + ' IS NOT NULL';
        }
        else if (_.isArray(value)) {
          return attrStr + " NOT IN (" + this.values(collectionName, value, key) + ")";
        }
        else {
          return attrStr + '<>' + this.prepareValue(collectionName, value, parentKey);
        }
      }
      else if (key === 'like') {
        return attrStr + ' LIKE ' + this.prepareValue(collectionName, value, parentKey);
      }
      else if (key === 'contains') {
        return attrStr + ' LIKE ' + this.prepareValue(collectionName, '%' + value + '%', parentKey);
      }
      else if (key === 'startsWith') {
        return attrStr + ' LIKE ' + this.prepareValue(collectionName, value + '%', parentKey);
      }
      else if (key === 'endsWith') {
        return attrStr + ' LIKE ' + this.prepareValue(collectionName, '%' + value, parentKey);
      }
      else {
        throw new Error('Unknown comparator: ' + key);
      }
    }
    else {
      attrStr = this.prepareAttribute(collectionName, value, key);
      if (_.isNull(value)) {
        return attrStr + " IS NULL";
      }
      else {
        return attrStr + "=" + this.prepareValue(collectionName, value, key);
      }
    }
  },

  prepareValue: function (collectionName, value, attrName) {
    // Cast dates to SQL
    //if (_.isDate(value)) {
    //	value = toSqlDate(value);
    //}

    // Cast functions to strings
    if (_.isFunction(value)) {
      value = value.toString();
    }

    return this.addParameter(collectionName, value, attrName);
  },

  prepareAttribute: function (collectionName, value, attrName) {
    return '[' + attrName + ']';
  },

  // Starting point for predicate evaluation
  // parentKey => if set, look for comparators and apply them to the parent key
  where: function (collectionName, where, key, parentKey) {
    return this.build(collectionName, where, this.predicate.bind(this), ' AND ', undefined, parentKey);
  },

  // Recursively parse a predicate calculus and build a SQL query
  predicate: function (collectionName, criterion, key, parentKey) {

    var queryPart = '';

    if (parentKey) {
      return this.prepareCriterion(collectionName, criterion, key, parentKey);
    }

    // OR
    if (key.toLowerCase() === 'or') {
      queryPart = this.build(collectionName, criterion, this.where.bind(this), ' OR ');
      return ' ( ' + queryPart + ' ) ';
    }

    // AND
    else if (key.toLowerCase() === 'and') {
      queryPart = this.build(collectionName, criterion, this.where.bind(this), ' AND ');
      return ' ( ' + queryPart + ' ) ';
    }

    // IN
    else if (_.isArray(criterion)) {
      var values = this.values(collectionName, criterion, key) || 'NULL';
      queryPart = this.prepareAttribute(collectionName, null, key) + " IN (" + values + ")";
      return queryPart;
    }

    // LIKE
    else if (key.toLowerCase() === 'like') {
      return this.build(collectionName, criterion, (collectionName, value, attrName) => {
        var attrStr = this.prepareAttribute(collectionName, value, attrName);
        if (_.isRegExp(value)) {
          throw new Error('RegExp not supported');
        }
        var valueStr = this.prepareValue(collectionName, value, attrName);
        // Handle escaped percent (%) signs [encoded as %%%]
        valueStr = valueStr.replace(/%%%/g, '\\%');

        return attrStr + " LIKE " + valueStr;
      }, ' AND ');
    }

    // NOT
    else if (key.toLowerCase() === 'not') {
      throw new Error('NOT not supported yet!');
    }

    // Basic criteria item
    else {
      return this.prepareCriterion(collectionName, criterion, key);
    }

  },

  serializeOptions: function (collectionName, options) {

    var queryPart = '';

    if (options.where) {
      queryPart += 'WHERE ' + this.where(collectionName, options.where) + ' ';
    }

    if (options.groupBy) {
      queryPart += 'GROUP BY ';

      // Normalize to array
      if (!Array.isArray(options.groupBy)) {
        options.groupBy = [options.groupBy];
      }
      options.groupBy.forEach(function (key) {
        queryPart += key + ', ';
      });

      // Remove trailing comma
      queryPart = queryPart.slice(0, -2) + ' ';
    }

    //options are sorted during skip when applicable
    if (options.sort && !options.skip) {
      queryPart += 'ORDER BY ';

      // Sort through each sort attribute criteria
      _.each(options.sort, (direction, attrName) => {

        queryPart += this.prepareAttribute(collectionName, null, attrName) + ' ';

        // Basic MongoDB-style numeric sort direction
        if (direction === 1) {
          queryPart += 'ASC, ';
        }
        else {
          queryPart += 'DESC, ';
        }
      });

      // Remove trailing comma
      if (queryPart.slice(-2) === ', ') {
        queryPart = queryPart.slice(0, -2) + ' ';
      }
    }

    return queryPart;
  },

  build: function (collectionName, collection, fn, separator, keyOverride, parentKey) {

    separator = separator || ', ';
    var $sql = '';

    _.each(collection, function (value, key) {
      $sql += fn(collectionName, value, keyOverride || key, parentKey);

      // (always append separator)
      $sql += separator;
    });

    return _.str.rtrim($sql, separator);
  }

};

// Cast waterline types into SQL data types
function sqlTypeCast(attribute) {

  var size;
  var type = attribute.type;
  type = type && type.toLowerCase();

  switch (type) {

    case 'binary':
      return 'VARBINARY(max)';

    case 'string':
      size = _.isNumber(attribute.size) ? attribute.size.toString() : "MAX";
      return 'NVARCHAR(' + size + ')';

    case 'array':
    case 'json':
    case 'text':
      return 'NVARCHAR(max)';

    case 'boolean':
      return 'BIT';

    case 'int':
    case 'integer':
      return 'INT';

    case 'float':
    case 'double':
      return 'FLOAT';

    case 'date':
      return 'DATE';
    case 'time':
      return 'TIME';
    case 'datetime':
      return 'DATETIME';

    default:
      console.error("Unregistered type given: " + type);
      return "VARCHAR";
  }
}

function wrapInQuotes(val) {
  return '"' + val + '"';
}

function toSqlDate(date) {
  date = date.getUTCFullYear() + '-' +
    ('00' + (date.getUTCMonth() + 1)).slice(-2) + '-' +
    ('00' + date.getUTCDate()).slice(-2) + ' ' +
    ('00' + date.getUTCHours()).slice(-2) + ':' +
    ('00' + date.getUTCMinutes()).slice(-2) + ':' +
    ('00' + date.getUTCSeconds()).slice(-2);

  return date;
}

function validSubAttrCriteria(c) {
  return _.isObject(c) && (
    !_.isUndefined(c.not) || !_.isUndefined(c.greaterThan) || !_.isUndefined(c.lessThan) || !_.isUndefined(c.greaterThanOrEqual) || !_.isUndefined(c.lessThanOrEqual) || !_.isUndefined(c['<']) || !_.isUndefined(c['<=']) || !_.isUndefined(c['!']) || !_.isUndefined(c['>']) || !_.isUndefined(c['>=']) || !_.isUndefined(c.startsWith) || !_.isUndefined(c.endsWith) || !_.isUndefined(c.contains) || !_.isUndefined(c.like));
}

module.exports = SqlContext;
