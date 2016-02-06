var mssql = require("mssql");
var _ = require("lodash");


/*
 * @TODO Create a way to override type casts per connection.
 * To prevent problems with unicode data, nvarchar is used for: string, json, text, array
 */
var cast = {

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

    var type = attr.type && attr.type.toLowerCase();

    switch (type) {

      case 'binary':
        return mssql.VarBinary(mssql.MAX);

      case 'string':
        return mssql.NVarChar(_.isNumber(attr.size) && attr.size <= 8000 ? attr.size.toString() : mssql.MAX);

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

  getSqlTypeByAttribute: function (attr) {

    var type = attr.type && attr.type.toLowerCase();

    switch (type) {

      case 'binary':
        return 'VARBINARY(max)';

      case 'string':
        return 'NVARCHAR(' + (_.isNumber(attr.size) && attr.size <= 8000 ? attr.size.toString() : "MAX") + ')';

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


};

module.exports = cast;
