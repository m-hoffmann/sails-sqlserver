### sails-sqlserver

### 1. Install
```sh
$ npm install sails-sqlserver --save
```

### 2. Configure

#### `config/models.js`
```js
{
  connection: 'sqlserver'
}
```

#### `config/connections.js`
```js
{
  sqlserver: {
    adapter: 'sails-sqlserver',
    user: 'cnect',
    password: 'pass',
    host: 'abc123.database.windows.net' // azure database
    database: 'mydb',
    options: {
      encrypt: true   // use this for Azure databases
    }
  }
}
```

## License
MIT
