const mysql = require('mysql');

try {
    const pool = mysql.createConnection({
        connectionLimit: process.env.SQL_CONNECTIONLIMIT,
        host: process.env.SQL_HOST,
        user: process.env.SQL_USER,
        password: process.env.SQL_PASSWORD,
        database: process.env.SQL_DATABASE,
        multipleStatements: true
    })
    module.exports = pool
}
catch (error) {
    console.log(error)
}