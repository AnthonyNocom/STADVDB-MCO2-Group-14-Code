const {createPool} = require('mysql')

const pool1 = createPool({
    host: "node-1.mysql.database.azure.com",
    user: "STADVDB",
    password: "MCO2_GRP14",
    connectionLimit: 10,
})

pool1.query(`select * from imdb_ijs.movies`, (err, res) =>{
    return console.log(res)
})

const pool2 = createPool({
    host: "node-2.mysql.database.azure.com",
    user: "STADVDB",
    password: "MCO2_GRP14",
    connectionLimit: 10,
})

pool2.query(`select * from imdb_ijs.movies`, (err, res) =>{
    return console.log(res)
})

const pool3 = createPool({
    host: "node-3.mysql.database.azure.com",
    user: "STADVDB",
    password: "MCO2_GRP14",
    connectionLimit: 10,
})

pool3.query(`select * from imdb_ijs.movies`, (err, res) =>{
    return console.log(res)
})