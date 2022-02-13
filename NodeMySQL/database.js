// WARNING: Requires: npm install mysql, npm install prompt-sync

// MySQL connection
const {createPool} = require('mysql')


// Prompts the user which case to simulate, returns 1, 2, or 3
function promptUser() {
    const prompt = require("prompt-sync")()

    const input = prompt("Input Case to Simulate (1, 2, or 3): ")

    console.log(`Simulated: ${input}`)

    return input
}

// Simulates the case based on parameter caseNum, otherwise, exits.
function simulateCase(caseNum) {
    switch (caseNum) {
        case "1": {
            case1()
            break;
        }
        case "2": {
            case2()
            break;
        }
        case "3": {
            case3()
            break;
        }
        default: {
            console.log("Please select a valid case to simulate\n")
            console.log("Closing...")
        }
        return;
    }
}

// Case #1: All transactions are reading.
function case1 () {
    try {
        // Node 1 connection
        const pool1 = createPool({
            host: "node-1.mysql.database.azure.com",
            user: "STADVDB",
            password: "MCO2_GRP14",
            connectionLimit: 10,
        })
        // Node 1 READ transaction
        pool1.query(`select * from imdb_ijs.movies`, (err, res) =>{
            return console.log(res)
        })

        // Node 2 connection
        const pool2 = createPool({
            host: "node-2.mysql.database.azure.com",
            user: "STADVDB",
            password: "MCO2_GRP14",
            connectionLimit: 10,
        })
        // Node 2 READ transaction
        pool2.query(`select * from imdb_ijs.movies`, (err, res) =>{
            return console.log(res)
        })

        // Node 3 connection
        const pool3 = createPool({
            host: "node-3.mysql.database.azure.com",
            user: "STADVDB",
            password: "MCO2_GRP14",
            connectionLimit: 10,
        })
        // Node 3 READ transaction
        pool3.query(`select * from imdb_ijs.movies`, (err, res) =>{
            return console.log(res)
        })
     } catch (err) {
         console.log(err)
     }
}

// Case #2: At least one transaction is writing (update / deletion) and the others are reading.
function case2() {

}

// Case #3: All transactions are writing (update / deletion).
function case3() {

}

// Simulate concurrency control
var input = promptUser()
simulateCase(input)