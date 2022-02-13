// WARNING: Requires: npm install mysql, npm install prompt-sync

// MySQL connection
const {createPool} = require('mysql')
const pool1 = createPool({
    host: "node-1.mysql.database.azure.com",
    user: "STADVDB",
    password: "MCO2_GRP14",
    connectionLimit: 10,
})
const pool2 = createPool({
    host: "node-2.mysql.database.azure.com",
    user: "STADVDB",
    password: "MCO2_GRP14",
    connectionLimit: 10,
})
const pool3 = createPool({
    host: "node-3.mysql.database.azure.com",
    user: "STADVDB",
    password: "MCO2_GRP14",
    connectionLimit: 10,
})

function askIsolationLevel() {
    const prompt = require("prompt-sync")()

    let ask = "Which Isolation Lavel Will You Choose?"
    let op1 = "[1] Read Uncommitted"
    let op2 = "\n[2] Read Committed"
    let op3 = "\n[3] Read Repeatable"
    let op4 = "\n[4] Serializable"
  
    const input = prompt(ask.concat(op1,op2,op3,op4))
    console.log(`Simulated: ${input}`)
    return input
}


function setIsolationLevel(num){
    let setSession = 'SET SESSION TRANSACTION ISOLATION LEVEL '

    switch(num){
      case "1":
       setSession = setSession.concat('Read Uncommitted')       
      break;

      case "2":
        setSession = setSession.concat('Read Committed') 
      break;

      case "3":
        setSession = setSession.concat('Read Repeatable') 
      break;

      case "4":
        setSession = setSession.concat('Serializable') 
      break;
    }

    pool1.query(setSession, function(err) {})
    pool2.query(setSession, function(err) {})
    pool3.query(setSession, function(err) {})
  
    return;
}

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

async function getNodeConn(node){
    switch(node){
        case 'node-1':
            return await pool1.getConnection();
        case 'node-2':
            return await pool2.getConnection();
        case 'node-3':
            return await pool3.getConnection();
        default:
            throw '404 node not found';
    }
}

async function executeQueryFromNode(node, query) {
    switch (node) {
        case 'node-1':
            return (await pool1.query(query))[0];
        case 'node-2':
            return (await pool2.query(query))[0];
        case 'node-3':
            return (await pool3.query(query))[0];

        default:
            throw '404 node not found';
    }
}
// Case #1: All transactions are reading.
function case1 () {
    try {
      
        // Node 1 READ transaction
        pool1.query(`select * from imdb_ijs.movies`, (err, res) =>{
            return console.log(res)
        })

        // Node 2 connection
        
        // Node 2 READ transaction
        pool2.query(`select * from imdb_ijs.movies`, (err, res) =>{
            return console.log(res)
        })

        // Node 3 connection
        
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
    try {
        // // Node 1 READ transaction
        pool1.query(`select * from imdb_ijs.movies where id <= 30`, (err, res) =>{
            return console.log(res)
        })
        
        // Node 2 UPDATE transaction
        pool2.getConnection(function(err, connection){
            if (err) throw err; // means not connected
            
            // use connection, begin transaction
            connection.beginTransaction(function(err) {
                // Read, wait for 8 seconds
                if (err) { throw err; }
                connection.query(`select * from imdb_ijs.movies where rank is null and id <= 30`, (err, res) =>{
                    return console.log(res)
                })
                console.log("-----FIRST READ STATEMENT-----")
                connection.query(`select sleep(8)`, (err, res) =>{
                    return console.log(res)
                })
                // Update movies with null ranks and id's <=30, set null ranks to 0.0
                connection.query(
                    `UPDATE imdb_ijs.movies
                    SET rank = 0.0
                    WHERE rank is null and id <=30;`, (err, res) =>{
                    if (err) { // safety net, rollback if update fails
                        return connection.rollback(function() {
                            throw err
                        })
                    }
                    return console.log(res)
                })
                console.log("-----UPDATE STATEMENT-----")
                // Read, wait for 10 seconds, rollback
                connection.query(`select * from imdb_ijs.movies where id <= 30`, (err, res) =>{
                    return console.log(res)
                })
                connection.query(`select sleep(10)`, (err, res) =>{
                    return console.log(res)
                })
                console.log("-----SECOND READ STATEMENT-----")
                // rollback changes every time to test what other nodes are reading
                connection.rollback()
                console.log('success!');
        })
    })
        
        // Node 3 READ transaction
        pool3.query(`select * from imdb_ijs.movies where id <= 30`, (err, res) =>{
            return console.log(res)
        })
     } catch (err) {
         console.log(err)
     }
}

// Case #3: All transactions are writing (update / deletion).
function case3() {

}
/*UPDATER UPDATES UPDATEE*/
function syncNodes(updatee,updater){
}
/* PARAMS
node:  node name
queryType: name of the CRUD operation
updater: Node that is doing the updating
updaterLastUpdate: When node last updated
data: data for CRUD operation
*/
async function sync(node,queryType,updater,updaterLastUpdate,data){
 const query= getCRUD(queryType,data);
 const queries = query.split(';');
 queries.pop();
 try{
     var conn = await startReplication(node);
     for(const query of queries){
         await conn.query(query);
     }
     await commitReplicationTransaction(conn);
     /*TODO: VERSION CONTROL FOR ROLLBACK*/
     await updateVersion();
 } catch(err){
     await rollbackReplication(conn);
     console.log(err);
 }
}
/*decides which crud operation to do for node synch*/
function syncCRUD(type,data){
    
    if(type === 'UPDATE'){
        var query = synchupdateQuery(data);
    }
    else if(type === 'DELETE'){
        var query = synchdeleteQuery(data);
    }
    if(type === 'INSERT'){
        var query = synchinsertQuery(data);
    }

}

async function startReplication(node){
    const conn = await getNodeConn(node);
    const start = 'start transaction';
    const repliflag = 'set @replicator := true';

    await conn.query(start);
    await conn.query(repliflag);
    conn.release();

}

async function commitReplication(conn){
    const commit = 'commit';
    const revertrepliflag = 'set @replicator := false';
    await conn.query(revertrepliflag);
    await conn.query(commit);
    conn.release();
}
async function rollbackReplication(){
    const rollback = 'rollback';
    await conn.query(rollback);
    conn.release();
}
// Simulate concurrency control

var isolationLevel = askIsolationLevel()
setIsolationLevel(isolationLevel)

var input = askSimulationCase()
simulateCase(input)
