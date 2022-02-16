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
    let op2 = "[2] Read Committed"
    let op3 = "[3] Read Repeatable"
    let op4 = "[4] Serializable"
	
	ask = ask.concat('\n',op1,'\n',op2,'\n',op3,'\n',op4,'\n')
	console.log(ask);
	
    const input = prompt('')
    console.log(`Simulated: ${input}`)
    return input
}


function setIsolationLevel(num){
    let setSession = 'SET SESSION TRANSACTION ISOLATION LEVEL '

    switch(num){
      case "1":
       setSession = setSession.concat('READ UNCOMMITTED')       
      break;

      case "2":
        setSession = setSession.concat('READ COMMITTED') 
      break;

      case "3":
        setSession = setSession.concat('REPEATABLE READ') 
      break;

      case "4":
        setSession = setSession.concat('SERIALIZABLE') 
      break;
    }

    pool1.on('connection', function (connection) {
        connection.query(setSession)
    });
    pool2.on('connection', function (connection) {
        connection.query(setSession)
    });
    pool3.on('connection', function (connection) {
        connection.query(setSession)
    });
  
    return;
}

// Prompts the user which case to simulate, returns 1, 2, or 3
function askSimulationCase() {
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
        pool1.query(`select * from imdb_ijs.movies where id <= 30`, (err, res) =>{
            return console.log(res)
        })

        // Node 2 READ transaction
        pool2.query(`select * from imdb_ijs.movies where id <= 30`, (err, res) =>{
            return console.log(res)
        })

        // Node 3 READ transaction
        pool3.query(`select * from imdb_ijs.movies where id <= 30`, (err, res) =>{
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
                connection.release();
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
    try {
        // Update first 10 movies and change their years to 1920
        // Node 1 UPDATE transaction
        pool1.getConnection(function (err, connection) {
            if (err) throw err
            connection.beginTransaction(function (err) {
                if (err) throw err
                connection.query(`UPDATE imdb_ijs.movies
                                SET year = 1920
                                WHERE id <= 10;`, (err, res) => {
                    if (err) {
                        return connection.rollback(function() {
                            throw err
                        })
                    }
                    return console.log(res)
                })
                connection.query(`select sleep(30)`, (err, res) =>{
                    return console.log(res)
                })
                connection.rollback()
            })
        })
        
        // Update the ranks of all null-rank movies with id's less than 30. Change the ranks to 0.0
        // Node 2 UPDATE transaction
        pool2.getConnection(function(err, connection){
            if (err) throw err;
            
            connection.beginTransaction(function(err) {
                if (err) throw err;
                connection.query(`select * from imdb_ijs.movies where rank is null and id <= 30`, (err, res) =>{
                    return console.log(res)
                })
                console.log("-----FIRST READ STATEMENT-----")
                connection.query(`select sleep(8)`, (err, res) =>{
                    return console.log(res)
                })
                connection.query(
                    `UPDATE imdb_ijs.movies
                    SET rank = 0.0
                    WHERE rank is null and id <=30;`, (err, res) =>{
                    if (err) {
                        return connection.rollback(function() {
                            throw err
                        })
                    }
                    return console.log(res)
                })
                console.log("-----UPDATE STATEMENT-----")
                connection.query(`select * from imdb_ijs.movies where id <= 30`, (err, res) =>{
                    return console.log(res)
                })
                connection.query(`select sleep(10)`, (err, res) =>{
                    return console.log(res)
                })
                console.log("-----SECOND READ STATEMENT-----")
                connection.rollback()
                console.log('success!');
                connection.release();
        })
    })
        
        // Delete the movies with id's 21 to 30
        // Node 3 DELETE transaction
        pool3.getConnection(function (err, connection) {
            if (err) throw err

            connection.beginTransaction(function (err) {
                if (err) throw err

                connection.query(`select sleep(10)`, (err, res) =>{
                    return console.log(res)
                })
                
                connection.query(`select * from imdb_ijs.movies where id > 20 AND id <= 30`, (err, res) =>{
                    return console.log(res)
                })
                connection.query(`DELETE FROM imdb_ijs.movies
                                WHERE id > 20 AND id <= 30`, (err, res) => {
                    if (err) {
                        return connection.rollback(function() {
                            throw err
                        })
                    }
                    return console.log(res)
                })
                connection.query(`select sleep(40)`, (err, res) =>{
                    return console.log(res)
                })
                connection.rollback()
            })
        })
     } catch (err) {
         console.log(err)
     }
}
function getYear(data,query){
    for(year in data){
        replicateDataIntoNodes(query,year);
    }
  
}
function replicateDataIntoNodes(query, year){
	if(year >= 1980)
	{
		try{
			pool1.query(query)
			pool3.query(query)
		}catch (err){
			console.log(err)
		}
	}
	else
	{
		try{
			pool1.query(query)
			pool2.query(query)
		}catch (err){
			console.log(err)
		}
	}
}
// /*UPDATER UPDATES UPDATEE*/
// function syncNodes(updatee,updater){
//     try{
//         const { recentUpdate : recendUpdate } = await getRecentUpdate(updater,updatee);
//         const logs = await getChangelogs(node,recentUpdate);
//     }
    

// }

// async function getRecentUpdate(updater,node){
//     const query1 = `   select * from imdb_ijs.changelog where updater = '${updater}'`;
//     const [getRecentUpdate] = await executeQueryFromNode(node,query1);
//     return {
//         ...getRecentUpdate,
//         last_update = new Date().toJSON().slice(0, 19).replace('T', ' '),
//     };
// }
/* PARAMS data holds the data to add for update, or the data to delete, or data to insert
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
     await commitReplication(conn);
     /*TODO: VERSION CONTROL FOR ROLLBACK*/
 } catch(err){
     await rollbackReplication(conn);
     console.log(err);
 }
}

async function getChangelogs(node, timestamp = new DateTime("January 1, 1970 00:00:00")){
    timestamp = new Date().toJSON().slice(0, 19).replace('T', ' ')
    const query = `select * from imdb_ijs.changelog where timestamp > '${timestamp}'`; 
    const results = await executeQueryFromNode(node,query);
    return results.map((data) => ({
        ...data,
        timestamp: timestamp,
    }));
}

async function synclogs(logs,updater,updatee){
    for(log of logs){
        const data = {
            node: updatee,
            name: log.name.replaceAll("'", "\\'").replaceAll('"', '\\"'),
            year: log.year,
            rank: log.rank,
            old_name: log.old_name,
            old_year: log.old_year,
            old_rank: log.old_year,
        }
        await sync(node,log.crudop,updater,updaterLastUpdate,data);
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
