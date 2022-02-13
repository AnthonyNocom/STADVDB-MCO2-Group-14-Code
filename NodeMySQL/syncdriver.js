const { syncNodes } = require('/syncdriver');
const nodes = [
    {
        name:'node-1',
        updatedby: ['node-2','node-3'],
        dependedby: ['node-2','node-3'],
    },
    {
        name:'node-2',
        updatedby: ['node-1','node-2','node-3'],
        dependedby: ['node-1','node-2','node-3'],
    },
    {
        name:'node-3',
        updatedby: ['node-1','node-2','node-3'],
        dependedby: ['node-1','node-2','node-3'],
    },


];
let syncing = false;

async function syncAll(){
    const procs = [];
    for(node of nodes){
        for(updatedby of node.updatedby){
            const proc = syncNode(node.name,updatedby);
            procs.push(proc);
        }
    }
    await Promise.all(procs);
    syncing = false;
}
async function startSync(){
    if(!syncing){
        syncing = true;
        await syncAll();
    }
    else{
        console.log("Already Synchronizing nodes");
    }
}

setInterval(startSync,1000);