const MongoClient = require('mongodb').MongoClient
const axios = require('axios');
const BN = require('bn.js');
const fs = require('fs');
const { getLastRecordAddedToDb, getSumarizedDataFromDb, getMapKey, getSumarizedData, getAndUpdateSumarizedData } = require('./src/lib/helper');

let mapSumarizedByDate = new Map();
const RESUME_PATH = "./nextIndex.json";
let recordToBeUpdate = null;
const url = 'mongodb://localhost:27017';
const dbName = 'metricDB';
const totalRequests = 100;


const getTransactions = async (next, amountRequests) => {
    console.log('call getTransactions');
    try{    
        const response = await axios.get('api-url', {
            params: {
                module: 'transactions',
                action: 'getTransactions',
                next: next,
                limit: 500
            }
        });

        console.log(response.data.data.length);
        amountRequests++;

        const transactions = response.data.data;
        transactions.forEach(tx => {
            let txDate = new Date(tx.timestamp * 1000);
            let mapKey = getMapKey(txDate);            
            
            if (mapSumarizedByDate.has(mapKey)){                
                let summary = getAndUpdateSumarizedData(mapSumarizedByDate, mapKey, tx);
                mapSumarizedByDate.set(mapKey, summary); 
            }
            else{
                mapSumarizedByDate.set(mapKey, getSumarizedData(tx));
            }
        });
        
        console.log(amountRequests);
        console.log(totalRequests);
        recordNextIndexToBeProcessed(response.data.pages.next);

        if (response.data.pages.next && amountRequests < totalRequests){
            await getTransactions(response.data.pages.next, amountRequests);
        }
    }
    catch(err){
        console.log(err);
    }
};

const recordNextIndexToBeProcessed = (index) => {
    fs.writeFileSync(RESUME_PATH, index.toString());
}

const getNextIndexToBeProcessed = () => {
    if (fs.existsSync(RESUME_PATH)) {
        return fs.readFileSync(RESUME_PATH, 'utf8');        
    }

    return null;
}

const setMapSumarizedForLastRecordDb = async (db) => {
    let dbRecord = await getLastRecordAddedToDb(db);
    let summary = getSumarizedDataFromDb(dbRecord);    
    if (summary !== null){
        console.log("Last record processed: " + dbRecord._id);
        mapSumarizedByDate.set(dbRecord._id, summary);
        recordToBeUpdate = dbRecord._id;
    }
}

(async () => {    
    const client = new MongoClient(url, { useUnifiedTopology: true });
    await client.connect();
    const db = client.db(dbName);

    await setMapSumarizedForLastRecordDb(db);
    let next = getNextIndexToBeProcessed();    
    await getTransactions(next, 0);
        
    //Get the data!
    let allPromises = [];
    console.log(mapSumarizedByDate);
    mapSumarizedByDate.forEach((value, key) => {         
        if (recordToBeUpdate !== null && recordToBeUpdate === key){
            console.log('update');
            allPromises.push(db.collection('txsSummary').updateOne({"_id": key }, { $set: {value}}));
        }else{
            console.log('insert');
            allPromises.push(db.collection('txsSummary').insertOne({"_id": key, value}));
        }        
    });

    await Promise.all(allPromises);
    await client.close();
})();