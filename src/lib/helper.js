const { txTypes } = require('./types');

const getMapKey = (date) => {    
    return date.getUTCFullYear() + "_" + date.getMonth() + "_" + date.getDay();
}

const setSummaryData = (tx, summary, isUpdate) => {
    switch (tx.txType) {
        case txTypes.default:
            isUpdate ? summary.normal++ : summary.normal = 1;
            break;
        case txTypes.remasc:
            isUpdate ? summary.remasc++ : summary.remasc = 1;
            break;
        case txTypes.bridge:
            isUpdate ? summary.bridge++ : summary.bridge = 1;
            break;
        case txTypes.contract:
            isUpdate ? summary.contractDeploy++ : summary.contractDeploy = 1;
            break;
        case txTypes.call:
            isUpdate ? summary.contractCall++ : summary.contractCall = 1;
            break;
        default:
            break;
    }
}

const getAndUpdateSumarizedData = (mapSumarizedByDate, mapKey, tx) => {
    let summary = mapSumarizedByDate.get(mapKey);    
    setSummaryData(tx, summary, true);
    summary.total++;
    return summary;
}

const getSumarizedData = (tx) => {
    let summary = {
        normal: 0,
        remasc: 0,
        bridge: 0,
        contractDeploy: 0,
        contractCall: 0,
        total: 0
    };

    setSummaryData(tx, summary, false);

    summary.total = 1;
    return summary;
}

const getSumarizedDataFromDb = (record) => {

    if (!record){
        return null;
    }
    
    let summary = {
        normal: record.value.normal,
        remasc: record.value.remasc, 
        bridge: record.value.bridge,
        contractDeploy: record.value.contractDeploy,
        contractCall: record.value.contractCall,
        total: record.value.total
    };

    return summary;    
}

const getLastRecordAddedToDb = async (db) => {
    let results = await db.collection('txsSummary').find().sort({_id: 1}).limit(1).toArray();    
    
    if (results != null && results.length > 0 ){
        return results[0];
    }

    return null;
}

module.exports = { 
    getLastRecordAddedToDb, 
    getSumarizedDataFromDb, 
    getSumarizedData, 
    getAndUpdateSumarizedData, 
    setSummaryData, 
    getMapKey 
}