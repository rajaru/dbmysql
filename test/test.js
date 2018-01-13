var db = require('../index');
var conf = { port: 9090, user: 'root', password: 'ganesh', database: 'kappid'};
var schema = {
    'test_table' : {
        fields: [
            {name: 'fld1', type: 'bigint', primary: true, null: 'NO'},
            {name: 'fld2', type: 'varchar', size: 20},
            {name: 'fld3', type: 'bigint'},
            {name: 'fld4', type: 'int'},
            {name: 'fld5', type: 'blob'},
            {name: 'fld6', type: 'timestamp'},
            {name: 'fld7', type: 'datetime'},
        ],
        index: [['fld1', 'fld2'],['fld3', 'fld2']],
        unique: ['fld1', 'fld7']
    },
    'loglevel' : {
        fields: [
            {name: 'log_id', type: 'bigint', primary: true, null: 'NO'},
            {name: 'name', type: 'varchar', size: 20},
            {name: 'user', type: 'bigint'},
            {name: 'at', type: 'int'},
            {name: 'u_ts', type: 'timestamp', default: ''},
        ],
        index: ['name', 'user'],
        unique: ['log_id']
    }
    
};

db.connect(conf, schema);

async function insertTest(){
    var res = await db.insertOrUpdate('test_table', [ {fld1:'3343', fld2:'Teasdst', fld3: 890}, {fld1:26, fld2:'Test', fld3: 9990}]);
    if( res === null ){
        console.log('insert failed, ', db.error);
    }
    else{
        console.log('insert completed...');
    }
    db.end();
}

async function queryTest(){
    console.log('exec: ',await db.exec("select * from test_table"));
    console.log('value: '+await db.value("select count(*) as  c from test_table"));
    console.log('rows: ',await db.rows("select * from test_table"));
    db.end();
}

function upgradeTest(){
    db.upgrade(function(){
        console.log('upgrade completed');
        db.end();
    });    
}

//queryTest();
insertTest();
//upgradeTest();