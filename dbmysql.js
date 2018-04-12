var mysql = require('mysql2');
//var mysql = require('mysql');

/*
 * conf:
 *      host, port, socketPath, user, password, database, charset
 */
class gdb{

    constructor(){
        this.name   = null; //data base (schema?) name
        this.schema = null;
        this.conf   = null;
        this.pool   = null;
        this.error  = null;
        this.verbose= 0;
    }

    _mergeProps(obj, prop){
        for(var p in prop)
            if( !obj.hasOwnProperty(p) )obj[p] = prop[p];
        return obj;
    }

    connect(conf, schema){
        this.name   = conf.database;
        this.schema = schema;
        this._makeFieldList();
        this.conf = this._mergeProps(conf, { host: '127.0.0.1', port: 3306, user: 'root', password: '', charset: 'utf8_general_ci', supportBigNumbers: true, dateStrings: true});
        if( this.verbose>2 )console.log('connect: ', conf.host+':'+conf.port, 'as user:'+conf.user, 'charset:', conf.charset, 'date as string: ', conf.dateStrings );
        this.pool = mysql.createPool(this.conf);
    }

    _makeFieldList(){
        for(var t in this.schema){
            var fldlist = {};
            for(var fld in this.schema[t].fields )
                fldlist[ this.schema[t].fields[fld].name ] = this.schema[t].fields[fld];
            this.schema[t].fldlist = fldlist;
        }
    }

    _check(){
        this.error = '';
        if( !this.conf ){
            this.error = "Connection not found, call connect before calling this API";
            return false;
        }
        if( !this.schema ){
            this.error = "Schema not found, call connect (with schema) before calling this API";
            return false;
        }
        if( !this.name ){
            this.error = "Database name (schema) not set, call connect";
            return false;
        }
        return true;
    }

    // use this escape only for data (since it wraps the result arround quotes)
    escape(val){
        return this.pool.escape(val);
    }

    // use this escape for field names query parts etc
    escapeId(val){
        return this.pool.escapeId(val);
    }

    print(ctx, sql, params, err, rows){
        if( !this.verbose )return;
        if( this.verbose>2 || err )console.log(ctx+":", sql, params, 'err:', err, rows);
        else if( this.verbose>1 )console.log(ctx+":", sql, params, 'err:', err);
        else if( this.verbose>0 && err )console.log(ctx+":", err);
    }

    query(sql, params, cb){
        if( !this._check() )return cb?cb(true):null;
        var self = this;
        this.pool.query(sql, params, function(err, rows, flds){
            self.print('query', sql, params, err, rows);
            if(cb)cb(err, rows, flds);
        });        
    }
    
    insert(sql, data, cb){
        if( this.schema.hasOwnProperty(sql) )
            this.query(sql, data, cb);
        else
            this._insertTable(sql, data, false, cb);
    }

    // get a single row, single col value as return value
    avalue(sql, params, cb){
        this.query(sql, params || [], function(err, rows, flds){
            if( rows.length>0 )return cb(err, rows[0][0], flds);
            cb('not found', null, flds);
        });
    }

    arow(sql, params, cb){
        this.query(sql, params, function(err, rows, flds){
            if( !err && rows.length>0 )cb(err, rows[0], flds);
            if(cb)cb('not found', null, flds);
        });
    }

    arows(sql, params, cb){
        this.query(sql, params, function(err, rows, flds){
            if(cb)cb(err, rows, flds);
        });
    }

    // private functions, do not call them directly, if you donot understand the implications
    //
    _insertTable(table, data, updt, cb){

        if( !data ){
            this.error = 'insert: invalid data param';
            return cb(this.error);
        }
        
        if( !(data instanceof Array) )data = [data];
        var fields = this.schema[table].fldlist;
        var primary = null;
        var primaryFunc = '';
        var prefix = "";
        for(var fld in fields ){
            if( fields[fld].primary && fields[fld].func ){
                primary = fld;
                primaryFunc = fields[fld].func;
                if( fields[fld].func_param )
                    prefix = fields[fld].func_param;
                else
                    prefix = primary[0].toUpperCase();
                break;
            }
        }

        var qfields = [];
        for(var fld in data[0]){
            if( !fields.hasOwnProperty(fld) || (fields[fld].auto && !data[0][fld])  || fld == primary )continue;
            if( fields.hasOwnProperty(fld) )qfields.push(fld);
        }
        //console.log('fields:', fields);
        var values = [];
        var holders = [];
        for(var d of data){
            var row = [];
            if( primary ){
                //console.log('primary:', primary, d[primary]);
                if(d.hasOwnProperty(primary) && d[primary] ) // if key field already present and valid, do not use the func to generate one
                    row.push(d[primary]);
                else
                    row.push( {toSqlString: function(){return primaryFunc+"('"+table+"', '"+prefix+"')"} } );
            }
            for(var fname of qfields ){
                row.push( d.hasOwnProperty(fname) ? d[fname]:'' );
            }
            values.push(row);
            holders.push('(?)');
        }

        if( primary )
            var sql = "insert into "+table+" ("+primary+","+qfields.join(',')+") values  "+holders.join(',');
        else
            var sql = "insert into "+table+" ("+qfields.join(',')+") values  "+holders.join(',');
        if (updt){
            sql += " on duplicate key update ";
            for(var fname of qfields ){
                if( !fields[fname].primary && !fields[fname].auto && fname!='uuid')
                    sql += fname+"=values("+fname+"),";
            }
            sql = sql.substring(0, sql.length-1);
        }
        //console.log(sql, values[0].length);
        this.pool.query(sql, values, function(err, rows, flds){
            //console.log(this.sql);
            cb(err, rows, flds)
        });
    }


    // to be called only if you are exiting the current applicatino
    // do not call in service (like http server) mode.
    //
    end(){
        if( this.pool ){
            if(this.verbose)console.log('closing mysql connection pool.');
            this.pool.end();
        }
    }

    _exec(sql, params){
        var self = this;
        return new Promise( (resolve, reject)=>{
            self.query(sql, params, (err, rows, flds)=>{
                if( err ){self.error = err; reject(err);}
                else resolve([rows, flds])
            })
        });
    }

    _insertOrUpdate(table, data){
        var self = this;
        return new Promise( (resolve, reject)=>{
            self._insertTable(table, data, true, (err, rows, flds)=>{
                if( err ){self.error = err; reject(err);}
                else resolve([rows, flds]);
            })
        });
    }

    // synchronous apis, if the return value === null, its error (error text in .error)
    //
    async exec(sql, params){
        try{
            return await this._exec(sql, params);
        }catch(e){
            this.error = e.message;
            return null;
        }
    }

    async value(sql, params){
        try{
            const [rows, flds] = await this._exec(sql, params);
            if( rows.length>0 )return rows[0][flds[0].name];
            return null;
        }catch(e){
            this.error = e.message;
            return null;
        }
    }

    async rows(sql, params){
        try{
            const [rows, flds] = await this._exec(sql, params);
            return rows;
        }catch(e){
            this.error = e.message;
            return null;
        }
    }
    
    async row(sql, params){
        try{
            const [rows, flds] = await this._exec(sql, params);
            return ( rows.length>0 ) ? rows[0] : null;
        }catch(e){
            this.error = e.message;
            return null;
        }
    }

    async map(sql, params, key, vcol){
        try{
            const [rows, flds] = await this._exec(sql, params);
            var obj = {};
            key = key || flds[0].name;
            if( vcol ){
                
                for(var row of rows )
                    obj[ row[key] ] = row[vcol];
                return obj;                    
            }
            for(var row of rows ){
                obj[ row[key] ] = row;
            }
            return obj;
        }catch(e){
            this.error = e.message;
            return null;
        }
    }

    async array(sql, params, key){
        try{
            const [rows, flds] = await this._exec(sql, params);
            key = key || flds[0].name;
            return Array.from(rows, x => x[key]);
            //for(var row of rows )arr.push( row[key] );
            //return arr;
        }catch(e){
            this.error = e.message;
            console.log(e.message);
            return null;
        }
    }


    async insertOrUpdate(table, data){
        try{
            const [rows, flds] = await this._insertOrUpdate(table, data);
            return rows;
        }catch(e){
            this.error = e.message;
            return null;
        }
    }

}

var _db = new gdb();
module.exports = _db;

// add reference to upgrade.
_db.upgrade = function( cb ){
    var upgrade = require('./upgrade');
    var upg = new upgrade(_db);
    upg.check( cb );
}
