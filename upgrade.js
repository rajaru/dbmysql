var db = require('./dbmysql');

class gupgrade{
    constructor(db){
        this.db = db;
        this.logs   = [];
        this.errors = [];
        this.sqls   = [];
    }

    log(msg){
        console.log(msg);
        this.logs.push(msg);
    }

    error(msg){
        console.log(msg);
        this.errors.push(msg);
    }

    processTableList(tables, cb){
        if( tables.length==0 )return cb.apply(this);
        var tname = tables.shift();
        this.checkTable( tname, this.db.schema[tname], function(){
            this.updateIndices(tname, this.db.schema[tname], function(){
                this.processTableList(tables, cb);
            });
        });
    }

    processQueries(sqls, cb){
        if( sqls.length==0 ){
            if( cb )cb(false, this.logsText()+this.errorsText());
            return;
        }
        var sql = sqls.shift();
        var self = this;
        this.db.query(sql, [], function(err, rows, flds){
            if( err ){
                console.log('failed to execute query ', sql);
                console.log(err);
            }
            self.processQueries(sqls, cb);
        })
    }

    //check all tables and fields, accumulate all the queries to be executed and 
    // run them one at a time
    //
    check(cb){
        this.processTableList(Object.keys(this.db.schema), function(){
            console.log(this.sqls.length+' task(s) found to be completed.');
            this.processQueries(this.sqls, cb);
        });
    }

    checkTable(tname, table, cb){
        var self = this;
        this.log('checking table '+tname);
        var sql = "select * from information_schema.columns where table_name=? and table_schema =?";
        db.query(sql, [tname, this.db.name], function(err, rows, flds){
            if( err || rows.length<=0 ){
                self.log('    does not existm making create statement');
                self.createTable(tname, table);
            }
            else{
                self.log('    matching table schema with definition');
                self.updateTable(tname, table, rows);
            }
            if( table.hasOwnProperty('trigger') ){
                if( !(table.trigger instanceof Array) )table.trigger = [table.trigger];
                for(var tr of table.trigger )self.sqls.push(tr);
            }

            if(cb)cb.apply(self);
        });
    }

    processIndexList(tname, indices, unique, cb){
        if( indices.length==0 )return cb?cb.apply(this):null;
        this.updateIndex(tname, indices.shift(), unique, function(){
            this.processIndexList(tname, indices, unique, cb);
        });
    }

    updateIndices(tname, table, cb){
        var indices = [];
        if( table.hasOwnProperty('index') )
            if( table.index.length>0 && table.index[0] instanceof Array )
                indices.push.apply(indices, table.index);
            else
                indices.push(table.index);

        this.processIndexList(tname, indices, false, function(){
            indices = [];
            if( table.hasOwnProperty('unique') )
                if( table.unique.length>0 && table.unique[0] instanceof Array )
                    indices.push.apply(indices, table.unique);
                else
                    indices.push(table.unique);
            this.processIndexList(tname, indices, true, function(){
                cb.apply(this);
            });
        });

    }

    updateIndex(tname, index, unique, cb){
        var idxName = tname;
        for(var i in index )idxName += '_'+index[i];

        var sql = "select index_name from information_schema.statistics WHERE table_name='"+tname+"' and index_name='"+idxName+"'";
        var self = this;
        db.query(sql, [], function(err, rows, flds){
            if( err || rows.length==0 ){
                self.log('    index '+idxName+' does not exists, creating it.');
                var sql = "create "+(unique?"UNIQUE":"")+" index "+idxName+" on "+tname+" ("+index.join(',')+")";
                self.sqls.push(sql);
            }
            else{
                //already exists...
                console.log('    index '+idxName+' already exists');
            }
            cb.apply(self);
        });
    }

    fieldDDL(fld){
        var ddl = fld.name;
        var type= (fld.hasOwnProperty('type')?fld.type:'varchar').toLowerCase();
        var size= fld.hasOwnProperty('size')?fld.size:'255';
        if( fld.hasOwnProperty('auto') && fld.auto)
            ddl += ' '+type+' AUTO_INCREMENT PRIMARY KEY';
        else if( type == 'varchar' )
            ddl += ' '+type+'('+size+')';
        else if( type == 'timestamp' )
            ddl += " timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
        else if( type == 'jsonb' )
            ddl += ' varchar('+size+')';
        else if( type == 'blob' )
            ddl += ' mediumblob ';
        else
            ddl += ' '+type+' ';
        
        if( fld.hasOwnProperty('primary') && fld.primary )
            ddl += ' PRIMARY KEY ';
        if( fld.hasOwnProperty('null') && fld.null.toUpperCase() == 'NO' )
            ddl += ' NOT NULL ';
        if( fld.hasOwnProperty('default') )
            if( (''+fld.default).toUpperCase()=='CURRENT_TIMESTAMP' )
                ddl += " default "+fld.default+"";
            else
                ddl += " default '"+fld.default+"'";
        return ddl;
    }

    createTable(tname, table){
        var sql = "create table "+tname+"(";
        for(var f in table.fields){
            sql += this.fieldDDL(table.fields[f])+",";
        }
        if( table.hasOwnProperty('unique') ){
            var uniques = table['unique'];
            if( uniques.length>0 ){
                if( !(uniques[0] instanceof Array) )uniques = [uniques];
                for(var i in uniques){
                    sql += "unique("+uniques[i].join(',')+"),";
                }
            }
        }
        sql = sql.substring(0, sql.length-1) + ")";
        if( table.hasOwnProperty('engine') )
            sql += " ENGINE="+table['engine'];
        sql += ";";
        this.sqls.push( sql );
    }

    updateTable(tname, table, tdef){
        const fields = table['fields'];
        var   tableFields = {};
        for(var i in tdef)tableFields[tdef[i]['COLUMN_NAME']] = tdef[i];
        //console.log( tableFields);
        var prev = '';
        for(var i in fields){
            const fld = fields[i];
            var type = (fld.hasOwnProperty('type')?fld.type:'varchar').toLowerCase();
            var size = fld.hasOwnProperty('size')?fld.size:'255';
            
            if( tableFields.hasOwnProperty(fld.name) ){
                const tfld= tableFields[fld.name];
                var needUpdate = false;
                if( (type == 'bigint' || type == 'int') && !tfld.COLUMN_TYPE.startsWith(type) ){
                    console.log('    '+fld.name+':type mismatch '+type);
                    needUpdate = true;
                }
                else if( type == 'varchar' && tfld.COLUMN_TYPE != 'varchar('+(size)+')' ){
                    console.log('    '+fld.name+':type mismatch '+type+' size: '+size);
                    needUpdate = true;
                }
                else if( (type == 'datetime' || type == 'timestamp') && tfld.COLUMN_TYPE !=type ){
                    console.log('    '+fld.name+':type mismatch '+type);
                    needUpdate = true;
                }
                else if( type != 'timestamp' && (fld.hasOwnProperty('null')?fld.null:"YES") != tfld.IS_NULLABLE ){
                    console.log('    '+fld.name+':nullable mismatch '+tfld.IS_NULLABLE+' : '+fld.hasOwnProperty('null'));
                    needUpdate = true;
                }
                
                if( needUpdate ){
                    this.log('    field '+fld.name+' needs upgrade, performing....');
                    var sql = "alter table "+tname+' modify column '+this.fieldDDL(fld);
                    this.sqls.push( sql );
                }
            }
            else{
                // create (add) new field
                this.log('    field '+fld.name+' does not exists, adding ...');
                var sql = "alter table "+tname+" add column "+this.fieldDDL(fld)+(prev?" after "+prev:"");
                this.sqls.push( sql );
            }
            prev = fld.name;
        }
    }

    logsText(){
        var txt = '';
        for(var i in this.logs)txt += this.logs[i]+'\n';
        return txt;
    }
    errorsText(){
        var txt = '';
        for(var i in this.errors)txt += this.errors[i]+'\n';
        return txt;
    }

}

module.exports = gupgrade;