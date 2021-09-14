const {
    Pool
} = require('pg')
const csv = require('csvtojson')
const getRandomUserAgent = require('./userAgent');
const superagent = require('superagent');
let Projects = []
var pool, amapurl, filename, parallelism, fields, tokenkey, srid, tablename;
async function run(config) {
    pool = new Pool(config.target);
    amapurl = config.source;
    filename = config.filename;
    parallelism = config.parallelism;
    fields = config.tablefileds;
    tablename = config.tablename;
    srid = config.srid;
    tokenkey = config.tokenkey;
    const converter = await csv().fromFile(filename)
    await creat_spatial_table();
    await features2Postgis(converter)
    console.log('finish');
}
async function creat_spatial_table() {
       let field_sql = '';
       for (let i = 0; i < fields.length; i++) {
           const field = fields[i]
           const field_name = field.name.toLowerCase();
           const field_type = type_Convert(field.type, field.length);
            if (CheckChinese(field_name))
                field_sql += `"${field_name}" ${field_type},\n`;
            else
                field_sql += `${field_name} ${field_type},\n`;
        }
        field_sql += `geom geometry(Point,${srid})`;
        let sql = `
        drop table if exists ${tablename};
        create table ${tablename}(
            gid serial primary key,
            ${field_sql}
        );
        create index ${tablename}_geom_idx on ${tablename} using gist(geom);
        COMMENT ON TABLE ${tablename} IS '${tablename}';
        `;
        pool.query(sql, function (err, res) {
            if (!sql) {
                console.log(err);
            } else {
                console.log(`服务 ${tablename} ->表 ${tablename} 创建完毕！`)
            }
        });
}
function getDate2pg(url,prjame, sql_header) {
    var p = new Promise(function (resolve, reject) {
        const userAgent = getRandomUserAgent();
        superagent.get(url).responseType('json')
            .set(userAgent).timeout({
                response: 6000000,
                deadline: 6000000,
            }).retry(3)
            .end(function (err, res) {
                if (err) {
                    console.log(url);
                    resolve(err);
                    // return;
                }
                const datas = res.body.toString();
                let dataSet;
                try {
                    dataSet = JSON.parse(datas);
                } catch (err1) {
                    console.log(url);
                    resolve(err1);
                    return;
                }
                let sql = sql_header;
                if (!dataSet || !dataSet.geocodes) {
                    console.log('features null', url);
                    resolve('null');
                    resolve(prjame.项目地点);
                    return;
                }
                for (let j = 0; j < dataSet.geocodes.length; j++) {
                    let values = '(';
                    const feature = dataSet.geocodes[j];
                    var attributes = Object.assign(feature, prjame)
                    const esri_geom = feature.location;
                    for (let key of fields) {
                        const column_name = key.name.toLowerCase();
                        if (key.type ==='String'){
                            values += `'${value_format(attributes[column_name])}',`;
                        }
                        else
                            values += `${attributes[column_name]},`;
                    }
                    let geom = createPT(esri_geom);
                    if (j != dataSet.geocodes.length - 1)
                        values += `ST_GeomFromText('${geom}', ${srid})),`;
                    else
                        values += `ST_GeomFromText('${geom}', ${srid}));`;
                    sql += values;
                }
                pool.query(sql, function (db_err, db_res) {
                    if (db_err) {
                        // console.log(db_err);
                        resolve(db_err);
                    }
                    reject('SUCCESS');
                });
            });

    }).then(undefined, (error) => {
        console.log(error);
    });
    return p;
}
async function features2Postgis(Projects) {
    //获取服务的要素总数
    const feature_count = Projects.length;
    let sql_header = `insert into ${tablename}(`;
     for (let key of fields) {
        if (CheckChinese(key.name))
            sql_header += `"${key.name}",`;
        else
            sql_header += `${key.name},`;
    }
    sql_header += 'geom) values ';

    let promises = [];
     for (let i = 0; i < feature_count; i++) {
         const url = `${amapurl}?address=${encodeURI(Projects[i].项目地点)}&key=${tokenkey}`;
         promises.push(getDate2pg(url, Projects[i], sql_header));
        if (promises.length == parallelism) {
            await Promise.all(promises);
            console.log(`insert into ${tablename} counts:${(i + 1)}/${feature_count}`);
            promises = [];
        }
    }
    if (promises.length > 0) {
        await Promise.all(promises);
    }
    console.log(tablename + '入库完毕！');
}
function value_format(_value) {
    if (!_value) return ''
    _value = _value.toString().replace(/'/g, "''");
    // _value = _value.replace(",", "");
    return _value;
}
function type_Convert(type,length) {
    let pg_type;
    switch (type) {
        case 'String':
            pg_type = `varchar(${length})`;
            break;
        case 'int':
            pg_type = 'int';
            break;
        default:
            pg_type = type;
            break;
    }
    return pg_type;
}
function createPT(geom) {
    return `Point(${geom.split(",")[0]} ${geom.split(",")[1]})`;
}

function CheckChinese(val) {
    var reg = new RegExp("[\\u4E00-\\u9FFF]+", "g");
    if (reg.test(val)) {
        return true;
    }
    return false;
}
module.exports = run;