const request       = require('request');
const cheerio       = require('cheerio');
const url           = require('url');
const mssql         = require('mssql');

var msssql_config = {
    user: 'crawler_account',
    password: 'password',
    server:'mssql_server',
    database: 'mssql_db',
    stream: false,
    requestTimeout: 10000,
    pool:{
        max:100,
        min:0,
        idleTimeoutMillis: 300000
    }
};

var mark =1;
var batch_number=100;
var hn_url = 'https://news.ycombinator.com/item?id=';
var resolvehandler;

/**
 * insert into sql server
 * 
 * @param {obj} hn_item
 *  
 */
function insert_hn_bypool(hn_item) {
    console.log('start insert:'+hn_item.hn_id);
    let request     = mssql_pool.request();
    //request.stream  = false;
    
    request.input('hn_id'                   ,mssql.Int                  ,hn_item.hn_id);
    request.input('story_title'             ,mssql.NVarChar(256)        ,hn_item.story_title);
    request.input('user_id'                 ,mssql.NVarChar(256)        ,hn_item.user_id);
    request.input('points'                  ,mssql.Int                  ,hn_item.points);
    request.input('hn_article_link'         ,mssql.NVarChar(400)        ,hn_item.hn_article_link);
    request.input('hn_article_content'      ,mssql.NVarChar(mssql.MAX)  ,hn_item.hn_article_content);
    request.input('article_pub_host'        ,mssql.NVarChar(256)        ,hn_item.article_pub_host);
    request.input('parent_id'               ,mssql.Int                  ,hn_item.parent_id);
    request.input('publish_time'            ,mssql.DateTime2            ,hn_item.publish_time);
    request.input('hn_comment'              ,mssql.NVarChar(mssql.MAX)  ,hn_item.hn_comment);
    request.input('comments_count'          ,mssql.Int                  ,hn_item.comments_count);

    request.execute('insert_hackernews', function (err, result) {
        console.log('mark:'+mark);
        if(mark>=batch_number){
            resolvehandler();
        }
        mark++;

        if(err){
            //console.error(err,'insert error');
            console.error('insert error',err);
        }else{
            console.log('result:'+ JSON.stringify(result));
        }
    });

    request.on('recordset', function (columns) {
        console.log('recordset ready');
    });
    //get result
    request.on('row', function (row) {
        console.log('row:'+row);
    });

    request.on('error', function (err) {
        console.error('something not right when query',err);
        mark++;
    });

    request.on('done', function (affected) {
        console.log(hn_item.hn_id + ' insert done now');
    });
}


function parse_hn_api(source_item,id){
    var hn_item                             = {};
    //get id, id is already there
    hn_item.hn_id                           = id;
    //get title
    hn_item.story_title                     = source_item.title;
    //get user_id'
    hn_item.user_id                         = source_item.by;
    //get points
    hn_item.points                          = source_item.score;
    //get article link
    hn_item.hn_article_link                 = source_item.url;
    //set content as null 
    hn_item.hn_article_content              = null;
    //get host 
    if(hn_item.hn_article_link){
        hn_item.article_pub_host            = url.parse(hn_item.hn_article_link).host;
    }
    //get parent_id
    hn_item.parent_id                       = source_item.parent;
    //get publish time
    hn_item.publish_time                    = new Date(source_item.time*1000);
    //get hn_comment
    hn_item.hn_comment                      = source_item.text;
    //get comment count
    if(source_item.kids){
        hn_item.comments_count               = source_item.kids.length;
    }

    return hn_item;
}


var api_url_prefix = 'https://hacker-news.firebaseio.com/v0/item/';
function get_hn_api(id){
    request({
        url: api_url_prefix + id+'.json',
        "rejectUnauthorized": false,
        timeout:7000
    }, function (error, response, body) {
        console.log('go to deal '+id);

        if(error){
            console.error('something wrong',error);
            mark++;
            if(mark>=batch_number){
                resolvehandler();
            }
            return false;
        }
        
        // console.log('error:', error);                                   // Print the error if one occurred
        // console.log('statusCode:', response && response.statusCode);    // Print the response status code if a response was received
        // console.log('body:', body);                                     // Print the HTML for the Google homepage.
        
        try {
            var source_item = JSON.parse(body);
            if(source_item.by){
               insert_hn_bypool(parse_hn_api(source_item, id));
               //console.log(source_item.by);
            }else{
                mark++;
                if(mark>=batch_number){
                    resolvehandler();
                }
            }
        } catch (error) {
            mark++;
            if(mark>=batch_number){
                resolvehandler();
            }
            console.error('get hn api error',error);
        }
    });
}

function run_update(startId){
    for(let id=startId;id<(startId+batch_number);id++){
        get_hn_api(id);
    }
}

async function runIt(startId,maxId){
    console.log('start get hn data');

    let loop_count = Math.round((maxId-startId)/batch_number)+1;
    console.log('loop count:'+loop_count);

    for(let i=0;i<loop_count;i++){
        await new Promise(function(resolve,reject){
            resolvehandler = resolve;
            run_update(startId+(i*batch_number));
        });
        console.log('======================================>'+i+' at:'+(new Date()));
        mark=1;
    }

    console.log('get hn data end');
}

//get maxId 
var maxId=0;
function getMaxId(resolve){
    console.log('start get maxId');
    request({
        url: 'https://hacker-news.firebaseio.com/v0/maxitem.json',
        "rejectUnauthorized": false
    },function(error,response,body){
        maxId = parseInt(body);
        resolve();
    });
};

async function main(){
    await new Promise(function(resolve,reject){
        getMaxId(resolve);
    });
    var startId=maxId-1000000;
    console.log('max id:'+maxId);
    runIt(startId,maxId);
}

let mssql_pool;
//create a connection pool
mssql_pool = new mssql.ConnectionPool(msssql_config,(err)=>{
    console.log('db connected');
    main();
});

let interval_microsecond = 1*24*60*60*1000;
//let interval_microsecond = 2000;
let interval_handler=setInterval(function(){
    mssql_pool = new mssql.ConnectionPool(msssql_config,(err)=>{
        console.log('db connected at:'+(new Date()));
        main();
    });
},interval_microsecond);
