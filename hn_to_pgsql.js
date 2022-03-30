const request   = require('request');
const http      = require('http');
const pg        = require('pg');
const Pool      = require('pg').Pool;
const cheerio   = require('cheerio');
const url       = require('url');

/**
 * postgresql db connection info
 */
var pg_conn_config = {
    //host: '192.168.3.31',
    host:'zhusd.com',
    port: 5432,
    user: 'username',
    password: 'password',
    database: 'dbname',
    max: 50,
    idleTimeoutMillis: 10000,
};

// settings for Azure postgresql
// var pg_conn_config = {
//     //host: '192.168.3.31',
//     host:'az-azure-postgres-server-01.postgres.database.azure.com',
//     port: 5432,
//     user: 'username',
//     password: 'password',
//     database: 'dbname',
//     max: 50,
//     idleTimeoutMillis: 10000,
// };

var pg_pool = new Pool(pg_conn_config);
var insert_hn_cmd = "select insert_hackernews($1 \
                                          ,$2 \
                                          ,$3 \
                                          ,$4 \
                                          ,$5 \
                                          ,$6 \
                                          ,$7 \
                                          ,$8 \
                                          ,$9 \
                                          ,$10 \
                                          ,$11 \
                                          ,$12 \
                                          ,$13)";

var mark =1;
var batch_number=10;
var hn_url = 'https://news.ycombinator.com/item?id=';
var resolvehandler;

//var id = 487171;
//var id = 2041;
//var id =13953800;

/**
 * 
 * @param {obj} hn_item 
 */

function insert_hn_bypool(hn_item) {
    pg_pool.connect(function (err, client, done) {
        if (err) {
            if(mark>=batch_number){
                resolvehandler();
            }
            mark++;
            console.error('pg db connection error',err);
            return false;
        }
        client.query({
            name: "insert hackernew content",
            text: insert_hn_cmd,
            values: [
                  hn_item.hn_id
                , hn_item.story_title
                , hn_item.user_id
                , hn_item.points
                , hn_item.hn_article_link
                , hn_item.hn_article_content
                , hn_item.article_pub_host
                , hn_item.kid_ids
                , hn_item.publish_time
                , hn_item.hn_comment
                , hn_item.comments_count
                , hn_item.parent
                , hn_item.content_type
            ]
        }, function (err, result) {
            if(mark>=batch_number){
                resolvehandler();
            }
            if (err) {
                console.error('error running query', err);
                mark++;
                return false;
            }
            done(err);
            
            console.log('insert new item:'+result.rows[0].insert_hackernews);

            console.log('mark:'+(mark++));
        });
    });
}

//test it
//insert_hn_bypool(123,'user_id',123,'hn_article_link','article_content','article_pub_host',111,'2017-03-25');

/**
 * the web will be blocked, so, this function is for reference only , don't call it for data collection
 */
function parse_hn_web(body, id) {
    var hn_item                         = {};
    let $                               = cheerio.load(body);

    //get id, id is already there
    hn_item.hn_id                       = id;
    //get user_id'
    hn_item.user_id                     = $('.hnuser').attr('href').split('=')[1];
    //get points
    hn_item.points                      = parseInt($('#score_1').text().split(' ')[0]);
    if (!hn_item.points) {
        hn_item.points                  = null;
    }
    //get article link
    hn_item.hn_article_link             = $('.storylink').attr('href');
    //assign the content to null for now
    hn_item.article_content             = null;
    if (hn_item.hn_article_link) {
        var tempUrl                     = url.parse(hn_item.hn_article_link);
        hn_item.article_pub_host        = tempUrl.host;
    }
    //get parent_id
    hn_item.parent_id=null;
    if(!hn_item.hn_article_link){
        hn_item.parent_id               = $('.par > a').attr('href').split('=')[1];
    }
    //get publish time
    hn_item.publish_time                = null;
    var timeString                      = $('.age > a').text();
    var timeNumber                      = timeString.split(' ')[0];
    var timeScalar                      = timeString.split(' ')[1];
    const MS_PER_MINUTE                 = 60*1000;
    const MS_PER_HOUR                   = 60*60*1000;
    const MS_PER_DAY                    = 24*60*60*1000;
    if(timeScalar === 'minutes' || timeScalar === 'minute'){
        hn_item.publish_time            = new Date((new Date())- timeNumber*MS_PER_MINUTE);
    }else if(timeScalar === 'hour' || timeScalar === 'hours'){
        hn_item.publish_time            = new Date((new Date()) - timeNumber*MS_PER_HOUR);
    }else if(timeScalar === 'day' || timeScalar === 'days'){
        hn_item.publish_time            = new Date((new Date()) - timeNumber*MS_PER_DAY);
    }

    //get hn_comment
    hn_item.hn_comment=null;
    if(!hn_item.hn_article_link){
        hn_item.hn_comment              = $('.comment').html().trim();
    }

    //test
    //console.log(hn_item);
    return hn_item;
}

function parse_hn_api(source_item,id){
    let hn_item                             = {};
    //get id, id is already there
    hn_item.hn_id                           = id;
    //get title
    hn_item.story_title                     = source_item.title;
    //get user_id'
    if(source_item.by){
        hn_item.user_id                     = source_item.by;
    }else{
        hn_item.user_id                     = "unknown";
    }
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
    //hn_item.parent_id                     = source_item.parent;
    //get publish time
    hn_item.publish_time                    = new Date(source_item.time*1000);
    //get hn_comment
    hn_item.hn_comment                      = source_item.text;
    //get comment count
    if(source_item.kids){
        hn_item.comments_count              = source_item.kids.length;
        hn_item.kid_ids                     = source_item.kids;
    }
    //get parent 
    if(source_item.parent){
        hn_item.parent                      = source_item.parent;
    }else{
        hn_item.parent                      = -1;
    }
    //get type 
    if(source_item.type){
        hn_item.content_type                = source_item.type;
    }else{
        hn_item.content_type                = "unknown";
    }

    return hn_item;
}

var api_url_prefix = 'https://hacker-news.firebaseio.com/v0/item/';
function get_hn_api(id){
    request({
        url: api_url_prefix + id+'.json',
        "rejectUnauthorized": false,
        timeout:20000
    }, function (error, response, body) {
        console.log('go to deal '+id);

        if(error){
            console.error('get api something wrong',error);
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
            if(source_item && source_item.by){
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
/**
 * 
 * this is for web page parse only
 * @param {Number} id 
 */
function getWeb(start_id) {
    request({
        url: hn_url + id,
        "rejectUnauthorized": false
    }, function (error, response, body) {
        console.log('go to deal '+id);
        
        // console.log('error:', error);                                   // Print the error if one occurred
        // console.log('statusCode:', response && response.statusCode);    // Print the response status code if a response was received
        // console.log('body:', body);                                     // Print the HTML for the Google homepage.
        try {
            insert_hn_bypool(parse_hn_web(body, id));
        } catch (error) {
            console.log(error);
        }
        
        if(body === 'No such item.'){
            console.log('no item');
        } else { 
            console.log('go next');
            id++;
            getWeb(id);
        }
    });
}


function run_update(startId){
    for(var id=startId;id<(startId+batch_number);id++){
        get_hn_api(id);
    }
} 

async function runIt(startId,maxId){
    console.log('start get hn data');

    let loop_count = Math.round((maxId-startId)/batch_number)+1;
    console.log('loop count:'+loop_count);

    for(var i=0;i<loop_count;i++){
        await new Promise(function(resolve,reject){
            resolvehandler = resolve;
            run_update(startId+(i*batch_number));
        });
        console.log('======================================>'+i);
        mark=1;
    }

    console.log('get hn data end');
}

//get maxId 
var maxId=0;
function getMaxId(resolve){
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
    let startId=maxId - 100000;
    //startId = 1;
    console.log('max id:'+maxId);
    runIt(startId,maxId);
}

main();

let interval_microsecond = 12*60*60*1000;
let interval_handler = setInterval(function(){
    console.log('program startup at:'+(new Date()));
    main();
},interval_microsecond);
