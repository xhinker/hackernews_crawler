const request   = require('request');

const { Client } = require('@elastic/elasticsearch');
const client = new Client({ node: 'http://localhost:9200' });

var mark =1;
var batch_number=10;
var hn_url = 'https://news.ycombinator.com/item?id=';
var resolvehandler;

var api_url_prefix = 'https://hacker-news.firebaseio.com/v0/item/';

async function add_doc_to_es(source_item){
    await client.index({
        index:"hackernews"
        ,id:source_item.id
        ,body:source_item
    });
    console.log("insert doc done:",source_item.id);
}

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

        try {
            var source_item = JSON.parse(body);
            if(source_item && source_item.by){
                add_doc_to_es(source_item);
                mark++;
                if(mark>=batch_number){
                    resolvehandler();
                }
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

        await client.indices.refresh(
            {
                index:"hackernews"
            }
        );
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

let interval_microsecond = 24*60*60*1000;
let interval_handler = setInterval(function(){
    console.log('program startup at:'+(new Date()));
    main();
},interval_microsecond);
