const Promise = require('bluebird');
const etl = require('etl');
const path = require('path');
const xmler = require('xmler');
const Stream = require('stream');
const request = Promise.promisifyAll(require('request'),{multiArgs:true});
const cheerio = require('cheerio');
const crypto = require('crypto');
const globalEmitter = new (require('events'))();

// Get the collection of rss links;
const links = request.getAsync('https://www.pacer.gov/psco/cgi-bin/links.pl')
  .spread( (res,body) => {
    return cheerio.load(body)('a')
      .map(function() { return cheerio(this).attr('href');})
      .toArray()
      .filter(d => d.indexOf('rss_outside') !== -1);
  });


module.exports = argv => {
  // Keep track of the latest guid we received for each court
  const cache = {};
  const out = etl.map();
  argv = argv || {};

  // Readable stream that spits out sequential links in a loop
  return Stream.Readable({
    objectMode: true,
    read : function()  {
      links.then(links => {
        const val = links.shift();
        this.push(val);
        links.push(val);
      });
    }
  })
  .pipe(etl.map(argv.concurrency || 10,url => {
    const req = request.get({
        url:url,
        gzip:true,
        rejectUnauthorized : false
      });

    let aborted;

    return new Promise( (resolve,reject) => req
      .pipe(xmler('item',{valuesOnly:true}))
      .pipe(etl.map(function(item) {
        item._id = crypto.createHash('md5').update(JSON.stringify(item)).digest('hex');
        if (!cache[url])
          cache[url] = item._id;
        // If we seen the hash before it is time to stop
        else if(cache[url] == item._id) {
          aborted = true;
          req.abort();
        } else {
          item.fetched = new Date();
          item.case = item.title && item.title.split(' ')[0];
          var m = /\[(.*?)\]/.exec(item.description || '');
          if (m) item.action = m[1];
          console.log(item);
          return item;
        }
      }))
      .on('error',reject)
      .on('finish',resolve)
      .pipe(out)
    );
  }))
  .pipe(out);
};

  // Express server
if (!module.parent) {
  module.exports().pipe(etl.map(item => globalEmitter.emit('event',item)));

  require('express')()
    .get('/events', (req,res) => {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      });
     
      function emit(d) {
        res.write('id: '+(new Date()).valueOf()+'\n');
        res.write('data: '+JSON.stringify(d)+'\n\n');
        if (typeof res.flush === 'function')
          res.flush();
      }

      globalEmitter.on('event',emit);
      req.on('close', () =>  globalEmitter.removeListener('event',emit));
    })
    .use( (req,res) => res.sendFile(path.join(__dirname,'index.html')))
    .listen(3000);
}


