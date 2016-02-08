var Promise = require('bluebird'),
    etl = require('etl'),
    path = require('path'),
    crypto = require('crypto'),
    expat = require('node-expat'),
    Stream = require('stream'),
    request = Promise.promisifyAll(require('request'),{multiArgs:true}),
    cheerio = require('cheerio'),
    globalEmitter = new (require('events'))();

// Get the collection of rss links;
var links = request.getAsync('https://www.pacer.gov/psco/cgi-bin/links.pl')
  .spread( (res,body) => {
    return cheerio.load(body)('a')
      .map(function() { return cheerio(this).attr('href');})
      .toArray()
      .filter(d => d.indexOf('rss_outside') !== -1);
  });

// Readable stream that spits out sequential links in a loop
var read = Stream.Readable({
  objectMode: true,
  links : [],
  read : function()  {
    var next = (this.links || []).pop();
    if (!next)
      links.then(links => {
        this.links = links.slice();
        this._read();
      });
    else
      this.push(next);
  }
});

// Keep track of the latest guid we received for each court
var cache = {};

read.pipe(etl.map(10,function(url,cb) {
  var req = request.get({
      url:url,
      gzip:true,
      rejectUnauthorized : false
    });

  var item,elem,text,aborted;

  req.pipe(new expat.Parser('UTF-8'))
    .on('startElement',d => {
      if (d == 'item')
        item = {};
      elem = d; text = '';
    })
    .on('endElement', (d) => {
      if (d == 'item') {
        item._id = crypto.createHash('md5').update(JSON.stringify(item)).digest('hex');
        if (!cache[url])
          cache[url] = item._id;
        // If we seen the hash before it is time to stop
        else if(cache[url] == item._id) {
          process.stdout.write('|');  // notify we have seen this before
          aborted = true;
          req.abort();
        } else {
          if (!aborted)
            this.push(item);
          item = undefined;
        }
      } else if(item) {
        item[elem] = text;
      }
    })
    .on('text',d => text+=d)
    .on('error',e => {console.log(e); cb();})  // just log the error, dont stop
    .on('end',cb);
}))
.pipe(etl.map(d => {
  d.fetched = new Date();
  d.case = d.title && d.title.split(' ')[0];
  var m = /\[(.*?)\]/.exec(d.description || '');
  if (m) d.action = m[1];
  globalEmitter.emit('event',d);
  return d;
}))
.pipe(etl.map(d => {
  // upload to a database
  process.stdout.write('.');
}));

// Express server

require('express')()
  .get('/events',function(req,res) {
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
    req.on('close',function() {
      globalEmitter.removeListener('event',emit);
    });
  })
  .use(function(req,res) {
    res.sendFile(path.join(__dirname,'index.html'));
  })
  .listen(3000);


