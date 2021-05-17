
const express = require("express")
const bodyParser = require("body-parser")
const elasticsearch = require("elasticsearch");
const fetch = require('node-fetch');
const app = express()
app.use(bodyParser.json())



const esClient = new elasticsearch.Client({
    host: '127.0.0.1:9200',
    log: 'error'
});

function bulkIndex(index, type, data, cb) {
    let bulkBody = [];
    
    data.forEach(item => {
      bulkBody.push({
        index: {
          _index: index,
          _type: type,
          _id: item.pid
        }
      });
  
      bulkBody.push(item);
    });
  
    esClient.bulk({body: bulkBody})
    .then(response => {
      console.log('here');
      let errorCount = 0;
      
      response.items.forEach(item => {
        if (item.index && item.index.error) {
          console.log(++errorCount, item.index.error);
        }
      });

      cb(response)

      console.log(
        `Successfully indexed ${data.length - errorCount}
         out of ${data.length} items`
      );
    })
    .catch(console.err);
  };

app.post('/create-mapping', (req,res) => {
  const { index, mapping } = req.body;

  esClient.indices.create({
    index,
    }, function(error, response, status) {

        if (error) {
          res.json(error);
        }
        else {
          res.json(response);
        }

      // esClient.indices.putMapping({  
      //   index,
      //   type: 'products',
      //   include_type_name: true,
      //   body: {
      //     properties: mapping,
      //   }
      // },function(err,resp,status){
      //     if (err) {
      //       res.json(err);
      //     }
      //     else {
      //       res.json(resp);
      //     }
      // })

    }
  );
});

app.post('/bulk-sync', (req, res) => {
    const { dataSource } = req.body;
    
    esClient.deleteByQuery({
      index: 'catalog-data',
      body: {
        query: {
          match_all: {},
        }
      }
    }, (err, resp) => {
      console.log('err', err);
      if (!err) {
        fetch(dataSource)
        .then(res => res.json())
        .then(json => {
            bulkIndex('catalog-data', 'products', json, (data) => {
              res.json(data);
            });
        }).catch(err => {
          res.json(err);
        })
      }
    })
});


function search(index, body) {
  return esClient.search({index: index, body: body});
}

app.get('/products', (req, res) => {
  const { searchText } = req.query;
    let body = {
       "query": {
           "multi_match": {
               query: searchText,
               fuzziness: 2,
               fields: ['product_name', 'description', 'categories']
           }
       }
    };

    search('catalog-data', body)
    .then(results => {
      console.log(results.hits.total.value);
      const data = results.hits.hits.map(
        (hit, index) => hit._source
      );
      res.json(data);
    })
    .catch(console.error);
});

app.listen(process.env.PORT || 3000, () => {
    console.log("connected")
});

