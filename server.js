
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

esClient.indices.delete({index: 'catalog-data'},function(err,resp,status) {  
  // esClient.indices.create({
  //   index: 'catalog-data',
  //   }, function(error, response, status) {
  //     console.log(error);
  //   }
  // );
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
  const { fields } = req.body;
  const customAnalyzer = {
    "settings": {
      "analysis": {
        "filter": {
          "autocomplete_filter": {
            "type": "edge_ngram",
            "min_gram": "1",
            "max_gram": "40"
          }
        },
        "analyzer": {
          "custom_analyzer": {
            "type":      "custom", 
            "tokenizer": "standard",
            "char_filter": [
              "html_strip"
            ],
            "filter": [
              "lowercase"
            ]
          },
          "autocomplete": {
            "filter": ["lowercase", "autocomplete_filter"],
            "type": "custom",
            "tokenizer": "whitespace"
          }
        }
      }
    },
    "mappings": {
      "products": {
        "properties": {
        }
      }
   
    }
  };

  fields.forEach(field => {
    customAnalyzer.mappings.products.properties[field] = {
      "type": "text",
      "fields": {
        "custom_analyzer": { 
          "type": "text",
          "search_analyzer": "custom_analyzer",
          "analyzer": "autocomplete"
        }
      }
    }
  });

  fetch('http://localhost:9200/catalog-data?include_type_name=true', {
    method: 'PUT', 
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(customAnalyzer),
  }).then(res => res.json())
  .then(json => {
    res.json(json);
  })
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
               fields: [
                 'product_name', 
                 'description', 
                 'categories', 
                ],
                "type" : "phrase_prefix",
                analyzer: "custom_analyzer"
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

/**
 * 
 *     const promisesArr = json.map((data, index) => {
              return fetch(`http://localhost:9200/catalog-product/_bulk`, {
                method: 'PUT',
                headers: {
                  'Content-Type': 'application/json'
                },
                body: JSON.stringify(data),
              }).then(response => response.json())
            });
            Promise.all(promisesArr)
              .then(json => {
                res.json(json);
              })
              .catch(err => {
                res.json(err);
              })
        }).catch(err => {
          res.json(err);
        })
 */