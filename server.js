const elasticsearch = require('elasticsearch');
const express = require('express');
const fetch = require('node-fetch');
const { nanoid } = require('nanoid');

const app = express();

const esClient = new elasticsearch.Client({
    host: '127.0.0.1:9200',
    log: 'error'
});


const bulkIndex = function bulkIndex(index, type, data, cb) {
    let bulkBody = [];
    
    esClient.indices.create({
            index,
        }, function(error, response, status) {
            if (error) {
                cb();
            } else {
                console.log(response);

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
              
                    cb(response);
              
                    console.log(
                      `Successfully indexed ${data.length - errorCount}
                       out of ${data.length} items`
                    );
                  })
                  .catch(console.err);
            }
    });
  };
  
  const search = function search(index, body) {
    return esClient.search({index: index, body: body});
  };

  
  const test = function test() {
    fetch('https://raw.githubusercontent.com/appbaseio/cdn/dev/appbase/ecommerce_data.json')
        .then(res => res.json())
        .then(json => {
            bulkIndex('product_catalog_1', 'products', json, () => {
                testSearch();
            });
            
        })
    
  };


  const testSearch = () => {
    let body = {
       "query": {
           "multi_match": {
               query: "Interface",
               fuzziness: 2,
               fields: ['product_name', 'description', 'categories']
           }
       }
    };

    search('product_catalog_1', body)
    .then(results => {
      console.log(`found ${results.hits.total} items in ${results.took}ms`);
      console.log(`returned article titles:`);
      results.hits.hits.forEach(
        (hit, index) => console.log(hit._source.pid)
      )
    })
    .catch(console.error);
  };




//   test();






app.listen(3000, () => {
    console.log('server started on PORT:', 3000);
});
