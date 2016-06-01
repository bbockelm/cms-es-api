
package main
import ("io"
        "fmt"
        "log"
        "encoding/json"
        "github.com/julienschmidt/httprouter"
        "net/http"
        "gopkg.in/olivere/elastic.v3"
       )

var server_url string = "http://hcc-metrics.unl.edu:9200"

func memoryUsage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

  v := r.URL.Query()
  request := v.Get("request")
  if request == "" {
    w.WriteHeader(http.StatusBadRequest)
    io.WriteString(w, "Request name not specified (hint: add ?request=pdmvserv_task_HIG-RunIISpring16DR80-01026__v1_T_160530_083522_4482 to URL)\n")
    return
  }

  client, err := elastic.NewClient(elastic.SetURL(server_url), elastic.SetSniff(false))
  if err != nil {
    w.WriteHeader(http.StatusInternalServerError)
    io.WriteString(w, "Failed to create a new internal ElasticSearch client\n")
    io.WriteString(w, fmt.Sprintf("%v\n", err))
    return
  }

  termQuery := elastic.NewMatchQuery("WMAgent_RequestName", request)
  agg := elastic.NewPercentilesAggregation().Field("MemoryMB").Percentiles(99)
  search := client.Search().Index("cms-*").Type("job").
            Query(termQuery).
            Aggregation("memory", agg).
            Size(0)

  searchResult, err := search.Do()
  if err != nil {
    w.WriteHeader(http.StatusBadRequest)
    io.WriteString(w, "Failure when querying for request\n")
    return
  }

  log.Printf("Query took %d ms.", searchResult.TookInMillis)

  if len(searchResult.Aggregations) > 0 {
    for _, hit := range searchResult.Aggregations {
      var f interface{}
      err := json.Unmarshal(*hit, &f)
      if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        io.WriteString(w, "Failure when unmarshalling response from ElasticSearch\n")
        return
      }

      m := f.(map[string]interface{})
      val, ok := m["values"]
      if !ok {
        w.WriteHeader(http.StatusInternalServerError)
        io.WriteString(w, "Response from ElasticSearch missing values field\n")
        return
      }
      values_map, ok := val.(map[string]interface{})
      if !ok {
        w.WriteHeader(http.StatusInternalServerError)
        io.WriteString(w, "Response from ElasticSearch has incorrect memory field type\n")
        return
      }

      val, ok = values_map["99.0"]
      if !ok {
        w.WriteHeader(http.StatusInternalServerError)
        io.WriteString(w, "Response from ElasticSearch has missing 99th percentile entry\n")
        return
      }

      val_float, ok := val.(float64)
      if !ok {
        w.WriteHeader(http.StatusInternalServerError)
        io.WriteString(w, "Response from ElasticSearch has incorrect memory field type\n")
        return
      } 

      w.WriteHeader(http.StatusOK)
      io.WriteString(w, fmt.Sprintf("%.2f\n", val_float))
      break
    }
  } else {
    w.WriteHeader(http.StatusBadRequest)
    io.WriteString(w, "Request returned no results.\n")
    return
  }
}

func main() {

  router := httprouter.New()
  router.GET("/cms/memory_usage", memoryUsage)

  log.Fatal(http.ListenAndServe(":8080", router))

}

