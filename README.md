TweetsAnalyzer
=============

Sentimental analysis of tweets with Spark

## Set up requirements
- Apache Spark
- Elasticsearch
- Logstash
- Kibana

## Ingest tweets
- Get Twitter API keys and Access Tokens. 
- Modify Logstash config file to use your Twitter API credentials. 
- Example of twitter_logstash.conf file is under `src/resources`

```
# start ES
ES_HOME_DIR/bin/elasticsearch

# start Kibana
KIBANA_HOME_DIR/bin/kibana

# ingest tweets
LOGSTASH_HOME_DIR/bin/logstash -f twitter_logstash.conf
```

By default:
Elasticsearch running on localhost:9200
Kibana running on localhost:5601


## Run
- Build project: `sbt -J-Xms2048m -J-Xmx2048m assembly`
- Modify `tweets_analyzer.conf` file
- Run `./run.sh tweets_analyzer.conf`