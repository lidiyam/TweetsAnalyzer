#!/bin/bash

CONF_FILE=$1

export $(grep -i SPARK_HOME_DIR $CONF_FILE | grep -v '#' | tr -d '[:blank:]' )
export $(grep -i master_url $CONF_FILE | grep -v '#' | tr -d '[:blank:]' )
export $(grep -i tweets_analyzer_jar $CONF_FILE | grep -v '#' | tr -d '[:blank:]' )
export $(grep -i elasticsearch_spark_jar $CONF_FILE | grep -v '#' | tr -d '[:blank:]' )
export $(grep -i training_data $CONF_FILE | grep -v '#' | tr -d '[:blank:]' )
export $(grep -i runTrainingOnly $CONF_FILE | grep -v '#' | tr -d '[:blank:]' )

# train models to classify tweets
$SPARK_HOME_DIR/bin/spark-submit --class "TrainModel" --master $master_url ${tweets_analyzer_jar} $training_data

if [ "$runTrainingOnly" == true ]; then
    exit 0
fi

# sentimental analysis
$SPARK_HOME_DIR/bin/spark-submit --jars ${elasticsearch_spark_jar} --class "ESTweets" --master $master_url ${tweets_analyzer_jar}

# generic tweets analysis
$SPARK_HOME_DIR/bin/spark-submit --jars ${elasticsearch_spark_jar} --class "TweetsAnalyzer" --master $master_url ${tweets_analyzer_jar}
