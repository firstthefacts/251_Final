sbt package && $SPARK_HOME/bin/spark-submit --master spark://final1:7077 $(find target -iname "*.jar")
