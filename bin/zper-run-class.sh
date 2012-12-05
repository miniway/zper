#!/bin/bash

if [ $# -lt 1 ];
then
    echo "USAGE: $0 classname [args]"
    exit 1
fi

base_dir=$(dirname $0)/..

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

CPFILE="$base_dir/classpath.txt"
if [ -f "$CPFILE" ]; then
  CLASSPATH=$CLASSPATH:`cat $CPFILE`
fi


OPTS="-server -Xmx512M -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -Dlog4j.configuration=file:$base_dir/conf/log4j.properties"

$JAVA $OPTS -cp $CLASSPATH $@
#echo $! > $pid
#sleep 1; head "$log"
