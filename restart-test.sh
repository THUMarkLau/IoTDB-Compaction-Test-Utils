#! /bin/bash

kill-iotdb(){
  iotdb_info=`jps | grep -i iotdb`
  arr=(${iotdb_info// / })
  if [ ${#arr[@]} -lt 2 ]; then
    echo "No IoTDB instance is running"
  else
    echo IoTDB pid is ${arr[0]}, kill it
    `kill -9 ${arr[0]}`
  fi
}

lauch-iotdb() {
  `nohup sh $1 > /dev/null 2>&1 &`
}

lauch-iotdb $1

echo sleeping
sleep 10s
echo kill iotdb
kill-iotdb