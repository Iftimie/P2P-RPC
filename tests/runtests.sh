clear_ports () {
  echo "Clearing processes"
  kill -9 $(ps ax | grep celery | fgrep -v grep | awk '{ print $1 }')
  kill -9 $(ps ax | grep defunct | fgrep -v grep | awk '{ print $1 }')
  kill -9 $(ps ax | grep python | fgrep -v grep | awk '{ print $1 }')
  kill -9 $(ps ax | grep mongo | fgrep -v grep | awk '{ print $1 }')
}

export PYTHONPATH=$PYTHONPATH:$(pwd)/../

for testnum in $(seq 1 7)
do
  clear_ports
  echo $testnum
  python stresstest.py $testnum
  if [ $? == 1 ]
  then
    echo "$testnum failed"
    exit 1
  fi
done

echo "All tests finished successfully"