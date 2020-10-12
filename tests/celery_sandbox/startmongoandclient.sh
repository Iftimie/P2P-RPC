kill -9 $(ps ax | grep celery | fgrep -v grep | awk '{ print $1 }')
kill -9 $(ps ax | grep defunct | fgrep -v grep | awk '{ print $1 }')
kill -9 $(ps ax | grep python | fgrep -v grep | awk '{ print $1 }')
kill -9 $(ps ax | grep mongo | fgrep -v grep | awk '{ print $1 }')

python client.py