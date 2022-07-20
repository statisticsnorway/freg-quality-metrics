APPNAME=$1
docker build . -t $APPNAME
docker run -p 8080:8080 $APPNAME
