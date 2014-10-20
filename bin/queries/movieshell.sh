MONGO_HOME=/opt/mongodb/mongodb-linux-x86_64-2.6.4

cd ../../src/queries
MOVIEJS=moviequeries.js

$MONGO_HOME/bin/mongo --shell $MOVIEJS
