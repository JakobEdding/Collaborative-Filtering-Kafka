cd dev

echo "topics before: `docker-compose exec kafka-1 ./usr/bin/kafka-topics --list --zookeeper zookeeper-1`"

docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movieIds-with-ratings --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic userIds-to-movieIds-ratings --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic eof --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features --zookeeper zookeeper-1

docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movieIds-with-ratings --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic userIds-to-movieIds-ratings --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic eof --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features --replication-factor 1 --partitions 4 --zookeeper zookeeper-1

echo "topics after: `docker-compose exec kafka-1 ./usr/bin/kafka-topics --list --zookeeper zookeeper-1`"

cd ..