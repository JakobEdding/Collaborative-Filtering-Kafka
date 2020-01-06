cd dev

docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movieIds-with-ratings --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic userIds-to-movieids-ratings --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic eof --zookeeper zookeeper-1

docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movieIds-with-ratings --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic userIds-to-movieids-ratings --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic eof --replication-factor 1 --partitions 4 --zookeeper zookeeper-1

cd ..