cd dev

#echo "topics before: `docker-compose exec kafka-1 ./usr/bin/kafka-topics --list --zookeeper zookeeper-1`"

docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movieIds-with-ratings --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic userIds-to-movieIds-ratings --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic eof --zookeeper zookeeper-1

for ((i=0; i<=$2; i++)); do
   docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-"$i" --zookeeper zookeeper-1
   docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-"$i" --zookeeper zookeeper-1
done

docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movieIds-with-ratings --replication-factor 1 --partitions "$1" --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic userIds-to-movieIds-ratings --replication-factor 1 --partitions "$1" --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic eof --replication-factor 1 --partitions "$1" --zookeeper zookeeper-1

for ((i=0; i<$2; i++)); do
   docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-"$i" --replication-factor 1 --partitions "$1" --zookeeper zookeeper-1
   docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-"$i" --replication-factor 1 --partitions "$1" --zookeeper zookeeper-1
done

docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-"$2" --replication-factor 1 --partitions 1 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-"$2" --replication-factor 1 --partitions 1 --zookeeper zookeeper-1

#echo "topics after: `docker-compose exec kafka-1 ./usr/bin/kafka-topics --list --zookeeper zookeeper-1`"

cd ..