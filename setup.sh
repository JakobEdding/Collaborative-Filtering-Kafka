cd dev

echo "topics before: `docker-compose exec kafka-1 ./usr/bin/kafka-topics --list --zookeeper zookeeper-1`"

docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movieIds-with-ratings --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic userIds-to-movieIds-ratings --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic eof --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-0 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-0 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-1 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-1 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-2 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-2 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-3 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-3 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-5 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-5 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-6 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-6 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic user-features-7 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --delete --topic movie-features-7 --zookeeper zookeeper-1

docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movieIds-with-ratings --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic userIds-to-movieIds-ratings --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic eof --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-0 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-0 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-1 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-1 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-2 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-2 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-3 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-3 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-4 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-4 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-5 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-5 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic user-features-6 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1
docker-compose exec kafka-1 ./usr/bin/kafka-topics --create --topic movie-features-6 --replication-factor 1 --partitions 4 --zookeeper zookeeper-1

echo "topics after: `docker-compose exec kafka-1 ./usr/bin/kafka-topics --list --zookeeper zookeeper-1`"

cd ..