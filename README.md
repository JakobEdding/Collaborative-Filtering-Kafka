`(cd dev && docker-compose down -v && docker-compose up)`

Wait for the broker, zookeeper and control center to be up and running. Control center is at [http://0.0.0.0:9021/](http://0.0.0.0:9021/).

Run `./setup.sh` to (re)create necessary topics that are not auto-created.

Run ALSAppRunner.main() from your IDE or execute `./gradlew run`.