# Usage

`$ (cd dev && docker-compose down -v && docker-compose up)`

Wait for the broker, zookeeper and control center to be up and running. Control center is at [http://0.0.0.0:9021/](http://0.0.0.0:9021/).

Run `./setup.sh` to (re)create necessary topics that are not auto-created.

Run ALSAppRunner.main() from your IDE or execute `./gradlew run`.

# Alternating Least Squares

* Zhou, Y., Wilkinson, D., Schreiber, R., & Pan, R. (2008, June). Large-scale parallel collaborative filtering for the netflix prize. In International conference on algorithmic applications in management (pp. 337-348). Springer, Berlin, Heidelberg.

## Implementation in Spark

* Das, A., Upadhyaya, I., Meng, X., & Talwalkar, A. (2017, November). Collaborative Filtering as a Case-Study for Model Parallelism on Bulk Synchronous Systems. In Proceedings of the 2017 ACM on Conference on Information and Knowledge Management (pp. 969-977).

# Architecture