# Collaborative Filtering in kafka

Collaborative filtering is a technique in which feedback from users for items (movies, songs, clothing ...) is used to predict how others users would rate these items.
This plays a major role for many companies that deal with huge user / item datasets and want to make predictions / recommendations for these users, e.g. Netflix, Spotify or Amazon.
We focus on explicit feedback, which means explicit ratings from users instead of implicit measurements like "time spent viewing item x" or mouse movements.
The problem can be imagined as a giant matrix where the users are the rows and the items are the columns.
Some cells contain the existing ratings, but the majority are empty (the matrix is very sparse) and we want to fill these empty cells to make predictions.

RatingsMatrix | Item1 | Item2
--- | --- | ---
User1 | null | 1
User2 | 2 | 3
User3 | null | null
User4 | 2 | null


## Alternating Least Squares
The algorithm we implemented is called Alternating Least Squares.
It tackles the problem of Collaborative Filtering with matrix multiplication.
ALS tries to find two low rank (to save storage and computing power) matrices whose multiplication approximates the original matrix ad also makes predictions for the empty fields.

> Zhou, Y., Wilkinson, D., Schreiber, R., & Pan, R. (2008, June). Large-scale parallel collaborative filtering for the netflix prize. In International conference on algorithmic applications in management (pp. 337-348). Springer, Berlin, Heidelberg.

### Implementation in Spark
One version of this algorithm is implemented in the Spark MLLib and makes some additional important optimizations for distributed calculation of ALS, as the data often is very large.
The current implementation is described n detail in this paper:
> Das, A., Upadhyaya, I., Meng, X., & Talwalkar, A. (2017, November). Collaborative Filtering as a Case-Study for Model Parallelism on Bulk Synchronous Systems. In Proceedings of the 2017 ACM on Conference on Information and Knowledge Management (pp. 969-977).
They take pressure of the Java garbage collector by using primitive arrays instead of tuples of integers and optimize the memory footprint of ALS by using smaller arrays and packing multiple values in a single integer.
In addition to greatly optimizing the memory consumption, the main takeaway is their approach to handling the communication of a distributed ALS approach.

// TODO

### Architecture in kafka

## Usage

To start the confluence kafka docker image execute the following:

`$ (cd dev && docker-compose down -v && docker-compose up)`

Wait for the broker, zookeeper and control center to be up and running. You can find the Control center at [http://0.0.0.0:9021/](http://0.0.0.0:9021/).

Run `./setup.sh` to (re)create necessary topics that are not auto-created.

Run ALSAppRunner.main() from your IDE or execute `./gradlew run`.