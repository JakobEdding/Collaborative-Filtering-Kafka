# Collaborative Filtering in kafka

Collaborative filtering is a technique in which feedback from users for items (movies, songs, clothing ...) is used to predict how others users would rate these items.
This plays a major role for many companies that deal with huge user / item datasets and want to make predictions / recommendations for these users, e.g. Netflix, Spotify or Amazon.
We focus on explicit feedback, which means explicit ratings from users instead of implicit measurements like "time spent viewing item x" or mouse movements.
We use movies as an example for items throughout out project.
The problem can be imagined as a giant matrix where the users are the rows and the items are the columns.
Some cells contain the existing ratings, but the majority are empty (the matrix is very sparse in practice) and we want to fill these empty cells to make predictions.

RatingsMatrix | Movie1 | Movie2 | Movie1 | Movie2
--- | --- | --- | --- | ---
User1 | null | 1 | null | 5
User2 | 2 | 3 | null | null
User3 | null | null | null | 4
User4 | 2 | null | 3 | null


## Alternating Least Squares
The algorithm we implemented is called Alternating Least Squares.
Netflix once started a challenge for improving it's prediction system.
The winning solution used different approaches, but ALS was one of them.
A detailed explanation can be found in the original paper:
> Zhou, Y., Wilkinson, D., Schreiber, R., & Pan, R. (2008, June). Large-scale parallel collaborative filtering for the netflix prize. In International conference on algorithmic applications in management (pp. 337-348). Springer, Berlin, Heidelberg.

ALS tackles the problem of Collaborative Filtering by using matrix factorization.
It finds two low rank matrices (to save storage and computing power) whose multiplication approximates the original matrix and which then also contain predictions for the empty fields.
These two matrices could look like this (values aren't correct in this example):

UserFeatureMatrix | Feature1 | Feature2
--- | --- | ---
User1 | 1.3 | 1.5
User2 | 2.0 | 3.2
User3 | 1.5 | 0.4
User4 | 2.4 | 4.2

MovieFeatureMatrix | Feature1 | Feature2
--- | --- | ---
Movie1 | 2.3 | 4.5
Movie2 | 2.2 | 1.2
Movie3 | 1.0 | 3.4
Movie4 | 2.6 | 3.2

These matrices would be multiplied to get the final prediction table:

FinalPredictionMatrix = UserFeatureMatrix * MovieFeatureMatrix<sup>T</sup>

To calculate these two feature tables, ALS needs an error function.
We are using a very similar function as the one described in the paper.
We calculate the RMSE between all cells that contain ratings in the original ratings matrix and the corresponding cells of the prediction matrix.
As we are using low rank approximations, it is very unlikely that ALS will correctly "predict" the original ratings.
The paper then normalizes the RMSE with a parameter lambda and, depending on the cell, with the number of ratings of the corresponding user and movie.
In our approach, we also normalize with lambda, but depending on the step we are in (see below), we only have access to the number of ratings of the user or the movie respectively.
We use this number to normalize in addition to lambda.

### Algorithm
We describe the steps of the algorithm schematically.
For the detailed mathematical explanation we refer to the paper above.

#### 0. Initialize UserFeatureMatrix
Initialize the UserFeatureMatrix with small random values in (0,1).

#### 1. Calculate MovieFeatureMatrix
Using the error function, we calculate a closed form solution for the MovieFeatureMatrix, because the original RatingsMatrix and the UserFeatureMatrix are fixed at this point.

#### 2. Calculate UserFeatureMatrix
Now we fix the MovieFeatureMatrix and calculate the UserFeatureMatrix from the error function.

#### 3. Repeat Step 1 and 2
Experiments have shown (see paper below) that even for the largest datasets this algorithm converges in 5 - 20 repetitions of these two steps.
We set a number of iterations in the beginning and repeat accordingly.

#### 4. Calculate Predicitions
In the end, we need to calculate the FinalPredictionMatrix by multiplying the two feature matrices.
This yields an approximation of the original RatingsMatrix with predictions for the previously empty cells.

### Distributing ALS

// TODO

### Implementation in Spark
One version of this algorithm is implemented in the Spark MLLib and makes some additional important optimizations for distributed calculation of ALS, as the data often is very large.
The current implementation is described n detail in this paper:
> Das, A., Upadhyaya, I., Meng, X., & Talwalkar, A. (2017, November). Collaborative Filtering as a Case-Study for Model Parallelism on Bulk Synchronous Systems. In Proceedings of the 2017 ACM on Conference on Information and Knowledge Management (pp. 969-977).

They take pressure of the Java garbage collector by using primitive arrays instead of tuples of integers and optimize the memory footprint of ALS by using smaller arrays and packing multiple values in a single integer.
In addition to greatly optimizing the memory consumption, the main takeaway is their approach to handling the communication of a distributed ALS approach.

// TODO

### Architecture in kafka

// TODO

### Benchmarks / Struggles

// TODO talk about biggest dataset, dropped messages\
// mention problems we overcame?\
// spark able to do way larger

## Usage

To start the confluence kafka docker image execute the following:

`$ (cd dev && docker-compose down -v && docker-compose up)`

Wait for the broker, zookeeper and control center to be up and running. You can find the Control center at [http://0.0.0.0:9021/](http://0.0.0.0:9021/).

Run `./setup.sh NUM_PARTITIONS NUM_ITERATIONS` (e.g. `./setup.sh 4 10`) to (re)create necessary topics that are not auto-created.

Run `./gradlew run --args='NUM_PARTITIONS NUM_FEATURES LAMBDA NUM_ITERATIONS PATH_TO_DATASET NUM_MOVIES NUM_USERS'`, e.g. `./gradlew run --args='4 10 0.05 10 ./data/data_sample_tiny.txt 426 302'`.

The options for the three supplied datasets (taken from the netflix prize challenge) are:

Dataset path | NUM_MOVIES | NUM_USERS
--- | ---: | ---:
./data/data_sample_tiny.txt | 426 | 302
./data/data_sample_small.txt | 2062 | 1034
./data/data_sample_medium.txt | 3590 | 2120
