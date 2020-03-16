# limitation: expects movie_id to be sorted in ratings_matrix file and predictions_matrix to be sorted by both movie_id and user_id

import numpy as np
import sys
import math

if len(sys.argv) != 3:
    print("need 2 arguments")
    sys.exit()

users = set()
movies = set()

current_movie_id = -1
with open(sys.argv[1], 'r') as ratings_matrix:
    for line in ratings_matrix:
        line = line.strip()
        if line.endswith(':'):
            current_movie_id = int(line.split(':')[0])
            continue
        movies.add(current_movie_id)
        user_id, _, _ = line.split(",")
        users.add(int(user_id))

print("#users in ratings_matrix: ", len(users))
print("#movies in ratings_matrix: ", len(movies))

ratings_np_matrix = np.empty([len(users), len(movies)])

map_user_id_to_idx = dict()
sorted_users = sorted(list(users))

for idx, user in enumerate(sorted_users):
    map_user_id_to_idx[user] = idx

# read original ratings

col_idx = -1
previous_movie_id = -2
current_movie_id = -1
with open(sys.argv[1], 'r') as ratings_matrix:
    for line in ratings_matrix:
        line = line.strip()
        if line.endswith(':'):
            current_movie_id = int(line.split(':')[0])
            continue
        user_id, rating, _ = line.split(",")
        user_id = int(user_id)
        rating = float(rating)
        if current_movie_id != previous_movie_id:
            previous_movie_id = current_movie_id
            # already sorted by movie_id so we can just increase index and don't need something like map_user_id_to_idx
            col_idx += 1
        try:
            ratings_np_matrix[map_user_id_to_idx[user_id]][col_idx] = rating
        except Exception as e:
            import pdb;pdb.set_trace()


# read predictions
predictions_np_matrix = np.empty([len(users), len(movies)])

with open(sys.argv[2], 'r') as predictions_matrix:
    idx = 0
    for line in predictions_matrix:
        if "real" in line:
            # skip header line
            continue
        line = line.strip()
        for idx_2, cell in enumerate(line.split(" ")):
            try:
                predictions_np_matrix[idx][idx_2] = float(cell)
            except Exception as e:
                import pdb;pdb.set_trace()
        idx += 1


# compute MSE
se = 0.0
count = 0

for i in range(len(users)):
    for j in range(len(movies)):
        if ratings_np_matrix[i][j] != 0:
            se += ((ratings_np_matrix[i][j] - predictions_np_matrix[i][j]) ** 2)
            count += 1

mse = se / float(count)
print("MSE:", mse)
print("RMSE:", math.sqrt(mse))
