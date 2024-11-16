from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Film Dataset Analysis").getOrCreate()

# Load the datasets as DataFrames
film_df = spark.read.csv('film.csv', header=True, inferSchema=True)
actor_df = spark.read.csv('actor.csv', header=True, inferSchema=True)
film_actor_df = spark.read.csv('film_actor.csv', header=True, inferSchema=True)

# Convert DataFrames to RDDs
film_rdd = film_df.rdd
actor_rdd = actor_df.rdd
film_actor_rdd = film_actor_df.rdd

def query_a(film_rdd):
    """
    Select title and description from the film dataset where the rating is 'PG'.
    """
    # Implement the RDD transformation and action here
    q_a = film_rdd.filter(lambda row: row.rating=='PG').map(lambda row:(row.title,row.description))
    return q_a

# %query_a output:

# [('ACADEMY DINOSAUR', 'A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies'), 
# ('AGENT TRUMAN', 'A Intrepid Panorama of a Robot And a Boy who must Escape a Sumo Wrestler in Ancient China'), 
# ('ALASKA PHANTOM', 'A Fanciful Saga of a Hunter And a Pastry Chef who must Vanquish a Boy in Australia'), 
# ('ALI FOREVER', 'A Action-Packed Drama of a Dentist And a Crocodile who must Battle a Feminist in The Canadian Rockies'), 
# ('AMADEUS HOLY', 'A Emotional Display of a Pioneer And a Technical Writer who must Battle a Man in A Baloon')]

def query_b(film_rdd):
    """
    Select the average replacement cost grouped by rating for films longer than 60 minutes,
    having at least 160 films per rating.
    """
    # Implement the RDD transformation and action here
    q_b = film_rdd.filter(lambda row: row.length >= 60).map(lambda row:(row.rating,(row.replacement_cost,1))).reduceByKey(lambda a,b:(a[0] + b[0],a[1] + b[1])) \
    .mapValues(lambda x:(x[0] / x[1], x[1])).filter(lambda x: x[1][1] >= 160).map(lambda x:(x[0], x[1][0]))
    return q_b

# %query_b output:

# [('PG', 18.84465116279062), ('PG-13', 20.579108910890984), ('NC-17', 20.265132275132174), ('R', 20.294347826086863)]

def query_c(film_actor_rdd):
    """
    Select actor IDs that appear in both film ID 1 and film ID 23.
    """
    # Implement the RDD transformation and action here
    q_c_part_a = film_actor_rdd.filter(lambda row:row.film_id==1).map(lambda row:row.actor_id)
    q_c_part_b = film_actor_rdd.filter(lambda row:row.film_id==23).map(lambda row:row.actor_id)
    q_c = q_c_part_a.intersection(q_c_part_b)
    return q_c

# %query_c output:

# [1]

def query_d(actor_rdd, film_actor_rdd):
    """
    Select distinct first name and last name of actors who acted in films 1, 2, or 3.
    """
    # Implement the RDD transformation and action here
    film_actors = film_actor_rdd.filter(lambda row:row.film_id in (1, 2, 3))
    actor_join = actor_rdd.map(lambda row:(row.actor_id,(row.first_name,row.last_name)))
    film_actor_join = film_actors.map(lambda row:(row.actor_id, row.film_id))
    joined = actor_join.join(film_actor_join)
    distinct_actors = joined.map(lambda row:(row[1][0][0],row[1][0][1])).distinct()
    q_d = distinct_actors.sortBy(lambda x: x[0])
    return q_d

# %query_d output:

# [('BOB', 'FAWCETT'), ('CAMERON', 'STREEP'), ('CHRIS', 'DEPP'), ('CHRISTIAN', 'GABLE'), ('JOHNNY', 'CAGE')]

def query_e(film_rdd):
    """
    Select rental duration, rating, minimum, maximum, and average length, and count of films,
    grouped by rental duration and rating, ordered by rental duration descending.
    """
    # Implement the RDD transformation and action here
    q_e = film_rdd.map(lambda row:((row.rental_duration,row.rating),(row.length,row.length,row.length,1))).reduceByKey(lambda a, b:(min(a[0],b[0]),max(a[1],b[1]),a[2] + b[2],a[3] + b[3])) \
    .mapValues(lambda x:(x[0],x[1],x[2] / x[3],x[3])).sortBy(lambda x:x[0][0],ascending=False)
    return q_e

# %query_e output:

# [((7, 'NC-17'), (48, 179, 118.7, 40)), ((7, 'PG-13'), (48, 185, 118.27272727272727, 44)), 
# ((7, 'PG'), (46, 182, 111.95555555555555, 45)), ((7, 'G'), (49, 185, 116.34482758620689, 29)), 
# ((7, 'R'), (59, 185, 131.27272727272728, 33)), ((6, 'PG'), (49, 182, 104.82051282051282, 39)), 
# ((6, 'G'), (57, 183, 128.0, 39)), ((6, 'PG-13'), (46, 185, 118.52, 50)), ((6, 'R'), (54, 181, 127.18518518518519, 27)), 
# ((6, 'NC-17'), (48, 184, 111.78947368421052, 57))]

def main():
    print("Query A:")
    print(query_a(film_rdd).take(5))

    print("Query B:")
    print(query_b(film_rdd).collect())

    print("Query C:")
    print(query_c(film_actor_rdd).collect())

    print("Query D:")
    print(query_d(actor_rdd, film_actor_rdd).take(5))

    print("Query E:")
    print(query_e(film_rdd).take(10))

if __name__ == "__main__":
    main()
