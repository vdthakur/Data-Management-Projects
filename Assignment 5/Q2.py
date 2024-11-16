from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

# Initialize SparkSession and load the datasets
spark = SparkSession.builder.appName("Film Dataset Analysis").getOrCreate()
film = spark.read.csv('film.csv', header=True, inferSchema=True)
actor = spark.read.csv('actor.csv', header=True, inferSchema=True)
film_actor = spark.read.csv('film_actor.csv', header=True, inferSchema=True)

def query_a(film_df):
    """
    Question: Select title and description from the film dataset where the rating is 'PG'.
    """
    # Implement the query here
    q_a = film.filter(film.rating=="PG").select("title","description").limit(5)
    return q_a

#  query_a Output:

# +----------------+--------------------+
# |           title|         description|
# +----------------+--------------------+
# |ACADEMY DINOSAUR|A Epic Drama of a...|
# |    AGENT TRUMAN|A Intrepid Panora...|
# |  ALASKA PHANTOM|A Fanciful Saga o...|
# |     ALI FOREVER|A Action-Packed D...|
# |    AMADEUS HOLY|A Emotional Displ...|
# +----------------+--------------------+


def query_b(film_df):
    """
    Question: Select the average replacement cost grouped by rating for films longer than 60 minutes,
              having at least 160 films per rating.
    """
    # Implement the query here
    q_b = (film.filter(film.length>=60).groupBy("rating").agg(fc.avg("replacement_cost").alias("average_replacement_cost"),fc.count("*").alias("count")).filter("count >= 160").select("rating","average_replacement_cost"))
    return q_b

# query_b Output:

# +------+------------------------+
# |rating|average_replacement_cost|
# +------+------------------------+
# |    PG|       18.84465116279062|
# | NC-17|      20.265132275132174|
# |     R|      20.294347826086863|
# | PG-13|      20.579108910890984|
# +------+------------------------+


def query_c(film_actor_df):
    """
    Question: Select actor IDs that appear in both film ID 1 and film ID 23.
    """
    # Implement the query here
    q_c_part_a = film_actor.filter(film_actor.film_id == 1).select("actor_id")
    q_c_part_b = film_actor.filter(film_actor.film_id == 23).select("actor_id")
    q_c = q_c_part_a.intersect(q_c_part_b)
    return q_c

# query_c Output:

# +--------+
# |actor_id|
# +--------+
# |       1|
# +--------+


def query_d(actor_df, film_actor_df):
    """
    Question: Select distinct first name and last name of actors who acted in films 1, 2, or 3.
              Order the result by first name.
    """
    # Implement the query here
    q_d = actor.join(film_actor,"actor_id").filter(film_actor.film_id.isin([1,2,3])).select("first_name","last_name").distinct().orderBy("first_name").limit(5)
    return q_d

# query_d Output:

# +----------+---------+
# |first_name|last_name|
# +----------+---------+
# |       BOB|  FAWCETT|
# |   CAMERON|   STREEP|
# |     CHRIS|     DEPP|
# | CHRISTIAN|    GABLE|
# |    JOHNNY|     CAGE|
# +----------+---------+    



def query_e(film_df):
    """
    Question: Select rental duration, rating, minimum, maximum, and average length, and count of films,
              grouped by rental duration and rating, ordered by rental duration descending.
    """
    # Implement the query here
    q_e = film.groupBy("rental_duration","rating").agg(fc.min("length").alias("min_length"),fc.max("length").alias("max_length"),fc.avg("length").alias("avg_length"),
    fc.count("length").alias("count_length")).orderBy(fc.desc("rental_duration")).limit(10)
    return q_e

# query_e Output:

# +---------------+------+----------+----------+------------------+------------+
# |rental_duration|rating|min_length|max_length|        avg_length|count_length|
# +---------------+------+----------+----------+------------------+------------+
# |              7|     R|        59|       185|131.27272727272728|          33|
# |              7| NC-17|        48|       179|             118.7|          40|
# |              7|    PG|        46|       182|111.95555555555555|          45|
# |              7|     G|        49|       185|116.34482758620689|          29|
# |              7| PG-13|        48|       185|118.27272727272727|          44|
# |              6| NC-17|        48|       184|111.78947368421052|          57|
# |              6| PG-13|        46|       185|            118.52|          50|
# |              6|     G|        57|       183|             128.0|          39|
# |              6|     R|        54|       181|127.18518518518519|          27|
# |              6|    PG|        49|       182|104.82051282051282|          39|
# +---------------+------+----------+----------+------------------+------------+


def main():
    print("Query A:")
    query_a(film).show()

    print("Query B:")
    query_b(film).show()

    print("Query C:")
    query_c(film_actor).show()

    print("Query D:")
    query_d(actor, film_actor).show()

    print("Query E:")
    query_e(film).show()

if __name__ == "__main__":
    main()
