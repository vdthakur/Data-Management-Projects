USE CINEMA;

-- 1) Find titles of the longest movies. Note that there might be more than such movie.
SELECT title FROM Movies WHERE length = (SELECT MAX(length) FROM Movies);

-- 2) Find out titles of movies that contain "Twilight" and are directed by "Steven Spielberg".
SELECT m.title FROM Movies m JOIN DirectedBy dirb ON m.id = dirb.movie_id JOIN Directors d ON dirb.director_id = d.id
WHERE m.title LIKE '%Twilight%' AND d.name = 'Steven Spielberg';

-- 3) Find out how many movies "Tom Hanks" has acted in.
SELECT COUNT(*) FROM ActIn ai JOIN Actors a ON ai.actor_id = a.id WHERE a.name = 'Tom Hanks';

-- 4) Find out which director directed only a single movie.
SELECT d.name FROM Directors d JOIN DirectedBy dirb ON d.id = dirb.director_id GROUP BY d.name HAVING COUNT(dirb.movie_id) = 1;

-- 5) Find titles of movies which have the largest number of actors. Note that there may be multiple such movies.
SELECT m.title FROM Movies m JOIN ActIn ai ON m.id = ai.movie_id GROUP BY m.title
HAVING COUNT(ai.actor_id) = (SELECT MAX(ActorCount) FROM (SELECT COUNT(ai.actor_id) AS ActorCount 
FROM Movies m JOIN ActIn ai ON m.id = ai.movie_id GROUP BY m.id) AS ActorCounts);

-- 6) Find names of actors who played in both English (language = "en") and French ("fr") movies.

SELECT DISTINCT a.name FROM Actors a JOIN ActIn ai ON a.id = ai.actor_id JOIN Movies m ON ai.movie_id = m.id
WHERE m.language IN ('en', 'fr') GROUP BY a.name HAVING COUNT(DISTINCT m.language) > 1;

-- 7) Find names of directors who only directed English movies.
SELECT DISTINCT d.name FROM Directors d
WHERE NOT EXISTS (SELECT 1 FROM DirectedBy dirb JOIN Movies m ON dirb.movie_id = m.id WHERE dirb.director_id = d.id AND m.language != 'en');
