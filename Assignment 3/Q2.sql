-- Use the CINEMA database
USE CINEMA;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS DirectedBy, ActIn, Directors, Actors, Movies;


-- Please create the tables as per the structure given.
-- Remember to consider appropriate data types and primary/foreign key constraints.

-- Movies(id, title, year, length, language)
CREATE TABLE Movies(
    id INT PRIMARY KEY,
    title VARCHAR(300) NOT NULL,
    year INT NOT NULL,
    length INT NOT NULL,
    language VARCHAR(30) NOT NULL);

-- Actors(id, name, gender)

CREATE TABLE Actors(
    id INT PRIMARY KEY,
    name VARCHAR(80) NOT NULL,
    gender VARCHAR(30) NOT NULL);


-- ActIn(actor_id, movie_id)
CREATE TABLE ActIn(
    actor_id INT,
    movie_id INT,
    PRIMARY KEY (actor_id, movie_id),
    FOREIGN KEY (actor_id) REFERENCES Actors(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (movie_id) REFERENCES Movies(id) ON DELETE CASCADE ON UPDATE CASCADE);

-- Directors(id, name, nationality)
CREATE TABLE Directors(
    id INT PRIMARY KEY,
    name VARCHAR(80) NOT NULL,
    nationality VARCHAR(120) NOT NULL);


-- DirectedBy(movie_id, director_id)
CREATE TABLE DirectedBy(
    movie_id INT,
    director_id INT,
    PRIMARY KEY (movie_id, director_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (director_id) REFERENCES Directors(id) ON DELETE CASCADE ON UPDATE CASCADE);

-- Please insert sample data into the tables created above.
INSERT INTO Movies (id, title, year, length, language) VALUES
    (1, 'The Twilight Saga: Eclipse', 2010, 124, 'en'),
    (2, 'Catch Me If You Can', 2002, 141, 'en'),
    (3, 'The Terminal', 2005, 127, 'en'),
    (4, 'Les Miserables', 2012, 150, 'fr'),
    (5, 'Inception', 2010, 148, 'en');

INSERT INTO Actors (id, name, gender) VALUES
    (1, 'Tom Hanks', 'male'),
    (2, 'Leonardo DiCaprio', 'male'),
    (3, 'Ellen Page', 'female'),
    (4, 'Kristen Stewart', 'female'),
    (5, 'Robert Pattinson', 'male');

INSERT INTO ActIn (actor_id, movie_id) VALUES
    (1, 2),
    (1, 3),
    (2, 2),
    (3, 5),
    (4, 1),
    (5, 1);

INSERT INTO Directors (id, name, nationality) VALUES
    (1, 'Steven Spielberg', 'American'),
    (2, 'Christopher Nolan', 'British'),
    (3, 'Jean-Pierre Jeunet', 'French'),
    (4, 'David Slade', 'British');


INSERT INTO DirectedBy (movie_id, director_id) VALUES
    (1, 4),
    (2, 1),
    (3, 1),
    (4, 3),
    (5, 2);

-- Note: Testing will be conducted on a blind test set, so ensure your table creation and data insertion scripts are accurate and comprehensive.
