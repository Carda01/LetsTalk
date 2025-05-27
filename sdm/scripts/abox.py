
import sys
import os
import names
import random
from sklearn.feature_extraction.text import TfidfVectorizer
sys.path.append(os.path.abspath("../../")) 
import hashlib
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD

# Your project modules
from dags.lib.pt_utils import *
from dags.lib.Processer import *
from delta import *

import math

from pyspark.sql.functions import col, concat_ws, when
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import Counter

def extract_column_keywords(df, text_columns=['content', 'title', 'description'], top_n=10):
    """
    Returns: List of top keywords (without frequencies)
    """
    combined_df = df.withColumn(
        "combined_text",
        concat_ws(" ", *[
            when(col(c).isNotNull(), col(c)).otherwise("")
            for c in text_columns
        ]))
    
    texts = [row.combined_text for row in combined_df.select("combined_text").collect()]
    
    vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
    X = vectorizer.fit_transform(texts)
    
    feature_names = vectorizer.get_feature_names_out()
    keywords = []
    
    for i in range(X.shape[0]):
        scores = X[i].toarray().flatten()
        top_indices = scores.argsort()[-3:][::-1]
        keywords.extend(feature_names[top_indices])
    
    # Return just the words as a list
    return [word for word, count in Counter(keywords).most_common(top_n)]

g = Graph()
DBO = Namespace("http://sdm_upc.org/ontology/")
DBR = Namespace("http://sdm_upc.org/resource/")


g.bind("dbo", DBO)
g.bind("dbr", DBR)

def consistent_hash(value):
    return int(hashlib.sha256(str(value).encode()).hexdigest(), 16)

USERS=100

is_gcs_enabled= "False"
if is_gcs_enabled.lower() == 'true':
    is_gcs_enabled = True
else:
    is_gcs_enabled = False

spark, base_path = get_spark_and_path(is_gcs_enabled)

trusted_path ='..\..\data\letstalk_trusted_zone_bdma'

#SPORTS
## Matches

subpath = 'matches'
path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()

for row in df.toLocalIterator():
    if row.status_long in ('match finished', 'walkover', 'technical loss', 'match abandoned'):
        subject = URIRef(DBR + f"match_{row.fixture_id}")

        if row.team_home_id is not None:
            g.add((subject, DBO.match_home_team, URIRef(DBR + f"team_{row.team_home_id}")))

        if row.team_away_id is not None:
            g.add((subject, DBO.match_away_team, URIRef(DBR + f"team_{row.team_away_id}")))

        if row.goals_home is not None:
            g.add((subject, DBO.match_home_goals, Literal(int(row.goals_home), datatype=XSD.integer)))
            
        if row.goals_away is not None:
            g.add((subject, DBO.match_away_goals, Literal(int(row.goals_away), datatype=XSD.integer)))

        if row.fixture_date is not None:
            g.add((subject, DBO.match_date, Literal(row.fixture_date, datatype=XSD.dateTime)))

        if row.status_long is not None:
            g.add((subject, DBO.match_played, Literal(str(row.status_long), datatype=XSD.string)))

        if row.league is not None:
            g.add((subject, DBO.match_competition, URIRef(DBR + f"competition_{row.league}")))

        if row.referee is not None:
            g.add((subject, DBO.match_referee, URIRef(DBR + f"referee_{consistent_hash(row.referee)}")))
            g.add((URIRef(DBR + f"referee_{consistent_hash(row.referee)}"), DBO.referee_name, Literal(str(row.referee), datatype=XSD.string)))

        if row.venue_id is not None:
            g.add((subject, DBO.match_venue, URIRef(DBR + f"venue_{consistent_hash(row.venue_id)}")))


## Competitions

subpath= 'leagues'
path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()
league_ids = df.select("league_id").distinct().rdd.map(lambda row: row.league_id).collect()
country_ids = df.select("country").distinct().rdd.map(
    lambda row: row.country.code
).filter(lambda code: code is not None).collect()
for row in df.toLocalIterator():
    subject = URIRef(DBR + f"competition_{row.league_id}")
    if row.league_type == 'cup':
        g.add((subject, RDF.type, DBO.Cup))
    else:
        g.add((subject, RDF.type, DBO.League))
    
    if row.league_name is not None:
        g.add((subject, DBO.competition_name,Literal(str(row.league_name), datatype=XSD.string)))

    if row.country is not None:
        if row.country.name== 'World':
            g.add((subject, DBO.competition_country, URIRef(DBR + f"country_{consistent_hash('world')}")))
            g.add((URIRef(DBR + f"country_{consistent_hash('world')}"), DBO.country_name, Literal(str(row.country.name), datatype=XSD.string)))
        else:
            g.add((subject, DBO.competition_country, URIRef(DBR + f"country_{consistent_hash(row.country.code)}")))
            g.add((URIRef(DBR + f"country_{consistent_hash(row.country.code)}"), DBO.country_name, Literal(str(row.country.name), datatype=XSD.string)))
        
            
## Teams
subpath= 'teams'
path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()
team_ids = df.select("team_id").distinct().rdd.map(lambda row: row.team_id).collect()
for row in df.toLocalIterator():
    subject = URIRef(DBR + f"team_{row.team_id}")
    if row.team_name is not None:
        g.add((subject, DBO.team_name, Literal(str(row.team_name), datatype=XSD.string)))
        
## Venues
subpath= 'venues'
path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()

for row in df.toLocalIterator():
    subject = URIRef(DBR + f"venue_{consistent_hash(row.venue_id)}")
    if row.venue_name is not None:
        g.add((subject, DBO.venue_name, Literal(str(row.venue_name), datatype=XSD.string)))
    if row.venue_city is not None:
        g.add((subject, DBO.venue_city, Literal(str(row.venue_city), datatype=XSD.string)))

#ENTERTAINMENT
##Movies

subpath= 'movie'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()
film_ids = df.select("film_id").distinct().rdd.map(lambda row: row.film_id).collect()
for row in df.toLocalIterator():

    subject = URIRef(DBR+f"film_{row.film_id}")
    
    if row.title is not None:
        g.add((subject, DBO.movie_title, Literal(str(row.title), datatype=XSD.string )))
    if row.original_title is not None:
        g.add((subject, DBO.movie_language, Literal(str(row.original_title), datatype=XSD.string )))
    if row.release_date is not None:
        g.add((subject, DBO.movie_release_date, Literal(row.release_date, datatype=XSD.date )))
    if row.revenue is not None:
        g.add((subject, DBO.movie_revenue, Literal(int(row.revenue), datatype=XSD.integer)))
    if row.budget is not None:
        g.add((subject, DBO.movie_budget, Literal(int(row.budget), datatype=XSD.integer )))
    runtime_value = row.runtime
    if runtime_value is not None and not math.isnan(runtime_value):
        g.add((subject, DBO.movie_runtime, Literal(int(runtime_value), datatype=XSD.integer)))
    if row.adult is not None:
        g.add((subject, DBO.movie_adult, Literal(bool(row.adult), datatype=XSD.boolean)))
    if row.popularity is not None:
        g.add((subject, DBO.movie_popularity, Literal(float(row.popularity), datatype=XSD.long)))
    if row.vote_average is not None:
        g.add((subject, DBO.movie_vote_avg, Literal(float(row.vote_average), datatype=XSD.long)))
    if row.vote_count is not None:
        g.add((subject, DBO.movie_vote_cnt, Literal(int(row.vote_count), datatype=XSD.integer)))

subpath= 'movie_genre'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()

for row in df.toLocalIterator():
    subject = URIRef(DBR+f"film_{row.film_id}")
    object = URIRef(DBR+f"genre_{row.genre_id}")
    g.add((subject, DBO.has_genre, object))

subpath= 'trending'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()

for row in df.toLocalIterator():
    subject = URIRef(DBR+f"film_{row.film_id}")
    g.add((subject, DBO.trending_movie, Literal(bool(True), datatype=XSD.boolean)))
  
subpath= 'upcoming'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()

for row in df.toLocalIterator():
    subject = URIRef(DBR+f"film_{row.film_id}")
    g.add((subject, DBO.upcoming_movie, Literal(bool(True), datatype=XSD.boolean)))

subpath= 'now_playing'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()

for row in df.toLocalIterator():
    subject = URIRef(DBR+f"film_{row.film_id}")
    g.add((subject, DBO.now_playing_movie, Literal(bool(True), datatype=XSD.boolean)))
    
## Genres
subpath= 'genre'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()
genre_ids = df.select("genre_id").distinct().rdd.map(lambda row: row.genre_id).collect()
for row in df.toLocalIterator():
    subject = URIRef(DBR+f"genre_{row.genre_id}")
    g.add((subject, DBO.genre_name, Literal(str(row.genre), datatype=XSD.string )))
    



#NEWS
kws=[]

subpath= 'entertainment'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()
kw= extract_column_keywords(df,top_n= 15)
kws.append(kw)
for row in df.toLocalIterator():
    subject = URIRef(DBR+f"news_{consistent_hash(row.url)}")
    g.add((subject, RDF.type, DBO.Entertainment_News))
    if row.author is not None:
        g.add((subject, DBO.written_by, URIRef(DBR+f"author_{consistent_hash(row.author)}")))
        g.add((URIRef(DBR+f"author_{consistent_hash(row.author)}"), DBO.author_name, Literal(str(row.author), datatype=XSD.string )))
    if row.source is not None:
        g.add((subject, DBO.published_at,  URIRef(DBR+f"source_{consistent_hash(row.source)}")))
        g.add((URIRef(DBR+f"source_{consistent_hash(row.source)}"), DBO.source_name, Literal(str(row.source), datatype=XSD.string )))
    if row.title is not None:
        g.add((subject, DBO.news_title, Literal(str(row.title), datatype=XSD.string )))
    if row.publishedAt is not None:
        g.add((subject, DBO.news_date, Literal(row.publishedAt, datatype=XSD.dateTime)))
    
    text_fields = [
        (row.title or "").lower(),
        (row.content or "").lower(),
        (row.description or "").lower()
    ]

    for keyword in kw:
        keyword_lower = keyword.lower()
        if any(keyword_lower in field for field in text_fields):
            g.add((subject, DBO.related_keyword, URIRef(DBR + f"keyword_{consistent_hash(keyword)}")))
            g.add((URIRef(DBR + f"keyword_{consistent_hash(keyword)}"),DBO.keyword_text, Literal(str(keyword), datatype=XSD.string )))

subpath= 'sports'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()
kw= extract_column_keywords(df,top_n= 15)
kws.append(kw)
for row in df.toLocalIterator():
    subject = URIRef(DBR+f"news_{consistent_hash(row.url)}")
    g.add((subject, RDF.type, DBO.Sports_News))
    if row.author is not None:
        g.add((subject, DBO.written_by, URIRef(DBR+f"author_{consistent_hash(row.author)}")))
        g.add((URIRef(DBR+f"author_{consistent_hash(row.author)}"), DBO.author_name, Literal(str(row.author), datatype=XSD.string )))
    if row.source is not None:
        g.add((subject, DBO.published_at,  URIRef(DBR+f"source_{consistent_hash(row.source)}")))
        g.add((URIRef(DBR+f"source_{consistent_hash(row.source)}"), DBO.source_name, Literal(str(row.source), datatype=XSD.string )))
    if row.title is not None:
        g.add((subject, DBO.news_title, Literal(str(row.title), datatype=XSD.string )))
    if row.publishedAt is not None:
        g.add((subject, DBO.news_date, Literal(row.publishedAt, datatype=XSD.dateTime)))
    
    text_fields = [
        (row.title or "").lower(),
        (row.content or "").lower(),
        (row.description or "").lower()
    ]

    for keyword in kw:
        keyword_lower = keyword.lower()
        if any(keyword_lower in field for field in text_fields):
            g.add((subject, DBO.related_keyword, URIRef(DBR + f"keyword_{consistent_hash(keyword)}")))
            g.add((URIRef(DBR + f"keyword_{consistent_hash(keyword)}"),DBO.keyword_text, Literal(str(keyword), datatype=XSD.string )))

subpath= 'technology'

path = os.path.join(trusted_path, subpath)
df = DeltaTable.forPath(spark, path).toDF()
kw= extract_column_keywords(df,top_n= 15)
kws.append(kw)
for row in df.toLocalIterator():
    subject = URIRef(DBR+f"news_{consistent_hash(row.url)}")
    g.add((subject, RDF.type, DBO.Tech_News))
    if row.author is not None:
        g.add((subject, DBO.written_by, URIRef(DBR+f"author_{consistent_hash(row.author)}")))
        g.add((URIRef(DBR+f"author_{consistent_hash(row.author)}"), DBO.author_name, Literal(str(row.author), datatype=XSD.string )))
    if row.source is not None:
        g.add((subject, DBO.published_at,  URIRef(DBR+f"source_{consistent_hash(row.source)}")))
        g.add((URIRef(DBR+f"source_{consistent_hash(row.source)}"), DBO.source_name, Literal(str(row.source), datatype=XSD.string )))
    if row.title is not None:
        g.add((subject, DBO.news_title, Literal(str(row.title), datatype=XSD.string )))
    if row.publishedAt is not None:
        g.add((subject, DBO.news_date, Literal(row.publishedAt, datatype=XSD.dateTime)))
    
    text_fields = [
        (row.title or "").lower(),
        (row.content or "").lower(),
        (row.description or "").lower()
    ]

    for keyword in kw:
        keyword_lower = keyword.lower()
        if any(keyword_lower in field for field in text_fields):
            g.add((subject, DBO.related_keyword, URIRef(DBR + f"keyword_{consistent_hash(keyword)}")))
            g.add((URIRef(DBR + f"keyword_{consistent_hash(keyword)}"),DBO.keyword_text, Literal(str(keyword), datatype=XSD.string )))


#USERS
for i in range(USERS):
    subject = URIRef(DBR+f"user_{i}")
    g.add((subject, DBO.user_name, Literal(str(names.get_full_name()), datatype=XSD.string )))
    g.add((subject, DBO.user_country, URIRef(DBR+f"country_{consistent_hash(random.choice(country_ids))}")))
     # Likes movies
    liked_films = random.sample(film_ids, min(len(film_ids), random.randint(0, 3)))
    for film_id in liked_films:
        g.add((subject, DBO.likes_movie, URIRef(DBR + f"film_{film_id}")))

    # Likes genres
    liked_genres = random.sample(genre_ids, min(len(genre_ids), random.randint(0, 3)))
    for genre_id in liked_genres:
        g.add((subject, DBO.likes_genre, URIRef(DBR + f"genre_{genre_id}")))

    # Likes teams
    liked_teams = random.sample(team_ids, min(len(team_ids), random.randint(0, 3)))
    for team_id in liked_teams:
        g.add((subject, DBO.likes_team, URIRef(DBR + f"team_{team_id}")))

    # Likes competitions
    liked_competitions = random.sample(league_ids, min(len(league_ids), random.randint(0, 3)))
    for comp_id in liked_competitions:
        g.add((subject, DBO.likes_competition, URIRef(DBR + f"competititon_{comp_id}")))

    # Interested in keywords
    interested_keywords = random.sample(kws, min(len(kws), random.randint(0, 5)))
    for kw in interested_keywords:
        g.add((subject, DBO.interested_in, URIRef(DBR + f"keyword_{consistent_hash(kw)}")))

g.serialize("../docker-import/abox.ttl", format="turtle")


