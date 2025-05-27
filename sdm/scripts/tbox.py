
from rdflib import Graph, Namespace
from rdflib.namespace import FOAF, RDFS, XSD 

DBO = Namespace("http://sdm_upc.org/ontology/")
g = Graph()

#USERS
g.add((DBO.user_name, RDFS.domain, DBO.User))
g.add((DBO.user_name, RDFS.range, XSD.string))

g.add((DBO.user_country, RDFS.domain, DBO.User))
g.add((DBO.user_country, RDFS.range, DBO.Country))

g.add((DBO.likes_movie, RDFS.domain, DBO.User))
g.add((DBO.likes_movie, RDFS.range, DBO.Movie))

g.add((DBO.likes_genre, RDFS.domain, DBO.User))
g.add((DBO.likes_genre, RDFS.range, DBO.Genre))

g.add((DBO.likes_team, RDFS.domain, DBO.User))
g.add((DBO.likes_team, RDFS.range, DBO.Team))

g.add((DBO.likes_competition, RDFS.domain, DBO.User))
g.add((DBO.likes_competition, RDFS.range, DBO.Competition))

g.add((DBO.interested_in, RDFS.domain, DBO.User))
g.add((DBO.interested_in, RDFS.range, DBO.Keyword))

#COUNTRY

g.add((DBO.country_name, RDFS.domain, DBO.Country))
g.add((DBO.country_name, RDFS.range, XSD.string))


#SPORTS
## Matches

g.add((DBO.Match, RDFS.subClassOf, DBO.Sports))

g.add((DBO.match_home_team, RDFS.domain, DBO.Match))
g.add((DBO.match_home_team, RDFS.range, DBO.Team))

g.add((DBO.match_away_team, RDFS.domain, DBO.Match))
g.add((DBO.match_away_team, RDFS.range, DBO.Team))

g.add((DBO.match_home_goals, RDFS.domain, DBO.Match))
g.add((DBO.match_home_goals, RDFS.range, XSD.integer))

g.add((DBO.match_away_goals, RDFS.domain, DBO.Match))
g.add((DBO.match_away_goals, RDFS.range, XSD.integer))

g.add((DBO.match_date, RDFS.domain, DBO.Match))
g.add((DBO.match_date, RDFS.range, XSD.dateTime))

g.add((DBO.match_status, RDFS.domain, DBO.Match))
g.add((DBO.match_status, RDFS.range, XSD.string))

g.add((DBO.match_competition, RDFS.domain, DBO.Match))
g.add((DBO.match_competition, RDFS.range, DBO.Competition))

g.add((DBO.match_referee, RDFS.domain, DBO.Match))
g.add((DBO.match_referee, RDFS.range, DBO.Referee))

g.add((DBO.match_venue, RDFS.domain, DBO.Match))
g.add((DBO.match_venue, RDFS.range, DBO.Venue))


## Competitions
g.add((DBO.Competition, RDFS.subClassOf, DBO.Sports))

g.add((DBO.competition_country, RDFS.domain, DBO.Competition))
g.add((DBO.competition_country, RDFS.range, DBO.Country))

g.add((DBO.competition_name, RDFS.domain, DBO.Competition))
g.add((DBO.competition_name, RDFS.range, XSD.string))

g.add((DBO.League, RDFS.subClassOf, DBO.Competition))
g.add((DBO.Cup, RDFS.subClassOf, DBO.Competition))

## Teams
g.add((DBO.Team, RDFS.subClassOf, DBO.Sports))

g.add((DBO.team_name, RDFS.domain, DBO.Team))
g.add((DBO.team_name, RDFS.range,XSD.string ))

## Referees
g.add((DBO.Referees, RDFS.subClassOf, DBO.Sports))

g.add((DBO.referee_name, RDFS.domain, DBO.Referee))
g.add((DBO.referee_name, RDFS.range, XSD.string ))

## Venues
g.add((DBO.Venue, RDFS.subClassOf, DBO.Sports))

g.add((DBO.venue_name, RDFS.domain, DBO.Venue))
g.add((DBO.venue_name, RDFS.range,XSD.string ))

g.add((DBO.venue_city, RDFS.domain, DBO.Venue))
g.add((DBO.venue_city, RDFS.range,XSD.string ))


#ENTERTAINMENT
## Movies
g.add((DBO.Movie, RDFS.subClassOf, DBO.Entertainment))

g.add((DBO.movie_title, RDFS.domain, DBO.Movie))
g.add((DBO.movie_title, RDFS.range, XSD.string))

g.add((DBO.movie_language, RDFS.domain, DBO.Movie))
g.add((DBO.movie_language, RDFS.range, XSD.string))

g.add((DBO.movie_release_date, RDFS.domain, DBO.Movie))
g.add((DBO.movie_release_date, RDFS.range, XSD.date))

g.add((DBO.movie_revenue, RDFS.domain, DBO.Movie))
g.add((DBO.movie_revenue, RDFS.range, XSD.integer))

g.add((DBO.movie_budget, RDFS.domain, DBO.Movie))
g.add((DBO.movie_budget, RDFS.range, XSD.integer))

g.add((DBO.movie_runtime, RDFS.domain, DBO.Movie))
g.add((DBO.movie_runtime, RDFS.range, XSD.integer))

g.add((DBO.movie_adult, RDFS.domain, DBO.Movie))
g.add((DBO.movie_adult, RDFS.range, XSD.boolean))

g.add((DBO.movie_popularity, RDFS.domain, DBO.Movie))
g.add((DBO.movie_popularity, RDFS.range, XSD.long))

g.add((DBO.movie_vote_avg, RDFS.domain, DBO.Movie))
g.add((DBO.movie_vote_avg, RDFS.range, XSD.long))

g.add((DBO.movie_vote_cnt, RDFS.domain, DBO.Movie))
g.add((DBO.movie_vote_cnt, RDFS.range, XSD.integer))

g.add((DBO.upcoming_movie, RDFS.domain, DBO.Movie))
g.add((DBO.upcoming_movie, RDFS.range, XSD.boolean))

g.add((DBO.trending_movie, RDFS.domain, DBO.Movie))
g.add((DBO.trending_movie, RDFS.range, XSD.boolean))

g.add((DBO.now_playing_movie, RDFS.domain, DBO.Movie))
g.add((DBO.now_playing_movie, RDFS.range, XSD.boolean))

g.add((DBO.has_genre, RDFS.range, DBO.Movie))
g.add((DBO.has_genre, RDFS.domain, DBO.Genre))

## Genres
g.add((DBO.Genre, RDFS.subClassOf, DBO.Entertainment))

g.add((DBO.genre_name, RDFS.domain, DBO.Genre))
g.add((DBO.genre_name, RDFS.range, XSD.string))

#NEWS
g.add((DBO.written_by, RDFS.domain, DBO.News))
g.add((DBO.written_by, RDFS.range, DBO.Author))

g.add((DBO.author_name, RDFS.domain, DBO.Author))
g.add((DBO.author_name, RDFS.range, XSD.string))

g.add((DBO.related_keyword, RDFS.domain, DBO.News))
g.add((DBO.related_keyword, RDFS.range, DBO.Keyword))

g.add((DBO.published_at, RDFS.domain, DBO.News))
g.add((DBO.published_at, RDFS.range, DBO.Source))

g.add((DBO.source_name, RDFS.domain, DBO.Source))
g.add((DBO.source_name, RDFS.range, XSD.string))

g.add((DBO.news_title, RDFS.domain, DBO.News))
g.add((DBO.news_title, RDFS.range, XSD.string))

g.add((DBO.news_date, RDFS.domain, DBO.News))
g.add((DBO.news_date, RDFS.range, XSD.dateTime))


g.add((DBO.Entertainment_News, RDFS.subClassOf, DBO.News))
g.add((DBO.Entertainment_News, RDFS.subClassOf, DBO.Entertainment))
g.add((DBO.Sports_News, RDFS.subClassOf, DBO.News))
g.add((DBO.Sports_News, RDFS.subClassOf, DBO.Sports))
g.add((DBO.Tech_News, RDFS.subClassOf, DBO.News))
g.add((DBO.Tech_News, RDFS.subClassOf, DBO.Technology))

# KEYWORDS
g.add((DBO.keyword_text, RDFS.domain, DBO.Keyword))
g.add((DBO.keyword_text, RDFS.range, XSD.string))



g.add((DBO.User, RDFS.subClassOf, FOAF.Person))
g.add((DBO.Referee, RDFS.subClassOf, FOAF.Person))
g.add((DBO.Author, RDFS.subClassOf, FOAF.Person))
g.add((DBO.user_name, RDFS.subPropertyOf, FOAF.name))
g.add((DBO.author_name, RDFS.subPropertyOf, FOAF.name))
g.add((DBO.referee_name, RDFS.subPropertyOf, FOAF.name))



g.serialize("../docker-import/tbox.ttl", format="turtle")



