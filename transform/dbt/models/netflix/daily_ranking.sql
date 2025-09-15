{{ config(
    materialized='external', 
    location='output/netflix_top_shows.csv', 
    format='csv'
 )}}

-- SQL GOES HERE
