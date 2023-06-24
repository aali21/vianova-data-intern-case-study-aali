# Case study


We want to know the __countries that don't host a megapolis__

This program fetches the [dataset of the population of all cities in the world](https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/export/?disjunctive.cou_name_en), stores it in a SQLite database, then performs a query that computes the countries that don't host a megapoliss (a city of more than 10,000,000 inhabitants)? 

The program saves the result (country code and country name) as a tabulated separated value file, ordered by country name. 

We imagine that the program will be run automatically every week to update the resulting data. All thats required now is to set up a cron job (or task scheduler on Windows) which will run the .py file at specified days and times.


