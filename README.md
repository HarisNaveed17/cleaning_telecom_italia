# cleaning_telecom_italia
Scripts to preprocess, clean and merge different Milan data sets available as part of Telecom Italia's Big Data Challenge 2013.

**Playing with Telecom Italia data sets**

In 2013, Telecom Italia released a wealth of different data sets as part of its Big Data Challenge. The following are some of the scripts I wrote to combine the raw
CDRs (Call Records), precipitation records, tweets, interactions between two regions and volume of calls made between different provinces. 

The scripts were written for the Milan sets but can be easily adapted for Trentino. 

For more information, read the paper released with the data set. The raw data can be downloaded from Harvard Dataverse. 
The paper can be found here: https://www.nature.com/articles/sdata201555

The purpose of each file is detailed under:

cleaning_txt.py:

Contains functions that produce comma separated values (.csv) files from the raw text files. They also allow you to specify the time_range and the CellIDs for which you
want the data. 

preprocessing.py:

Take the generated csv files and combine them. Specifically contains functions that label the tweets based on their location and map your CellIDs from the CDR grid to the weather grid. 
Also let you merge all the different data sets into one file. 

social_pulse.py:

Uses Geopy along with the Bing Maps API to label the tweets stored in the raw geojson file. For some reason, the geojson file does not open with geopandas so I was forced to pull data from it manually. 

precip_cell_locate.py:

Basic script to find where your CellIDs lie in the weather grid. 


