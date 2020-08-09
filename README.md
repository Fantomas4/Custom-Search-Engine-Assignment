# Crawler #
This project is a simple crawler, that runs in parallel. All data are saved in a mongodb from which further analysis can be made.

## Installation ##
pip3 install -r requirements.txt  
docker-compose up -d 

## Execution ##
python3 myCrawler.py startingUrl pages_to_crawl append_pages number_of_threads

example:\
python3 myCrawler.py https://en.wikipedia.org/wiki/Apage 100 0 8 

### Tip: ###
For better visualize mongodb, download mongo compass