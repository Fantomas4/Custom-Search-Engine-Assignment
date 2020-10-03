# Custom Search Engine Assignment
This is a project I was assigned for the "Information Retrieval" course of my university. The objective of this exercise is to create a simple, custom search engine that maintains the functionality of the more complex, large scale search engines found in the market.

## Installation ##
1) pip3 install -r requirements.txt  
2) docker-compose up 

## Execution ##
1) Begin by executing the crawler:
   python3 crawler.py [starting_url] [pages_to_crawl] [append_pages] [number_of_threads]
   
   Where:
   - starting_url: The page url from which the crawler will begin the crawling process.
   - pages_to_crawl: The number of pages we want to be scanned by the crawler
   - append_pages: Determines whether any page data collected from previous crawls should be deleted
     (append_pages=0) or new page data from the current crawl should be appended to any existing page data
     (append_pages=1).
   - number_of_threads: The number of threads that should be used during the crawler's execution.

   Example:
   python3 myCrawler.py https://en.wikipedia.org/wiki/Apage 100 0 8 
    
    After the crawling is completed, the crawler will automatically call the indexer to start the 
    index building process.
    
2) Execute the Flask Server in order to access the Search Engine User Interface in localhost:
   python3 app.py [number_of_threads]
   
   Where number_of_threads is the number of threads that should be used during the query handler's
   execution (query handler is initialized inside app.py).
   
   Example:
   python3 app.py 8
   
### Tip: ###
For the purpose of visualizing MongoDB and easily inspecting its contents, 
try using MongoDB Compass.
