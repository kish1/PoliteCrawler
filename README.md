# PoliteCrawler
A polite web-crawler with a specific frontier management strategy.

# Politeness Policy
Before the first page from a given domain is crawled, its robots.txt file is fetched and it is made sure that the crawer only requests pages that it is allowed to access. 

Between two successive GET requests to the same domain, we make a HEAD request.

# Frontier Management Strategy
Seed URLs should always be crawled first.

Prefer pages with higher in-link counts.

If multiple pages have maximal in-link counts, choose the option which has been in the queue the longest.
