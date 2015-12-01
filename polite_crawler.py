import urlparse
import urllib3
import requests
import time
import heapq
from bs4 import BeautifulSoup
import robotparser
import os

CRLF = '\r\n'
FIELD_END = '#'
ENTRY_END = '$'

# It would be more natural to use a Max-Heap in this program.
# Since I already had my old code for a Min-Heap, I employed that by negating the values stored in it.

# min-heap
class min_heap():
	# min-heap : Void -> min-heap
	# Returns: An empty min-heap object.
	def __init__(self):
		self.heap = []

	# insert : PosInt -> Void
	# Effect: Adds the given value into the min-heap.
	def insert(self, value):
		heapq.heappush(self.heap, value)

	# pop : Void -> PosInt
	# Returns: The root of the min-heap, ie the minimum value.
	def pop(self):
		if self.heap == []:
			return None
		return heapq.heappop(self.heap)

	# view_top : Void -> PosInt
	# Returns: The root of the min-heap without removing it from the min-heap.
	def view_top(self):
		if self.heap == []:
			return None
		return self.heap[0]

	# heapify : Void -> Void
	# Effect : Readjusts the heap by applying the min-heap property
	def heapify(self):
		heapq.heapify(self.heap)

	# size : Void -> Integer
	# Returns : The number of elements in the heap.
	def size(self):
		return len(self.heap)


class queue_element():
	def __init__(self, url, inlinks):
		self.url = url
		self.inlinks = -inlinks
		self.timestamp = time.time()

	def increase_inlinks(self, delta):
		self.inlinks -= delta

	def __cmp__(self, other):
		if self.inlinks == other.inlinks:
			if self.timestamp < other.timestamp:
				return -1
			return 1
		return self.inlinks - other.inlinks

def text_out_links(html, base):
	soup = BeautifulSoup(html)
	if soup.title != None:
		text = soup.title.text
	else:
		text = ''
	text += ''.join(map(lambda x: x.text.strip(), soup.find_all('p')))
	return (text, map(lambda x: canonicalize(x['href'], base), filter(lambda x: x.has_attr('href') and x.text != '', soup.find_all('a'))))


def canonicalize(url, base):
	if url.endswith('/'):
		url = url[:-1]
	parsed = urlparse.urlparse(url)
	if (parsed[1] == '' and parsed[2] != '' and not (parsed[2].startswith('/') or parsed[2].startswith('.'))) or (parsed[0] == '' and parsed[1] != ''):
		for i in range(len(url)):
			if url[i].isalnum():
				break
		url = 'http://' + url[i:]
		parsed = urlparse.urlparse(url)
	if ':' in parsed.netloc:
		if (parsed.scheme == 'http' and parsed.netloc.split(':')[1] == '80') or (parsed.scheme == 'https' and parsed.netloc.split(':')[1] == '443'):
			parsed = (parsed.scheme, parsed.netloc.lower().split(':')[0], parsed.path, parsed.params, parsed.query, '')
	else:
		parsed = (parsed.scheme, parsed.netloc.lower(), parsed.path, parsed.params, parsed.query, '')
	#print(parsed)
	return urlparse.urljoin(base, urlparse.urlunparse(parsed))


def fetch(session, url, again=True):
	try:
		session.head(url)
		r = session.get(url, headers={'accept':'text/html'})
		#print(r.status_code)
		if r.status_code == 200:
			html = r.text
			ct = r.headers.get('content-type')
			content_type = None
			charset = 'utf-8'
			#charset = 'iso-8859-1'

			if ct != None:
					ct_header = ct.split(';')
					content_type = ct_header[0]
					#if len(ct_header) > 1 and ct_header[1].strip().startswith('charset='):
					#	charset = ct_header[1].split('=')[1]

			soup = BeautifulSoup(html)
			if (soup.html.get('lang') == None or soup.html['lang'] == 'en') and content_type == 'text/html':
				return (html, charset, True)
	except requests.exceptions.ConnectionError as e:
		print(e)
		if again:
			return fetch(session, url, False)
	except:
		#print(e)
		print(url)

	return ('', None, False)

def store(results_filename, charset, url, text, html, outlinks):
	fp = open(results_filename, 'w+')
	fp.write(url + '\n' + FIELD_END + '\n')
	fp.write(text.encode(charset, 'ignore') + '\n' + FIELD_END + '\n')
	fp.write(html.strip().encode(charset, 'ignore') + '\n' + FIELD_END + '\n')
	fp.write(','.join(outlinks).encode(charset, 'ignore'))

	fp.close()

def polite(robotcheckers, url):
	host = urlparse.urlparse(url).netloc
	try:
		rc = robotcheckers[host]
	except KeyError:
		rc = robotparser.RobotFileParser()
		rc.set_url('http://' + host + '/robots.txt')
		rc.read()
		robotcheckers[host] = rc
	return rc.can_fetch('*', url)

def hours_minutes(seconds):
	return str(seconds/3600) + ' hours, ' + str((seconds%3600)/60) + ' mins elapsed.'

# best-first policy priorities:
# 1. seed URLs
# 2. higher number of in-links
# 3. longest time spent in the queues
def crawl(CRAWL_LIMIT, results_filepath, seeds):
	heap = min_heap()
	visited = set()
	queue = {}
	robotcheckers = {}
	crawled = 1
	start_time = time.time()
	time_taken = start_time
	string = ''
	session = requests.Session()
	#print('hi')

	for seed in seeds:
		if not polite(robotcheckers, seed):
			#print('impolite' + seed)
			continue

		html, charset, ok = fetch(session, seed)

		if not ok:
			#print('not okay')
			continue
		text, outlinks =  text_out_links(html, seed)

		for link in outlinks:
			if link not in visited:
				try:
					queue[link].increase_inlinks(1)
				except KeyError:
					new_element = queue_element(link, 1)
					queue[link] = new_element
					heap.insert(new_element)
		store(os.path.join(results_filepath,str(crawled)+'.txt'), charset, link, text, html, outlinks)
		visited.add(seed)
		crawled += 1

	while(crawled < CRAWL_LIMIT):
		next_element = heap.pop()
		next_link = next_element.url
		queue.pop(next_link)

		if not polite(robotcheckers, next_link):
			continue

		html, charset, ok = fetch(session, next_link)

		if not ok:
			continue
		text, outlinks =  text_out_links(html, next_link)

		for link in outlinks:
			if link not in visited:
				try:
					queue[link].increase_inlinks(1)
				except KeyError:
					new_element = queue_element(link, 1)
					queue[link] = new_element
					heap.insert(new_element)
		store(os.path.join(results_filepath,str(crawled)+'.txt'), charset, next_link, text, html, outlinks)
		visited.add(next_link)
		crawled += 1
		heap.heapify()

		if crawled % 100 == 0:
				print('Last batch took ' + str(int(time.time() - time_taken)) + ' seconds')
				print(str(crawled) + ' pages crawled.')
				#print('Frontier Size: ' + str(len(queue)))
				print('Heap Size: ' + str(heap.size()))
				time_taken = time.time()
				print(hours_minutes(int(time_taken - start_time)) + '\n')

#	print(visited)
	print('Crawling complete')
	print(len(visited))
	#print(fetched)

def main():
	urllib3.disable_warnings()
	# replace the example websites given below with your seed URLs
	seeds = ('www.abc.com', 'www.pqr.com', 'www.xyz.com')
	results_filename = '~/Documents/IR/code/hw3/data/fresh_crawl'
	crawl(21000, results_filename, seeds)

	#print(pages)

def tests():
	b1 = 'http://www.google.com'
	b2 = 'http://www.google.com/a/b.txt'
	u1 = '../SomeFile.txt'
	u2 = 'http://www.Example.com/SomeFile.txt'
	u3 = 'www.example.com/SomeFile.txt'
	u4 = '//www.example.com/SomeFile.txt'
	u5 = '#skip'
	u6 = 'http://www.google.com/'
	r1 = 'http://www.google.com/SomeFile.txt'
	r2 = 'http://www.example.com/SomeFile.txt'
	cases = [
	(canonicalize(u1, b2), r1, '1'),
	(canonicalize(u2, b1), r2, '2'),
	(canonicalize(u3, b1), r2, '3'),
	(canonicalize(u4, b1), r2, '4'),
	(canonicalize(u5, b1), b1, '5'),
	(canonicalize(u6, b1), b1, '6'), ]

	def check(tup_3):
		return 'Expected: ' + tup_3[1] + '\nGot: ' + tup_3[0] + '\n' + tup_3[2]

	print('\n'.join(map(check, filter(lambda x: x[0] != x[1], cases))))

if __name__ == '__main__':
	main()
#	tests()
