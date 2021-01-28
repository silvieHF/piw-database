import requests
from xml.etree import ElementTree
import time
from sys import argv
import os
import psycopg2

# groel

# SELECT avg(t1.length) FROM (SELECT length(replace(substring(fasta, strpos(fasta, E'\n')), E'\n', '')) FROM fastas_done WHERE name = 'groel') t1;

# SELECT count(*) FROM (SELECT DISTINCT regexp_replace(regexp_replace(fasta, '.*\[', ''), '\].*', '') AS name FROM fastas_done WHERE name = 'groes' AND strpos(fasta, '[') <> 0 AND strpos(fasta, ']') <> 0) t1;

def ncbi_search_ids(query, offset, limit, api_key = None):
	if offset < 0:
		raise Exception("offset must be >= 0")
		
	if limit < 0 or limit > 100000:
		raise Exception('limit must be >= 0 and <= 100000')
		
	params = {'db': 'protein', 'term': query + ' AND (bacteria[filter]) AND (("90"[SLEN] : "200"[SLEN]))', 'retmax': limit, 'retstart': offset}
	
	if api_key != None:
		params['api_key'] = api_key
	
	response = requests.get('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi', params = params)
	
	if response.status_code != requests.codes.ok:
		raise Exception('Response code is {}'.format(response.status_code))
	
	root = ElementTree.fromstring(response.content)
	return_list = []
	
	for id in root.iter('Id'):
		return_list.append(id.text)
		
	return return_list
	
def ncbi_search_all_ids(query, api_key = None):
	ids = []	
	limit = 100000
	offset = 0

	while True:
		print("Retrieving [{}; {}] for {}".format(offset, offset + limit, query))
		data = ncbi_search_ids(query, offset, limit, api_key)
		if len(data) == 0:
			break
	
		ids.extend(data)
		offset += limit
			
	return ids

def ncbi_fetch_fasta(ids, api_key = None):
	params = {'db': 'protein', 'rettype': 'fasta'}
	
	if api_key != None:
		params['api_key'] = api_key
	
	data = {'id': ids}
	response = requests.post('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi', params = params, data = data)
		
	if response.status_code != requests.codes.ok:
		raise Exception('Response code is {}'.format(response.status_code))
	
	return response.content.decode("utf8")

def uniprot_fetch_all_fasta(query):
	params = {'query': query, 'format': 'fasta'}
	response = requests.get('https://www.uniprot.org/uniprot/', params = params)
	
	if response.status_code != requests.codes.ok:
		raise Exception('Response code is {}'.format(response.status_code))
	
	return response.content.decode("utf8")

def update(query, connection, api_key = None):
	cursor = connection.cursor()
	
	cursor.execute("DELETE FROM fastas_todo WHERE name = %s AND source_type = 'ncbi'", (query,))
	connection.commit()
	
	values = map(lambda x: (query, x), ncbi_search_all_ids(query, api_key))
	cursor.executemany("INSERT INTO fastas_todo VALUES(%s, 'ncbi', %s)", values)
	connection.commit()
	
def upgrade_ncbi(query, connection, api_key = None):
	try:
		cursor = connection.cursor()
		chunk_size = 500
		
		cursor.execute("SELECT source_id FROM fastas_todo WHERE name = %s AND source_type = 'ncbi'", (query,))
		ids = cursor.fetchall()
		
		for i in range(0, len(ids), chunk_size):	
			sub_ids = list(map(lambda x: x[0], ids[i:i + chunk_size]))	
			fastas = []
			attempt = 0
					
			print('Downloading {} {} records from ncbi ({}/{}, {}%)'.format(len(sub_ids), query, i, len(ids), i * 100.0 / len(ids)))
			start_time = time.time()
			while True:
				try:
					down_start = time.time()
					fastas = filter(lambda x: len(x), ncbi_fetch_fasta(sub_ids, api_key).split(">"))
					fastas = map(lambda x: '>' + x, fastas)
					down_end = time.time()
					break
				except Exception as e:
					attempt = attempt + 1
					print('Received exception! Waiting {}s...'.format(attempt * 60))
					time.sleep(attempt * 60)
					print('Retrying...')
			end_time = time.time()	
			print('Finished in {}s!'.format(end_time - start_time))
			
			fastas = list(fastas)
			
			if len(sub_ids) != len(fastas):
				raise Exception('Length mismatch')

			delete_values = list(map(lambda x: (query, x), sub_ids))
			insert_values = list(map(lambda x: (query, *x), zip(sub_ids, fastas)))
						
			print('Inserting records into local database...'.format(len(fastas)))
			start_time = time.time()
			cursor.executemany("DELETE FROM fastas_done WHERE name = %s AND source_type = 'ncbi' AND source_id = %s", delete_values)
			cursor.executemany("DELETE FROM fastas_todo WHERE name = %s AND source_type = 'ncbi' AND source_id = %s", delete_values)
			cursor.executemany("INSERT INTO fastas_done (name, source_type, source_id, fasta) VALUES(%s, 'ncbi', %s, %s)", insert_values)
			connection.commit()
			end_time = time.time()
			print('Finished in {}s!'.format(end_time - start_time))
			
	except Exception as e:
		connection.rollback()
		raise e
		
def upgrade_uniprot(query, connection):
	print('Downloading all {} records from uniprot...'.format(query))
	start_time = time.time()
	fastas = uniprot_fetch_all_fasta(query)
	end_time = time.time()	
	print('Finished in {}s!'.format(end_time - start_time))
	
	fastas = filter(lambda x: len(x), fastas.split(">"))
	values = list(map(lambda x: (query, ">" + x), fastas))
	
	cursor = connection.cursor()
	print('Inserting {} records into local database...'.format(len(values)))
	start_time = time.time()
	cursor.execute("DELETE FROM fastas_done WHERE name = %s AND source_type = 'uniprot'", (query,))
	cursor.executemany("INSERT INTO fastas_done (name, source_type, source_id, fasta) VALUES(%s, 'uniprot', NULL, %s)", values)
	connection.commit()
	end_time = time.time()
	print('Finished in {}s!'.format(end_time - start_time))

connection = psycopg2.connect(database='fasta', user='fasta', password='fasta')

api_key = os.environ.get('API_KEY')

if len(argv) < 2:
	argv = [argv[0], 'help']

if argv[1] == 'update':
	query = ' '.join(argv[2:])
	update(query, connection, api_key)
elif argv[1] == 'upgrade_uniprot':
	query = ' '.join(argv[2:])
	upgrade_uniprot(query, connection)
elif argv[1] == 'upgrade_ncbi':
	query = ' '.join(argv[2:])
	upgrade_ncbi(query, connection, api_key)
else:
	print('subcommand not found')
