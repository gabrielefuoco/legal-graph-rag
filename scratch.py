import urllib.request
import urllib.parse
import json

sparql = """
CONSTRUCT { ?s ?p ?o }
WHERE {
  ?s ?p ?o .
  FILTER(STRSTARTS(STR(?s), "http://dati.senato.it/teseo/"))
} LIMIT 10
"""

url = 'http://dati.senato.it/sparql?query=' + urllib.parse.quote(sparql)
req = urllib.request.Request(url, headers={
    'Accept': 'text/turtle',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
})

try:
    with urllib.request.urlopen(req) as response:
        print('Status:', response.status)
        data = response.read().decode('utf-8')
        print('Data sample:', data[:500])
except Exception as e:
    print('Error:', e)
