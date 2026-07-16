import urllib.request
import urllib.parse
import json
import os
import time

def escape_o(o):
    o_clean = o.replace('"', '\\"')
    return f'"{o_clean}"@it'

output_file = 'data/external/teseo_full.ttl'
os.makedirs(os.path.dirname(output_file), exist_ok=True)

limit = 10000
offset = 0

print("Downloading TESEO concepts and labels using paginated SELECT...")

with open(output_file, 'w', encoding='utf-8') as f:
    f.write("@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n")
    f.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\n")
    
    while True:
        sparql = f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?concept ?label ?altLabel
        WHERE {{
          ?concept a skos:Concept .
          ?concept skos:prefLabel ?label .
          OPTIONAL {{ ?concept skos:altLabel ?altLabel }}
          FILTER(STRSTARTS(STR(?concept), "http://dati.senato.it/teseo/"))
        }}
        LIMIT {limit} OFFSET {offset}
        """
        
        url = 'http://dati.senato.it/sparql?query=' + urllib.parse.quote(sparql) + '&format=json'
        req = urllib.request.Request(url, headers={
            'Accept': 'application/sparql-results+json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print(f"Fetching LIMIT {limit} OFFSET {offset}...")
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode('utf-8'))
                bindings = data['results']['bindings']
                
                if not bindings:
                    print("No more results. Done.")
                    break
                
                for b in bindings:
                    s = f"<{b['concept']['value']}>"
                    f.write(f"{s} rdf:type skos:Concept .\n")
                    o = escape_o(b['label']['value'])
                    f.write(f"{s} skos:prefLabel {o} .\n")
                    if 'altLabel' in b:
                        o_alt = escape_o(b['altLabel']['value'])
                        f.write(f"{s} skos:altLabel {o_alt} .\n")
                
                offset += limit
                time.sleep(1) # Be nice to the server
                
        except Exception as e:
            print('Error:', e)
            print("Retrying with smaller limit in 5s...")
            limit = int(limit / 2)
            if limit < 100:
                print("Limit too small, giving up.")
                break
            time.sleep(5)
