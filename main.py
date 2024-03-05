import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import time

username = 'juankisuelamartin'
token = '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'
client_id = 'e7983846c4408e5ad01c'
client_secret = '6d4302cf6d2eeeb2644551b2d579db110f5ccfff'
headers = {'Authorization': 'Bearer '+token, 'Accept': 'application/vnd.github+json'}

uri = "mongodb+srv://admin:ynxfWVMa1xYyNShY@gestiondedatos.phpxq3k.mongodb.net/?retryWrites=true&w=majority&appName=GestiondeDatos"

# Create a new client and connect to the server
connection = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    connection.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Conexión a MongoDB
collCommits = connection["GestiondeDatos"]["Commits"]

repos_url = 'https://api.github.com/repos/{}/{}/commits?page={}&per_page={}'
commit_url = 'https://api.github.com/repos/{}/{}/commits/{}'
user = 'sourcegraph'
project = 'sourcegraph'

# Fecha de inicio
since_date = datetime(2019, 1, 1)

# Obtener el último commit SHA procesado
last_commit_record = collCommits.find_one({}, sort=[('_id', -1)])
last_commit_sha = last_commit_record['sha'] if last_commit_record else None

# Formato de fecha para la API de GitHub
since_str = since_date.strftime('%Y-%m-%dT%H:%M:%SZ')

page = 1
while True:
    url = repos_url.format(user, project, page, 100)
    query = {'client_id': client_id, 'client_secret': client_secret, 'since': since_str}
    r = requests.get(url, params=query, headers=headers)
    if r.status_code == 403:
        print("Rate limit exceeded. Sleeping for a minute...")
        time.sleep(120)
        continue
    commits_dict = r.json()
    if not commits_dict:  # Si la lista está vacía, hemos llegado al final
        break
    for commit in commits_dict:
        commit['projectId'] = project
        print(str(commit))
        # Obtener información adicional del commit
        commit_sha = commit['sha']
        if commit_sha == last_commit_sha:
            # Si encontramos el último commit procesado, terminamos
            break
        commit_info_url = commit_url.format(user, project, commit_sha)
        r = requests.get(commit_info_url, headers=headers)
        commit_info = r.json()
        if 'files' in commit_info:
            commit['files'] = commit_info['files']
        else:
            commit['files'] = []
        if 'stats' in commit_info:
            commit['stats'] = commit_info['stats']
        else:
            commit['stats'] = []
        collCommits.insert_one(commit)
    else:
        # Si no se encontró el último commit procesado, continuamos con la siguiente página
        page += 1
        continue
    break  # Terminamos el bucle si encontramos el último commit procesado
