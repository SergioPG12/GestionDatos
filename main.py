import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

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
user = 'brave'
project = 'brave-core'
url = repos_url.format(user, project, 1, 1)
query = {'client_id': client_id, 'client_secret': client_secret}
r = requests.get(url, params=query, headers=headers)
commits_dict = r.json()

for commit in commits_dict:
    commit['projectId'] = project
    print(str(commit))
    collCommits.insert_one(commit)
