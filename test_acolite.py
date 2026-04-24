import requests
r = requests.get("https://api.github.com/repos/acolite/acolite/tags", timeout=10)
for tag in r.json()[:5]:
    print(tag["name"])
