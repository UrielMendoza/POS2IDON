import requests

polygon = "POLYGON((-87.15 20.50,-87.15 20.75,-86.95 20.75,-86.95 20.50,-87.15 20.50))"
odata_url = (
    "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    f"?$filter=Collection/Name eq 'SENTINEL-2'"
    f" and contains(Name,'MSIL1C')"
    f" and OData.CSC.Intersects(area=geography'SRID=4326;{polygon}')"
    f" and ContentDate/Start gt 2026-04-10T00:00:00.000Z"
    f" and ContentDate/Start lt 2026-04-11T00:00:00.000Z"
    f"&$orderby=ContentDate/Start desc&$top=10"
)
resp = requests.get(odata_url, timeout=30)
print("Status:", resp.status_code)
items = resp.json().get("value", [])
print(f"Found {len(items)} products:")
for item in items:
    print(" -", item.get("Name"))
