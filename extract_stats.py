import requests
import csv

url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

querystring = {"formatType":"odi"}

headers = {
	"X-RapidAPI-Key": "e3f4345048msh0c98a793249d42bp1e71c4jsn9ebda9676726",
	"X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)


if response.status_code == 200:
    data = response.json().get('rank', [])
    csv_filename = 'batsmen_odi_rankings.csv'

    if data:
        attributes = ['rank', 'name', 'country']

        with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=attributes)
            # writer.writeheader()
            for record in data:
                writer.writerow({attribute: record.get(attribute) for attribute in attributes})
        
        print(f"Data written to {csv_filename}")
    else:
        print("No data available from API!")

else:
    print(f"Failed to fetch data from API! Status code: {response.status_code}")