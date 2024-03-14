import requests
import csv
from google.cloud import storage

class extractBatsmenRanking():
    def __init__(self) -> None:
        self.url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"
        self.querystring = {"formatType":"odi"}
        self.headers = {
            "X-RapidAPI-Key": "e3f4345048msh0c98a793249d42bp1e71c4jsn9ebda9676726",
            "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
        }

    def extract_stats(self) -> None:
        response = requests.get(self.url, headers=self.headers, params=self.querystring)

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
                self.push_to_gcs(csv_filename)
            else:
                print("No data available from API!")

        else:
            print(f"Failed to fetch data from API! Status code: {response.status_code}")

    def push_to_gcs(self, csv_filename):
        bucket_name = 'cricket-statistics-batsmen-rankings'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")

if __name__ =="__main__":
    extractBatsmenRanking().extract_stats()