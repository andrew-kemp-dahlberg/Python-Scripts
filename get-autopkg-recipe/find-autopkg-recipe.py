#!/usr/bin/env python3
import requests
import csv
import json
import os
import time
import zipfile

def read_csv(import_file):
    """Read CSV file and return list of dictionaries."""
    with open(import_file, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def fetch_repos(endpoint):
        """Download Autopkg repo and parse metadata."""
        url = f"https://api.github.com/users/autopkg/repos"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)
        status = response.raise_for_status()
        repos = []
        while url:
            print(f"Fetching: {url}")
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                
                rate_limit_remaining = int(response.headers.get('x-rate-limit-remaining', 1))
                if rate_limit_remaining <= 1:
                    current_time = int(time.time())
                    reset_time = int(response.headers.get('x-rate-limit-reset', current_time + 60))
                    sleep_duration = reset_time - current_time + 1
                    sleep_duration = max(sleep_duration, 0)
                    print(f"Rate limit approaching. Sleeping {sleep_duration} seconds")
                    time.sleep(sleep_duration)
                repos.extend(response.json())
                
                link_header = response.headers.get('link', '')
                url = None
                for link in link_header.split(','):
                    if 'rel="next"' in link:
                        url = link.split(';')[0].strip(' <>')
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching repos: {e}")
                break
                
        return repos

def download_repos(repo_list, output_directory):
    for repo in repo_list:
        repo_name = repo['name']
        owner = repo['owner']['login']
        default_branch = repo['default_branch']
        url = f"https://github.com/{owner}/{repo_name}/archive/refs/heads/{default_branch}.zip"







def main():

    '''Setup input and output files and create directories'''
    input_csv = input("Enter the path to the input CSV file (Requires Column \"Application\"): ")
    input_csv =  os.path.abspath(os.path.expanduser(input_csv))
    app_import = read_csv(input_csv)
    output_csv_location = input_csv.replace(".csv", "-with-autopkg-recipe.csv")
    output_directory_location = os.makedirs(os.path.join(output_csv_location.split("/")[0:-1],"output-recipes"), exist_ok=True) 
    all_recipes_directory = os.makedirs("/usr/local/tmp/autopkg-recipes", exist_ok=True)


    '''download repos and build metadata'''
    repo_list = fetch_repos()
    download_repos(repo_list, all_recipes_directory)
    repo_metadata = 

    


if __name__ == "__main__":
    main()