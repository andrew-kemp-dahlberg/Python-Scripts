#!/usr/bin/env python3
import requests
import csv
import json
import os
import time
import zipfile
import shutil

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
    '''downloads repos as zip files and extracts them to output directory in'''
    for repo in repo_list:
        repo_name = repo['name']
        owner = repo['owner']['login']
        default_branch = repo['default_branch']
        url = f"https://github.com/{owner}/{repo_name}/archive/refs/heads/{default_branch}.zip"
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            zip_path = os.path.join(output_directory, f"{repo_name}.zip")
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(output_directory)
            os.remove(zip_path)
            print(f"Downloaded and extracted {repo_name}")
        else:
            print(f"Failed to download {repo_name}: HTTP {response.status_code}")

            # Check rate limit headers
            rate_limit_remaining = int(response.headers.get('x-rate-limit-remaining', 1))
            if rate_limit_remaining <= 1:
                current_time = int(time.time())
                reset_time = int(response.headers.get('x-rate-limit-reset', current_time + 60))
                sleep_duration = reset_time - current_time + 1
                sleep_duration = max(sleep_duration, 0)
                print(f"Rate limit approaching. Sleeping {sleep_duration} seconds")
                time.sleep(sleep_duration)








def main():

    '''Setup input and output files and create directories'''
    input_csv = input("Enter the path to the input CSV file (Requires Column \"Application\"): ")
    input_csv =  os.path.abspath(os.path.expanduser(input_csv))
    app_import = read_csv(input_csv)
    output_csv_location = input_csv.replace(".csv", "-with-autopkg-recipe.csv")
    output_directory_location = os.makedirs(os.path.join(output_csv_location.split("/")[0:-1],"output-recipes"), exist_ok=True) 
    
    
    '''chooses whether or not use existing downloaded repos and cleans up if you want a refresh '''
    if os.path.exists(all_recipes_directory):
        use_local_repos = input("Do you want to use existing autopkg repos type y or n: ")
    repo_list = fetch_repos()
    if use_local_repos.lower() == 'n' or not os.path.exists(all_recipes_directory):
        if use_local_repos.lower() and os.path.exists(all_recipes_directory)  :
            shutil.rmtree(all_recipes_directory)

        download_repos(repo_list, all_recipes_directory)
    all_recipes_directory = os.makedirs("/usr/local/tmp/autopkg-recipes", exist_ok=True)


if __name__ == "__main__":
    main()