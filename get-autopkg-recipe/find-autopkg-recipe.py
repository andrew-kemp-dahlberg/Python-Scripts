#!/usr/bin/env python3
import requests
import csv
import json
import os
import time
import shutil
from subprocess import PIPE, STDOUT, CalledProcessError, run
from dotenv import load_dotenv

def read_csv(import_file):
    """Read CSV file and return list of dictionaries."""
    with open(import_file, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def fetch_repos(github_token):
        """Download Autopkg repo and parse metadata."""
        url = f"https://api.github.com/users/autopkg/repos"

        payload = {}
        headers = { 'Authorization': f'Bearer {github_token}'}

        response = requests.request("GET", url, headers=headers, data=payload)
        status = response.raise_for_status()
        repos = []
        while url:
            print(f"Fetching: {url}")
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                
                rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
                if rate_limit_remaining <= 1:
                    current_time = int(time.time())
                    reset_time = int(response.headers.get('X-RateLimit-Reset', current_time + 60))
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


def build_metadata(repo_list, recipe_path):
    """Build metadata for each repo and save to recipe_path."""
    metadata = []
    for repo in repo_list:
        repo_name = repo['name']
        owner = repo['owner']['login']
        default_branch = repo['default_branch']
        stars = repo['stargazers_count']

        metadata.append({
            "name": repo_name,
            "owner": owner,
            "default_branch": default_branch,
            "recipe_path": os.path.join(recipe_path, f"{repo_name}.recipe"),
            "stars": stars
        })
    return metadata


def _run_command(shell_cmd):
    result = run(shell_cmd, stdout=PIPE, stderr=PIPE, shell=True)
    
    # Decode outputs
    stdout = result.stdout.decode().strip() if result.stdout else ""
    stderr = result.stderr.decode().strip() if result.stderr else ""
    
    # If command failed, show details
    if result.returncode != 0:
        print(f"\nCommand failed: {shell_cmd}")
        print(f"Exit code: {result.returncode}")
        if stdout:
            print(f"STDOUT: {stdout}")
        if stderr:
            print(f"STDERR: {stderr}")
        raise CalledProcessError(result.returncode, shell_cmd, output=stdout, stderr=stderr)
    
    return result.returncode, stdout
def download_repos(repo_list):
    """Download all autopkg recipe repos to a local directory."""
    for repo in repo_list:
        repo_name = repo['name']
        _run_command(f"autopkg repo-add {repo_name}")

def parse_recipe_results(results):
    """Parse autopkg search results and extract recipe info"""
    recipes = []
    for line in results.split('\n'):
        if line.strip() and not line.startswith('Name') and not line.startswith('----'):
            # autopkg search output format: "Recipe-Name    Repo-Path    Recipe-Description"
            parts = line.split(None, 2)  # Split on whitespace, max 3 parts
            if len(parts) >= 2:
                recipe_name = parts[0]
                repo_path = parts[1]
                # Extract repo name from path like "com.github.autopkg.recipes"
                repo_name = repo_path.split('/')[-1] if '/' in repo_path else repo_path
                recipes.append({
                    'name': recipe_name,
                    'repo': repo_name,
                    'full_path': repo_path
                })
    return recipes


def get_recipe_dependencies(recipe_path):
    """Extract parent recipe dependencies from a recipe file."""
    dependencies = []
    try:
        # Read the recipe plist to find ParentRecipe
        with open(recipe_path, 'rb') as f:
            import plistlib
            plist = plistlib.load(f)
            parent = plist.get('ParentRecipe')
            if parent:
                dependencies.append(parent)
                # Could recursively check parent's dependencies too
    except Exception as e:
        print(f"Error reading recipe dependencies: {e}")
    return dependencies

def is_recipe_deprecated(recipe_path):
    """Check if a recipe is marked as deprecated."""
    try:
        with open(recipe_path, 'rb') as f:
            import plistlib
            plist = plistlib.load(f)
            # Check for DeprecationWarning or Deprecated keys
            if plist.get('DeprecationWarning') or plist.get('Deprecated'):
                return True
    except Exception:
        pass
    return False

def search_and_filter_recipes(app_name, recipe_type, recipe_dir, repo_stars):
    """
    Search for recipes of a specific type and filter out deprecated ones.
    
    Args:
        app_name: Name of the application to search for
        recipe_type: Type of recipe ('munki' or 'download')
        recipe_dir: Directory containing recipe repositories
        repo_stars: Dictionary mapping repo names to star counts
        
    Returns:
        List of non-deprecated recipes sorted by star count
    """
    # Create grep pattern from app name for multi-word apps
    app_words = app_name.split()
    grep_pattern = '.*'.join(app_words)  # "google chrome" -> "google.*chrome"
    
    # Search for recipes
    recipes = []
    try:
        debug_cmd = f'autopkg search {app_name}'
        _, raw_results = _run_command(debug_cmd)
        print(f"Debug search command: {debug_cmd}")
        print(f"Debug search results:\n{raw_results}")
        cmd = f'autopkg search {app_name} | grep -iE "{grep_pattern}" | grep -i "\\.{recipe_type}\\.recipe"'
        print(f"Running command: {cmd}")
        
        exit_code, results_str = _run_command(cmd)
        if results_str.strip():
            recipes = parse_recipe_results(results_str)
        print(f"Found {len(recipes)} {recipe_type} recipes for '{app_name}'")
    except CalledProcessError as e:
        print(f"ERROR: Filtered search failed for '{app_name}'")
        #print(f"Command was: {cmd}")
        print(f"Error: {e}")
        pass
    
    # Filter out deprecated recipes
    if recipes:
        non_deprecated = []
        for recipe in recipes:
            recipe_path = f"{recipe_dir}/{recipe['repo']}/{recipe['name']}"
            if not is_recipe_deprecated(recipe_path) and 'deprecated' not in recipe['name'].lower():
                non_deprecated.append(recipe)
        if non_deprecated:
            recipes = non_deprecated
    
    # Sort recipes by star count (highest first)
    if recipes:
        recipes.sort(key=lambda r: repo_stars.get(r['repo'], 0), reverse=True)
    
    return recipes


def find_recipes(apps, repo_list, recipe_dir, output_dir):   
    """
    Search for autopkg recipes with the given terms.
    Prioritizes .munki recipes, falls back to .download recipes.
    Within each type, selects the recipe from the repo with most stars.
    Returns the apps list with recipe results added.
    """
    # Create a dict mapping repo names to star counts for quick lookup
    repo_stars = {repo['name']: repo['stars'] for repo in repo_list}
    
    # Create results list
    results = []
    
    for app in apps:
        app_name = app["Application"]
        
        # Search for munki recipes first
        munki_recipes = search_and_filter_recipes(app_name, 'munki', recipe_dir, repo_stars)
        
        # Only search for download recipes if no munki recipes found
        download_recipes = []
        if not munki_recipes:
            download_recipes = search_and_filter_recipes(app_name, 'download', recipe_dir, repo_stars)
        
        # Create result entry starting with original app data
        result_entry = app.copy()
        
        # Choose the best recipe (munki preferred over download)
        found_recipe = None
        recipe_type = None
        
        if munki_recipes:
            found_recipe = munki_recipes[0]  # Already sorted by stars
            recipe_type = "munki"
        elif download_recipes:
            found_recipe = download_recipes[0]  # Already sorted by stars
            recipe_type = "download"
        
        if not found_recipe:
            print(f"No recipes found for: {app_name}")
            result_entry['recipe_name'] = "Not Found"
            result_entry['recipe_type'] = "N/A"
            result_entry['repo'] = "N/A"
            result_entry['repo_stars'] = 0
            results.append(result_entry)
            continue
        
        # Add recipe info to result
        stars = repo_stars.get(found_recipe['repo'], 0)
        result_entry['recipe_name'] = found_recipe['name']
        result_entry['recipe_type'] = recipe_type
        result_entry['repo'] = found_recipe['repo']
        result_entry['repo_stars'] = stars
        
        print(f"Found {recipe_type} recipe for {app_name}: {found_recipe['name']} (from {found_recipe['repo']} - {stars} stars)")
        
        # Add the repo if needed
        repo_name = found_recipe['repo']
        try:
            _run_command(f"autopkg repo-add {repo_name}")
            print(f"Added repo: {repo_name}")
        except CalledProcessError as e:
            print(f"Repo may already exist or error adding: {repo_name}")
        
        # Create app-specific subdirectory in output
        app_output_dir = os.path.join(output_dir, app_name.replace("/", "_").replace(" ", "_"))
        os.makedirs(app_output_dir, exist_ok=True)
        
        # Copy the main recipe
        recipe_filename = found_recipe['name']
        source_path = f"{recipe_dir}/{repo_name}/{recipe_filename}"
        dest_path = f"{app_output_dir}/{recipe_filename}"
        
        copied_recipes = []
        try:
            _run_command(f"cp '{source_path}' '{dest_path}'")
            print(f"Copied recipe to: {dest_path}")
            copied_recipes.append(recipe_filename)
            
            # Check for dependencies and copy them too
            dependencies = get_recipe_dependencies(source_path)
            for dep in dependencies:
                # Find the dependency recipe
                dep_source = f"{recipe_dir}/{repo_name}/{dep}"
                dep_dest = f"{app_output_dir}/{dep}"
                
                # Check if dependency exists and is not deprecated
                if os.path.exists(dep_source) and not is_recipe_deprecated(dep_source):
                    try:
                        _run_command(f"cp '{dep_source}' '{dep_dest}'")
                        print(f"Copied dependency: {dep}")
                        copied_recipes.append(dep)
                    except CalledProcessError:
                        print(f"Error copying dependency: {dep}")
                else:
                    # Try to find dependency in other repos
                    print(f"Dependency {dep} not found in same repo or is deprecated, searching...")
                    # You could add logic here to search for the dependency in other repos
            
            result_entry['copied'] = True
            result_entry['copied_recipes'] = copied_recipes
        except CalledProcessError as e:
            print(f"Error copying recipe: {e}")
            result_entry['copied'] = False
            result_entry['copied_recipes'] = []
        
        results.append(result_entry)
    
    return results

def write_results_to_csv(results, output_file):
    """Write results to CSV file.
    
    Args:
        results: List of dictionaries containing the results
        output_file: Path to the output CSV file
    """
    with open(output_file, 'w', newline='') as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
    print(f"Results written to: {output_file}")

def main():
    '''Setup input and output files and create directories'''

    load_dotenv()  # Load environment variables from .env file if present
    github_token = os.getenv("GITHUB_TOKEN")
    if github_token:
        # Set the token for autopkg to use
        run(['defaults', 'write', 'com.github.autopkg', 'GITHUB_TOKEN', github_token])
        print("GitHub token set for AutoPkg")
    else:
        print("WARNING: No GITHUB_TOKEN found in environment. AutoPkg searches may fail.")

    input_csv = input("Enter the path to the input CSV file (Requires Column \"Application\"): ")
    input_csv = os.path.abspath(os.path.expanduser(input_csv))
    app_import = read_csv(input_csv)
    output_csv_location = input_csv.replace(".csv", "-with-autopkg-recipe.csv")
    output_directory_location = os.path.join(os.path.dirname(output_csv_location), "output-recipes")
    os.makedirs(output_directory_location, exist_ok=True)
    
    # Always use fresh repos
    all_recipes_directory = "~/tmp/autopkg-recipes"
    if os.path.exists(all_recipes_directory):
        shutil.rmtree(all_recipes_directory)
    os.makedirs(all_recipes_directory, exist_ok=True)
    
    # Fetch and process repos
    repo_list = fetch_repos(github_token)
    download_repos(repo_list)
    repo_list = build_metadata(repo_list, all_recipes_directory)
    applications_result = find_recipes(app_import, repo_list, all_recipes_directory, output_directory_location)
    write_results_to_csv(applications_result, output_csv_location)

if __name__ == "__main__":
    main()