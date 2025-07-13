#!/usr/bin/env python3
"""
AutoPkg Recipe Finder - Web Version
Searches for AutoPkg recipes using autopkgweb.com instead of GitHub API
"""

import requests
import csv
import time
import os
from urllib.parse import quote
from bs4 import BeautifulSoup
from typing import Dict, List, Optional


def read_csv(import_file: str) -> List[Dict[str, str]]:
    """Read CSV file and return list of dictionaries."""
    with open(import_file, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def search_autopkg_web(app_name: str, recipe_type: str = "") -> List[Dict[str, str]]:
    """
    Search autopkgweb.com for recipes.
    
    Args:
        app_name: Application name to search for
        recipe_type: Type of recipe (download, munki, pkg, etc.) or empty for all
        
    Returns:
        List of recipe dictionaries with name, type, description, repo, and file info
    """
    # Build search URL
    base_url = "https://autopkgweb.com/"
    params = {
        "search": app_name
    }
    if recipe_type:
        params["type"] = recipe_type
    
    # Construct URL with parameters
    param_string = "&".join([f"{k}={quote(v)}" for k, v in params.items()])
    search_url = f"{base_url}?{param_string}"
    
    print(f"Searching: {search_url}")
    
    try:
        response = requests.get(search_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching search results: {e}")
        return []
    
    # Parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all recipe rows
    recipes = []
    recipe_rows = soup.select('tr.recipe-row')
    
    for row in recipe_rows:
        cells = row.find_all('td')
        if len(cells) >= 5:
            # Extract recipe information
            name_cell = cells[0]
            type_cell = cells[1]
            desc_cell = cells[2]
            repo_cell = cells[3]
            file_cell = cells[4]
            
            # Get recipe name (remove icon and whitespace)
            name = name_cell.get_text(strip=True)
            # Remove icon text if present
            if name.startswith('Logi'):  # Adjust based on actual icon text
                name = name
            
            # Get recipe type from badge
            recipe_type = type_cell.find('span', class_='badge')
            recipe_type = recipe_type.get_text(strip=True) if recipe_type else ""
            
            # Get description
            desc_div = desc_cell.find('div', class_='description-content')
            description = desc_div.get_text(strip=True) if desc_div else ""
            
            # Get repository name
            repo_link = repo_cell.find('a')
            repo = repo_link.get_text(strip=True) if repo_link else ""
            repo_url = repo_link.get('href') if repo_link else ""
            
            # Get recipe file info
            file_link = file_cell.find('a')
            recipe_file = file_link.get_text(strip=True) if file_link else ""
            recipe_file_url = file_link.get('href') if file_link else ""
            
            # Check if deprecated (look for deprecated indicator in name cell)
            deprecated = "deprecated" in name_cell.get_text(strip=True).lower()
            
            recipes.append({
                "name": name,
                "type": recipe_type,
                "description": description,
                "repo": repo,
                "repo_url": repo_url,
                "recipe_file": recipe_file,
                "recipe_file_url": recipe_file_url,
                "deprecated": deprecated
            })
    
    return recipes


def download_recipe_file(recipe_url: str, output_path: str) -> bool:
    """
    Download a recipe file from GitHub.
    
    Args:
        recipe_url: GitHub URL to the recipe file
        output_path: Local path to save the file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Convert GitHub blob URL to raw URL
        # From: https://github.com/autopkg/user-recipes/blob/master/Recipe.recipe
        # To: https://raw.githubusercontent.com/autopkg/user-recipes/master/Recipe.recipe
        raw_url = recipe_url.replace('github.com', 'raw.githubusercontent.com')
        raw_url = raw_url.replace('/blob/', '/')
        
        response = requests.get(raw_url, timeout=30)
        response.raise_for_status()
        
        # Create directory if needed
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        return True
    except Exception as e:
        print(f"Error downloading recipe: {e}")
        return False


def get_parent_recipes(recipe_content: str) -> List[str]:
    """
    Extract parent recipe dependencies from recipe content.
    
    Args:
        recipe_content: Recipe plist content
        
    Returns:
        List of parent recipe identifiers
    """
    import re
    
    parents = []
    # Look for ParentRecipe key in plist
    parent_match = re.search(r'<key>ParentRecipe</key>\s*<string>([^<]+)</string>', recipe_content)
    if parent_match:
        parents.append(parent_match.group(1))
    
    return parents


def find_best_recipe(app_name: str) -> Optional[Dict[str, str]]:
    """
    Find the best recipe for an application.
    Prioritizes munki recipes over download recipes.
    Filters out deprecated recipes.
    
    Args:
        app_name: Application name to search for
        
    Returns:
        Best recipe found or None
    """
    print(f"\nSearching for: {app_name}")
    
    # First search for munki recipes
    munki_recipes = search_autopkg_web(app_name, "munki")
    
    # Filter out deprecated recipes
    munki_recipes = [r for r in munki_recipes if not r["deprecated"]]
    
    if munki_recipes:
        print(f"Found {len(munki_recipes)} munki recipe(s)")
        # Return the first (autopkgweb.com already sorts by popularity)
        return munki_recipes[0]
    
    # If no munki recipes, search for download recipes
    download_recipes = search_autopkg_web(app_name, "download")
    
    # Filter out deprecated recipes
    download_recipes = [r for r in download_recipes if not r["deprecated"]]
    
    if download_recipes:
        print(f"Found {len(download_recipes)} download recipe(s)")
        return download_recipes[0]
    
    # No recipes found
    print(f"No recipes found for: {app_name}")
    return None


def process_applications(apps: List[Dict[str, str]], output_dir: str) -> List[Dict[str, str]]:
    """
    Process a list of applications and find recipes for each.
    
    Args:
        apps: List of dictionaries with "Application" key
        output_dir: Directory to save recipe files
        
    Returns:
        List of results with recipe information added
    """
    results = []
    
    for i, app in enumerate(apps, 1):
        app_name = app.get("Application", "")
        if not app_name:
            print(f"Skipping row {i}: No application name")
            continue
        
        print(f"\n[{i}/{len(apps)}] Processing: {app_name}")
        
        # Create result entry starting with original app data
        result_entry = app.copy()
        
        # Find best recipe
        recipe = find_best_recipe(app_name)
        
        if recipe:
            result_entry.update({
                "recipe_name": recipe["name"],
                "recipe_type": recipe["type"],
                "repo": recipe["repo"],
                "repo_url": recipe["repo_url"],
                "recipe_file": recipe["recipe_file"],
                "recipe_file_url": recipe["recipe_file_url"],
                "description": recipe["description"],
                "found": True
            })
            print(f"✓ Found {recipe['type']} recipe: {recipe['name']} (from {recipe['repo']})")
            
            # Create app-specific subdirectory
            safe_app_name = app_name.replace("/", "_").replace(" ", "_").replace(":", "")
            app_output_dir = os.path.join(output_dir, safe_app_name)
            
            # Download the recipe file
            recipe_filename = recipe["recipe_file"]
            local_recipe_path = os.path.join(app_output_dir, recipe_filename)
            
            print(f"  Downloading recipe to: {app_output_dir}")
            
            downloaded_files = []
            if download_recipe_file(recipe["recipe_file_url"], local_recipe_path):
                print(f"  ✓ Downloaded: {recipe_filename}")
                downloaded_files.append(recipe_filename)
                
                # Check for parent recipes and download them too
                with open(local_recipe_path, 'r', encoding='utf-8') as f:
                    recipe_content = f.read()
                
                parent_recipes = get_parent_recipes(recipe_content)
                for parent in parent_recipes:
                    print(f"  Found parent recipe: {parent}")
                    # Search for parent recipe
                    parent_results = search_autopkg_web(parent, "")
                    if parent_results:
                        parent_recipe = parent_results[0]
                        parent_filename = parent_recipe["recipe_file"]
                        parent_path = os.path.join(app_output_dir, parent_filename)
                        if download_recipe_file(parent_recipe["recipe_file_url"], parent_path):
                            print(f"  ✓ Downloaded parent: {parent_filename}")
                            downloaded_files.append(parent_filename)
                    else:
                        print(f"  ✗ Could not find parent recipe: {parent}")
                
                result_entry["downloaded"] = True
                result_entry["downloaded_files"] = ", ".join(downloaded_files)
                result_entry["local_path"] = app_output_dir
            else:
                result_entry["downloaded"] = False
                result_entry["downloaded_files"] = ""
                result_entry["local_path"] = ""
                print(f"  ✗ Failed to download recipe")
                
        else:
            result_entry.update({
                "recipe_name": "Not Found",
                "recipe_type": "N/A",
                "repo": "N/A",
                "repo_url": "",
                "recipe_file": "N/A",
                "recipe_file_url": "",
                "description": "",
                "found": False,
                "downloaded": False,
                "downloaded_files": "",
                "local_path": ""
            })
            print(f"✗ No recipes found")
        
        results.append(result_entry)
        
        # Be polite to the server
        time.sleep(0.5)
    
    return results


def write_results_to_csv(results: List[Dict[str, str]], output_file: str):
    """Write results to CSV file."""
    if not results:
        print("No results to write")
        return
    
    # Define field order (original fields first, then new fields)
    original_fields = [k for k in results[0].keys() if k not in [
        "recipe_name", "recipe_type", "repo", "repo_url", 
        "recipe_file", "recipe_file_url", "description", "found",
        "downloaded", "downloaded_files", "local_path"
    ]]
    
    new_fields = [
        "recipe_name", "recipe_type", "repo", "repo_url", 
        "recipe_file", "recipe_file_url", "description", "found",
        "downloaded", "downloaded_files", "local_path"
    ]
    
    fieldnames = original_fields + new_fields
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nResults written to: {output_file}")


def generate_summary_report(results: List[Dict[str, str]], output_file: str):
    """Generate a summary report of the results."""
    total = len(results)
    found = sum(1 for r in results if r.get("found", False))
    not_found = total - found
    downloaded = sum(1 for r in results if r.get("downloaded", False))
    
    munki_count = sum(1 for r in results if r.get("recipe_type") == "munki")
    download_count = sum(1 for r in results if r.get("recipe_type") == "download")
    
    # Count by repository
    repo_counts = {}
    for r in results:
        if r.get("found", False):
            repo = r.get("repo", "Unknown")
            repo_counts[repo] = repo_counts.get(repo, 0) + 1
    
    report = f"""AutoPkg Recipe Search Summary
=============================
Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}

Total Applications: {total}
Recipes Found: {found} ({found/total*100:.1f}%)
Not Found: {not_found} ({not_found/total*100:.1f}%)
Successfully Downloaded: {downloaded} ({downloaded/total*100:.1f}%)

Recipe Types:
- Munki: {munki_count}
- Download: {download_count}

Top Repositories:
"""
    
    # Add top 10 repositories
    for repo, count in sorted(repo_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        report += f"- {repo}: {count} recipes\n"
    
    # List applications without recipes
    if not_found > 0:
        report += f"\nApplications without recipes ({not_found}):\n"
        for r in results:
            if not r.get("found", False):
                report += f"- {r.get('Application', 'Unknown')}\n"
    
    # List applications with failed downloads
    failed_downloads = [r for r in results if r.get("found", False) and not r.get("downloaded", False)]
    if failed_downloads:
        report += f"\nFailed downloads ({len(failed_downloads)}):\n"
        for r in failed_downloads:
            report += f"- {r.get('Application', 'Unknown')}\n"
    
    # Save report
    report_file = output_file.replace('.csv', '-summary.txt')
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"Summary report written to: {report_file}")
    print("\n" + report)


def main():
    """Main function."""
    print("AutoPkg Recipe Finder - Web Version")
    print("===================================\n")
    
    # Get input file
    input_csv = input("Enter the path to the input CSV file (requires column 'Application'): ")
    input_csv = os.path.abspath(os.path.expanduser(input_csv))
    
    if not os.path.exists(input_csv):
        print(f"Error: File not found: {input_csv}")
        return
    
    # Read applications from CSV
    try:
        apps = read_csv(input_csv)
        print(f"\nLoaded {len(apps)} applications from CSV")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    # Verify required column exists
    if apps and "Application" not in apps[0]:
        print("Error: CSV must contain an 'Application' column")
        print(f"Found columns: {', '.join(apps[0].keys())}")
        return
    
    # Set output paths
    output_csv = input_csv.replace(".csv", "-autopkg-recipes.csv")
    output_dir = os.path.join(os.path.dirname(input_csv), "autopkg-recipes")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nRecipe files will be saved to: {output_dir}")
    
    # Process applications
    print("\nSearching for recipes...")
    print("=" * 50)
    
    results = process_applications(apps, output_dir)
    
    # Write results
    write_results_to_csv(results, output_csv)
    
    # Generate summary report
    generate_summary_report(results, output_csv)
    
    print("\nDone!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()