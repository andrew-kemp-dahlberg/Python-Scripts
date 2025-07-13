#!/usr/bin/env python3
"""
AutoPkg Recipe Finder - Enterprise Version
Part of automated user onboarding system

Searches for and downloads AutoPkg recipes from autopkgweb.com
Organizes in standard structure: com.github.autopkg.{repo_name}/{app_name}/
"""

import requests
import csv
import time
import os
import re
import logging
import json
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path

# Configuration
RECIPE_TYPES_PRIORITY = ["munki", "download"]
RECIPE_SUFFIXES = ['.download', '.munki', '.pkg', '.install', '.jss', '.jamf']
SEARCH_TIMEOUT = 30
REQUEST_DELAY = 0.5
MAX_RETRIES = 3
RETRY_DELAY = 2

# Setup logging
def setup_logging(log_dir: Path) -> logging.Logger:
    """Setup enterprise logging with file and console output."""
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"autopkg_finder_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

@dataclass
class Recipe:
    """Recipe data structure for type safety."""
    name: str
    type: str
    description: str
    repo: str
    repo_url: str
    recipe_file: str
    recipe_file_url: str
    deprecated: bool

@dataclass
class ProcessingResult:
    """Result of processing an application."""
    application: str
    recipe_name: str
    recipe_type: str
    repo: str
    found: bool
    downloaded: bool
    downloaded_files: List[str]
    local_paths: List[str]
    error: Optional[str] = None

class AutoPkgRecipeFinder:
    """Main class for finding and downloading AutoPkg recipes."""
    
    def __init__(self, output_dir: Path, logger: logging.Logger):
        self.output_dir = output_dir
        self.logger = logger
        self.session = requests.Session()
        self.stats = {
            "total": 0,
            "found": 0,
            "downloaded": 0,
            "errors": 0
        }
    
    def sanitize_name(self, name: str) -> str:
        """Convert name to safe directory name."""
        return re.sub(r'[<>:"/\\|?*]', '_', name)
    
    def strip_recipe_suffix(self, name: str) -> str:
        """Remove common recipe suffixes from name."""
        for suffix in RECIPE_SUFFIXES:
            if name.endswith(suffix):
                return name[:-len(suffix)]
        return name
    
    def search_autopkg_web(self, app_name: str, recipe_type: str = "") -> List[Recipe]:
        """Search autopkgweb.com for recipes with retry logic."""
        params = {"search": app_name}
        if recipe_type:
            params["type"] = recipe_type
        
        url = f"https://autopkgweb.com/?{'&'.join(f'{k}={quote(v)}' for k, v in params.items())}"
        self.logger.info(f"Searching: {url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=SEARCH_TIMEOUT)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                self.logger.warning(f"Search attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    self.logger.error(f"Failed to search after {MAX_RETRIES} attempts")
                    return []
        
        return self._parse_search_results(response.text)
    
    def _parse_search_results(self, html: str) -> List[Recipe]:
        """Parse HTML search results into Recipe objects."""
        soup = BeautifulSoup(html, 'html.parser')
        recipes = []
        
        for row in soup.select('tr.recipe-row'):
            cells = row.find_all('td')
            if len(cells) < 5:
                continue
            
            try:
                # Extract data safely
                name = cells[0].get_text(strip=True)
                type_elem = cells[1].find('span', class_='badge')
                desc_elem = cells[2].find('div', class_='description-content')
                repo_link = cells[3].find('a')
                file_link = cells[4].find('a')
                
                recipe = Recipe(
                    name=name,
                    type=type_elem.get_text(strip=True) if type_elem else "",
                    description=desc_elem.get_text(strip=True) if desc_elem else "",
                    repo=repo_link.get_text(strip=True) if repo_link else "",
                    repo_url=repo_link.get('href', '') if repo_link else "",
                    recipe_file=file_link.get_text(strip=True) if file_link else "",
                    recipe_file_url=file_link.get('href', '') if file_link else "",
                    deprecated="deprecated" in name.lower()
                )
                recipes.append(recipe)
            except Exception as e:
                self.logger.warning(f"Error parsing recipe row: {e}")
        
        return recipes
    
    def download_recipe_file(self, url: str, output_path: Path) -> bool:
        """Download recipe file from GitHub with retry logic."""
        raw_url = url.replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(raw_url, timeout=SEARCH_TIMEOUT)
                response.raise_for_status()
                
                # Create directory and save file
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(response.text, encoding='utf-8')
                
                self.logger.info(f"Downloaded: {output_path}")
                return True
                
            except Exception as e:
                self.logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        
        self.logger.error(f"Failed to download {url} after {MAX_RETRIES} attempts")
        return False
    
    def get_parent_recipes(self, content: str) -> List[str]:
        """Extract parent recipe names from recipe content."""
        parents = []
        
        # Check plist format
        if match := re.search(r'<key>ParentRecipe</key>\s*<string>([^<]+)</string>', content):
            parents.append(match.group(1))
        
        # Check YAML format
        if match := re.search(r'ParentRecipe:\s*([^\n]+)', content):
            parent = match.group(1).strip().strip('"').strip("'")
            parents.append(parent)
        
        return parents
    
    def find_best_recipe(self, app_name: str) -> Optional[Recipe]:
        """Find the best recipe for an app (prioritize munki over download)."""
        for recipe_type in RECIPE_TYPES_PRIORITY:
            recipes = self.search_autopkg_web(app_name, recipe_type)
            # Filter out deprecated
            valid_recipes = [r for r in recipes if not r.deprecated]
            
            if valid_recipes:
                self.logger.info(f"Found {len(valid_recipes)} {recipe_type} recipe(s) for {app_name}")
                return valid_recipes[0]  # Return first (best match)
        
        return None
    
    def process_application(self, app_data: Dict[str, str]) -> ProcessingResult:
        """Process a single application: find and download recipes."""
        app_name = app_data.get("Application", "")
        
        if not app_name:
            return ProcessingResult(
                application="Unknown",
                recipe_name="Not Found",
                recipe_type="N/A",
                repo="N/A",
                found=False,
                downloaded=False,
                downloaded_files=[],
                local_paths=[],
                error="No application name provided"
            )
        
        self.logger.info(f"Processing: {app_name}")
        self.stats["total"] += 1
        
        try:
            # Find recipe
            recipe = self.find_best_recipe(app_name)
            
            if not recipe:
                self.logger.warning(f"No recipes found for: {app_name}")
                return ProcessingResult(
                    application=app_name,
                    recipe_name="Not Found",
                    recipe_type="N/A",
                    repo="N/A",
                    found=False,
                    downloaded=False,
                    downloaded_files=[],
                    local_paths=[]
                )
            
            self.stats["found"] += 1
            
            # Download recipe and parents
            downloaded_files, local_paths = self._download_recipe_chain(recipe)
            
            if downloaded_files:
                self.stats["downloaded"] += 1
            
            return ProcessingResult(
                application=app_name,
                recipe_name=recipe.name,
                recipe_type=recipe.type,
                repo=recipe.repo,
                found=True,
                downloaded=bool(downloaded_files),
                downloaded_files=downloaded_files,
                local_paths=local_paths
            )
            
        except Exception as e:
            self.logger.error(f"Error processing {app_name}: {e}")
            self.stats["errors"] += 1
            return ProcessingResult(
                application=app_name,
                recipe_name="Error",
                recipe_type="N/A",
                repo="N/A",
                found=False,
                downloaded=False,
                downloaded_files=[],
                local_paths=[],
                error=str(e)
            )
    
    def _download_recipe_chain(self, recipe: Recipe) -> Tuple[List[str], List[str]]:
        """Download a recipe and all its parents."""
        downloaded_files = []
        local_paths = []
        
        # Prepare paths for main recipe
        repo_dir = f"com.github.autopkg.{recipe.repo}"
        app_dir = self.sanitize_name(self.strip_recipe_suffix(recipe.name))
        recipe_path = self.output_dir / repo_dir / app_dir / recipe.recipe_file
        
        # Download main recipe
        if self.download_recipe_file(recipe.recipe_file_url, recipe_path):
            downloaded_files.append(recipe.recipe_file)
            local_paths.append(str(recipe_path.relative_to(self.output_dir)))
            
            # Check for parent recipes
            content = recipe_path.read_text(encoding='utf-8')
            for parent_name in self.get_parent_recipes(content):
                self.logger.info(f"Found parent recipe: {parent_name}")
                
                # Search for parent
                parent_recipes = self.search_autopkg_web(parent_name)
                if parent_recipes:
                    parent = parent_recipes[0]
                    parent_repo_dir = f"com.github.autopkg.{parent.repo}"
                    parent_app_dir = self.sanitize_name(self.strip_recipe_suffix(parent.name))
                    parent_path = self.output_dir / parent_repo_dir / parent_app_dir / parent.recipe_file
                    
                    if self.download_recipe_file(parent.recipe_file_url, parent_path):
                        downloaded_files.append(parent.recipe_file)
                        local_paths.append(str(parent_path.relative_to(self.output_dir)))
        
        return downloaded_files, local_paths
    
    def process_applications(self, apps: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Process all applications and return results."""
        results = []
        
        for i, app in enumerate(apps, 1):
            self.logger.info(f"[{i}/{len(apps)}] Processing application")
            
            # Merge original data with processing result
            result = app.copy()
            processing_result = self.process_application(app)
            
            # Convert ProcessingResult to dict and merge
            result.update({
                "recipe_name": processing_result.recipe_name,
                "recipe_type": processing_result.recipe_type,
                "repo": processing_result.repo,
                "found": processing_result.found,
                "downloaded": processing_result.downloaded,
                "downloaded_files": ", ".join(processing_result.downloaded_files),
                "local_paths": "; ".join(processing_result.local_paths),
                "error": processing_result.error or ""
            })
            
            results.append(result)
            
            # Rate limiting
            time.sleep(REQUEST_DELAY)
        
        return results
    
    def generate_report(self) -> Dict[str, any]:
        """Generate processing report with statistics."""
        return {
            "timestamp": datetime.now().isoformat(),
            "statistics": self.stats,
            "success_rate": self.stats["found"] / self.stats["total"] * 100 if self.stats["total"] > 0 else 0,
            "download_rate": self.stats["downloaded"] / self.stats["found"] * 100 if self.stats["found"] > 0 else 0
        }


def main(input_csv_path: Optional[str] = None):
    """Main entry point for standalone execution or integration."""
    
    # If called from another system, skip interactive parts
    if input_csv_path:
        input_csv = Path(input_csv_path).expanduser().absolute()
    else:
        print("AutoPkg Recipe Finder - Enterprise Version\n" + "="*40 + "\n")
        input_csv = input("Enter CSV path (requires 'Application' column): ")
        input_csv = Path(input_csv).expanduser().absolute()
    
    if not input_csv.exists():
        print(f"Error: File not found: {input_csv}")
        return 1
    
    # Setup paths
    base_dir = input_csv.parent
    output_dir = base_dir / "autopkg-recipes"
    log_dir = base_dir / "logs"
    
    # Initialize logging
    logger = setup_logging(log_dir)
    logger.info(f"Starting AutoPkg Recipe Finder")
    logger.info(f"Input: {input_csv}")
    logger.info(f"Output: {output_dir}")
    
    try:
        # Read applications
        with open(input_csv, 'r', encoding='utf-8-sig') as f:
            apps = list(csv.DictReader(f))
        
        if not apps:
            logger.error("No applications found in CSV")
            return 1
        
        if "Application" not in apps[0]:
            logger.error(f"Missing 'Application' column. Found: {list(apps[0].keys())}")
            return 1
        
        logger.info(f"Loaded {len(apps)} applications")
        
        # Process applications
        finder = AutoPkgRecipeFinder(output_dir, logger)
        results = finder.process_applications(apps)
        
        # Write results
        output_csv = input_csv.with_name(input_csv.stem + "-autopkg-recipes.csv")
        
        if results:
            keys = list(results[0].keys())
            with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(results)
            
            logger.info(f"Results written to: {output_csv}")
        
        # Generate report
        report = finder.generate_report()
        report_file = output_csv.with_suffix('.json')
        report_file.write_text(json.dumps(report, indent=2))
        
        # Print summary
        print(f"\nProcessing Complete!")
        print(f"Total: {report['statistics']['total']}")
        print(f"Found: {report['statistics']['found']} ({report['success_rate']:.1f}%)")
        print(f"Downloaded: {report['statistics']['downloaded']} ({report['download_rate']:.1f}%)")
        print(f"Errors: {report['statistics']['errors']}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


# For integration with larger system
def process_user_applications(applications: List[Dict[str, str]], 
                            output_dir: Path, 
                            logger: Optional[logging.Logger] = None) -> Dict[str, any]:
    """
    API function for integration with user onboarding system.
    
    Args:
        applications: List of dicts with 'Application' key
        output_dir: Where to save recipes
        logger: Optional logger instance
        
    Returns:
        Dict with results and statistics
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    finder = AutoPkgRecipeFinder(output_dir, logger)
    results = finder.process_applications(applications)
    report = finder.generate_report()
    
    return {
        "results": results,
        "report": report,
        "success": report["statistics"]["errors"] == 0
    }


if __name__ == "__main__":
    exit(main())