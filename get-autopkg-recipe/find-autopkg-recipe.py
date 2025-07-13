#!/usr/bin/env python3
"""
AutoPkg Recipe Finder - Complete Enterprise Version with Last Mile Support
Combines standard AutoPkg recipe finding with Perplexity + Recipe Robot for custom recipes

Searches for and downloads AutoPkg recipes from autopkgweb.com
For apps without recipes, uses Perplexity API to find downloads and Recipe Robot to create them
"""

import requests
import csv
import time
import os
import re
import logging
import json
import subprocess
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

# Last Mile Configuration
PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
RECIPE_ROBOT_PATH = "/usr/local/bin/recipe-robot"
LAST_MILE_RECIPE_PREFIX = "com.github.anywhereops.anywhereops-recipes"
RECIPE_OUTPUT_DIR = "~/Library/AutoPkg/Recipe Robot Output"

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


# ===== LAST MILE HANDLER CLASSES =====

class PerplexityDownloadFinder:
    """Find application download URLs using Perplexity API."""
    
    def __init__(self, api_key: str, logger: logging.Logger):
        self.api_key = api_key
        self.logger = logger
        self.base_url = "https://api.perplexity.ai/async/chat/completions"
    
    def find_download_url(self, app_name: str) -> Dict[str, any]:
        """Query Perplexity to find the best download URL for an application."""
        
        prompt = f"""Find the official download URL for "{app_name}" for macOS. 
        Requirements:
        1. Must be a direct download link (ends in .pkg, .dmg, .zip)
        2. Prioritize .pkg files over .dmg or .zip
        3. Must be from the official website or trusted source
        4. Must be the latest stable version
        5. If no direct download link exists, return "NO_DIRECT_DOWNLOAD"
        
        Return ONLY the download URL or "NO_DIRECT_DOWNLOAD", nothing else."""
        
        payload = {
            "request": {
                "model": "sonar",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a precise download URL finder. Return only direct download URLs or 'NO_DIRECT_DOWNLOAD'."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "search_mode": "web",
                "reasoning_effort": "high",
                "max_tokens": 200,
                "temperature": 0.1,
                "top_p": 0.95,
                "stream": False,
                "web_search_options": {"search_context_size": "high"}
            }
        }
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            self.logger.info(f"Querying Perplexity for {app_name} download URL")
            response = requests.post(self.base_url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            request_id = result.get("id")
            
            # Poll for completion
            for attempt in range(10):
                time.sleep(2)
                status_response = requests.get(
                    f"{self.base_url}/{request_id}",
                    headers=headers
                )
                status_data = status_response.json()
                
                if status_data.get("status") == "COMPLETED":
                    return self._parse_download_url(status_data, app_name)
                elif status_data.get("status") == "FAILED":
                    self.logger.error(f"Perplexity request failed: {status_data.get('error_message')}")
                    return None
            
            self.logger.error("Perplexity request timed out")
            return None
            
        except Exception as e:
            self.logger.error(f"Error querying Perplexity: {e}")
            return None
    
    def _parse_download_url(self, response_data: Dict, app_name: str) -> Dict[str, any]:
        """Parse Perplexity response to extract download URL and metadata."""
        try:
            content = response_data["response"]["choices"][0]["message"]["content"].strip()
            citations = response_data["response"].get("citations", [])
            search_results = response_data["response"].get("search_results", [])
            
            if content == "NO_DIRECT_DOWNLOAD" or not content:
                return {
                    "found": False,
                    "url": None,
                    "type": None,
                    "source": None
                }
            
            url = content.strip()
            if not url.startswith(("http://", "https://")):
                return {"found": False, "url": None, "type": None, "source": None}
            
            download_type = "unknown"
            if url.endswith(".pkg"):
                download_type = "pkg"
            elif url.endswith(".dmg"):
                download_type = "dmg"
            elif url.endswith(".zip"):
                download_type = "zip"
            
            source = None
            if search_results:
                source = search_results[0].get("url", "Unknown")
            
            return {
                "found": True,
                "url": url,
                "type": download_type,
                "source": source,
                "citations": citations
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing Perplexity response: {e}")
            return {"found": False, "url": None, "type": None, "source": None}


class RecipeRobotHandler:
    """Handle Recipe Robot operations."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.recipe_robot_path = Path(RECIPE_ROBOT_PATH)
        self.output_dir = Path(RECIPE_OUTPUT_DIR).expanduser()
        
        if not self.recipe_robot_path.exists():
            self._setup_recipe_robot()
    
    def _setup_recipe_robot(self):
        """Set up Recipe Robot symlink if not present."""
        try:
            self.logger.info("Setting up Recipe Robot symlink")
            subprocess.run([
                "ln", "-s",
                "/Applications/Recipe Robot.app/Contents/Resources/scripts/recipe-robot",
                str(self.recipe_robot_path)
            ], check=True)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to setup Recipe Robot: {e}")
            raise
    
    def create_recipes(self, app_name: str, download_url: str, 
                      download_type: str) -> Dict[str, any]:
        """Use Recipe Robot to create recipes from download URL."""
        
        cmd = [
            str(self.recipe_robot_path),
            "--verbose",
            "--ignore-existing",
            f"--recipe-identifier-prefix={LAST_MILE_RECIPE_PREFIX}",
            download_url
        ]
        
        self.logger.info(f"Running Recipe Robot for {app_name}")
        self.logger.debug(f"Command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            return self._parse_recipe_robot_output(
                result.stdout,
                result.stderr,
                result.returncode,
                app_name
            )
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Recipe Robot timed out for {app_name}")
            return {
                "success": False,
                "error": "Recipe Robot timed out",
                "created_recipes": []
            }
        except Exception as e:
            self.logger.error(f"Error running Recipe Robot: {e}")
            return {
                "success": False,
                "error": str(e),
                "created_recipes": []
            }
    
    def _parse_recipe_robot_output(self, stdout: str, stderr: str, 
                                  returncode: int, app_name: str) -> Dict[str, any]:
        """Parse Recipe Robot output to extract created recipes."""
        
        created_recipes = []
        recipe_dir = None
        
        recipe_pattern = r"Created recipe: (.+\.recipe)"
        for match in re.finditer(recipe_pattern, stdout):
            recipe_path = match.group(1)
            created_recipes.append(recipe_path)
            
            if recipe_dir is None:
                recipe_dir = str(Path(recipe_path).parent)
        
        if returncode != 0 or "ERROR" in stderr:
            error_msg = stderr.strip() if stderr else "Unknown error"
            return {
                "success": False,
                "error": error_msg,
                "created_recipes": created_recipes,
                "recipe_directory": recipe_dir
            }
        
        if "already exists" in stdout:
            return {
                "success": True,
                "warning": "Recipes already exist",
                "created_recipes": created_recipes,
                "recipe_directory": recipe_dir
            }
        
        return {
            "success": True,
            "created_recipes": created_recipes,
            "recipe_directory": recipe_dir,
            "output": stdout
        }
    
    def move_recipes_to_target(self, recipe_dir: str, target_dir: Path, 
                              app_name: str) -> List[str]:
        """Move created recipes to target directory structure."""
        if not recipe_dir:
            return []
        
        source_dir = Path(recipe_dir)
        if not source_dir.exists():
            return []
        
        safe_app_name = re.sub(r'[<>:"/\\|?*]', '_', app_name)
        app_target_dir = target_dir / LAST_MILE_RECIPE_PREFIX / safe_app_name
        app_target_dir.mkdir(parents=True, exist_ok=True)
        
        moved_files = []
        
        for recipe_file in source_dir.glob("*.recipe*"):
            target_path = app_target_dir / recipe_file.name
            try:
                recipe_file.rename(target_path)
                moved_files.append(str(target_path.relative_to(target_dir)))
                self.logger.info(f"Moved {recipe_file.name} to {target_path}")
            except Exception as e:
                self.logger.error(f"Failed to move {recipe_file}: {e}")
        
        return moved_files


class LastMileProcessor:
    """Main processor for handling last mile applications."""
    
    def __init__(self, perplexity_api_key: str, output_dir: Path, logger: logging.Logger):
        self.logger = logger
        self.output_dir = output_dir
        self.perplexity = PerplexityDownloadFinder(perplexity_api_key, logger)
        self.recipe_robot = RecipeRobotHandler(logger)
    
    def process_application(self, app_name: str, original_data: Dict[str, str]) -> Dict[str, any]:
        """Process a single application through the last mile workflow."""
        
        self.logger.info(f"Starting last mile processing for: {app_name}")
        
        # Find download URL via Perplexity
        download_info = self.perplexity.find_download_url(app_name)
        
        if not download_info or not download_info.get("found"):
            self.logger.warning(f"No download URL found for {app_name}")
            return self._create_result(
                original_data,
                success=False,
                error="No direct download URL found",
                perplexity_result=download_info
            )
        
        self.logger.info(f"Found download URL: {download_info['url']} (type: {download_info['type']})")
        
        # Create recipes with Recipe Robot
        recipe_result = self.recipe_robot.create_recipes(
            app_name,
            download_info["url"],
            download_info["type"]
        )
        
        if not recipe_result.get("success"):
            return self._create_result(
                original_data,
                success=False,
                error=recipe_result.get("error", "Recipe Robot failed"),
                perplexity_result=download_info,
                recipe_robot_result=recipe_result
            )
        
        # Move recipes to target directory
        moved_files = []
        if recipe_result.get("recipe_directory"):
            moved_files = self.recipe_robot.move_recipes_to_target(
                recipe_result["recipe_directory"],
                self.output_dir,
                app_name
            )
        
        return self._create_result(
            original_data,
            success=True,
            perplexity_result=download_info,
            recipe_robot_result=recipe_result,
            moved_files=moved_files
        )
    
    def _create_result(self, original_data: Dict[str, str], success: bool, 
                      error: str = None, **kwargs) -> Dict[str, any]:
        """Create standardized result dictionary."""
        result = original_data.copy()
        
        result.update({
            "last_mile_processed": True,
            "last_mile_success": success,
            "last_mile_error": error or "",
            "download_url": kwargs.get("perplexity_result", {}).get("url", ""),
            "download_type": kwargs.get("perplexity_result", {}).get("type", ""),
            "download_source": kwargs.get("perplexity_result", {}).get("source", ""),
            "recipe_robot_success": kwargs.get("recipe_robot_result", {}).get("success", False),
            "created_recipes": ", ".join(kwargs.get("recipe_robot_result", {}).get("created_recipes", [])),
            "recipe_paths": "; ".join(kwargs.get("moved_files", [])),
            "last_mile_timestamp": datetime.now().isoformat()
        })
        
        if success and kwargs.get("moved_files"):
            result.update({
                "recipe_name": f"Custom - {original_data.get('Application', 'Unknown')}",
                "recipe_type": "custom",
                "repo": "anywhereops-recipes",
                "found": True,
                "downloaded": True,
                "local_paths": "; ".join(kwargs.get("moved_files", []))
            })
        
        return result


def process_last_mile_apps(apps_without_recipes: List[Dict[str, str]], 
                          output_dir: Path,
                          perplexity_api_key: str,
                          logger: logging.Logger) -> List[Dict[str, str]]:
    """Process applications without AutoPkg recipes through last mile workflow."""
    
    if not perplexity_api_key:
        logger.error("No Perplexity API key provided")
        return apps_without_recipes
    
    processor = LastMileProcessor(perplexity_api_key, output_dir, logger)
    results = []
    
    for i, app in enumerate(apps_without_recipes, 1):
        app_name = app.get("Application", "")
        if not app_name:
            continue
        
        logger.info(f"[{i}/{len(apps_without_recipes)}] Processing last mile: {app_name}")
        
        try:
            result = processor.process_application(app_name, app)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to process {app_name}: {e}", exc_info=True)
            error_result = app.copy()
            error_result.update({
                "last_mile_processed": True,
                "last_mile_success": False,
                "last_mile_error": str(e)
            })
            results.append(error_result)
        
        time.sleep(2)  # Rate limiting
    
    return results


# ===== MAIN AUTOPKG FINDER CLASS =====

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
        
        if match := re.search(r'<key>ParentRecipe</key>\s*<string>([^<]+)</string>', content):
            parents.append(match.group(1))
        
        if match := re.search(r'ParentRecipe:\s*([^\n]+)', content):
            parent = match.group(1).strip().strip('"').strip("'")
            parents.append(parent)
        
        return parents
    
    def find_best_recipe(self, app_name: str) -> Optional[Recipe]:
        """Find the best recipe for an app (prioritize munki over download)."""
        for recipe_type in RECIPE_TYPES_PRIORITY:
            recipes = self.search_autopkg_web(app_name, recipe_type)
            valid_recipes = [r for r in recipes if not r.deprecated]
            
            if valid_recipes:
                self.logger.info(f"Found {len(valid_recipes)} {recipe_type} recipe(s) for {app_name}")
                return valid_recipes[0]
        
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
        
        repo_dir = f"com.github.autopkg.{recipe.repo}"
        app_dir = self.sanitize_name(self.strip_recipe_suffix(recipe.name))
        recipe_path = self.output_dir / repo_dir / app_dir / recipe.recipe_file
        
        if self.download_recipe_file(recipe.recipe_file_url, recipe_path):
            downloaded_files.append(recipe.recipe_file)
            local_paths.append(str(recipe_path.relative_to(self.output_dir)))
            
            content = recipe_path.read_text(encoding='utf-8')
            for parent_name in self.get_parent_recipes(content):
                self.logger.info(f"Found parent recipe: {parent_name}")
                
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
            
            result = app.copy()
            processing_result = self.process_application(app)
            
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


# ===== MAIN FUNCTIONS =====

def main(input_csv_path: Optional[str] = None, enable_last_mile: bool = True):
    """Main entry point for standalone execution or integration."""
    
    if input_csv_path:
        input_csv = Path(input_csv_path).expanduser().absolute()
    else:
        print("AutoPkg Recipe Finder - Complete Enterprise Version\n" + "="*50 + "\n")
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
        
        # STEP 1: Process applications with standard AutoPkg finder
        finder = AutoPkgRecipeFinder(output_dir, logger)
        results = finder.process_applications(apps)
        
        # STEP 2: Check for last mile processing
        apps_without_recipes = [
            r for r in results 
            if not r.get("found") or r.get("recipe_name") == "Not Found"
        ]
        
        perplexity_api_key = PERPLEXITY_API_KEY
        
        if apps_without_recipes and enable_last_mile and perplexity_api_key:
            logger.info(f"\n{'='*60}")
            logger.info(f"Found {len(apps_without_recipes)} apps without recipes")
            logger.info(f"Starting last mile processing with Perplexity + Recipe Robot")
            logger.info(f"{'='*60}\n")
            
            # Process last mile apps
            last_mile_results = process_last_mile_apps(
                apps_without_recipes=apps_without_recipes,
                output_dir=output_dir,
                perplexity_api_key=perplexity_api_key,
                logger=logger
            )
            
            # Update original results
            for updated in last_mile_results:
                for i, original in enumerate(results):
                    if original.get("Application") == updated.get("Application"):
                        results[i] = updated
                        break
            
            logger.info("Last mile processing complete")
        
        elif apps_without_recipes and not perplexity_api_key:
            logger.warning(f"Found {len(apps_without_recipes)} apps without recipes, but no PERPLEXITY_API_KEY set")
            logger.warning("Set PERPLEXITY_API_KEY environment variable to enable last mile processing")
        
        # Write results
        output_csv = input_csv.with_name(input_csv.stem + "-autopkg-recipes.csv")
        
        if results:
            # Ensure all rows have same keys
            all_keys = set()
            for r in results:
                all_keys.update(r.keys())
            
            for r in results:
                for key in all_keys:
                    if key not in r:
                        r[key] = ""
            
            keys = sorted(list(all_keys))
            with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(results)
            
            logger.info(f"Results written to: {output_csv}")
        
        # Generate enhanced report
        report = finder.generate_report()
        
        if enable_last_mile and perplexity_api_key:
            last_mile_count = sum(1 for r in results if r.get("last_mile_processed"))
            last_mile_success = sum(1 for r in results if r.get("last_mile_success"))
            report["last_mile"] = {
                "processed": last_mile_count,
                "successful": last_mile_success,
                "failed": last_mile_count - last_mile_success
            }
        
        report_file = output_csv.with_suffix('.json')
        report_file.write_text(json.dumps(report, indent=2))
        
        # Print enhanced summary
        print(f"\nProcessing Complete!")
        print(f"{'='*50}")
        print(f"Total Applications: {report['statistics']['total']}")
        print(f"Found in AutoPkg: {report['statistics']['found']} ({report['success_rate']:.1f}%)")
        print(f"Downloaded: {report['statistics']['downloaded']} ({report['download_rate']:.1f}%)")
        
        if "last_mile" in report:
            print(f"\nLast Mile Processing:")
            print(f"Processed: {report['last_mile']['processed']}")
            print(f"Successful: {report['last_mile']['successful']}")
            print(f"Failed: {report['last_mile']['failed']}")
        
        print(f"\nErrors: {report['statistics']['errors']}")
        print(f"{'='*50}")
        
        # Print apps that still need attention
        still_missing = [r for r in results if not r.get("found") or 
                        (r.get("last_mile_processed") and not r.get("last_mile_success"))]
        
        if still_missing:
            print(f"\nApplications still needing attention ({len(still_missing)}):")
            for app in still_missing:
                print(f"- {app.get('Application')}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


def process_user_applications(applications: List[Dict[str, str]], 
                            output_dir: Path, 
                            logger: Optional[logging.Logger] = None,
                            enable_last_mile: bool = True) -> Dict[str, any]:
    """
    API function for integration with user onboarding system.
    
    Args:
        applications: List of dicts with 'Application' key
        output_dir: Where to save recipes
        logger: Optional logger instance
        enable_last_mile: Whether to process apps without recipes through Perplexity
        
    Returns:
        Dict with results and statistics
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    finder = AutoPkgRecipeFinder(output_dir, logger)
    results = finder.process_applications(applications)
    
    # Last mile processing if enabled
    if enable_last_mile and PERPLEXITY_API_KEY:
        apps_without_recipes = [
            r for r in results 
            if not r.get("found") or r.get("recipe_name") == "Not Found"
        ]
        
        if apps_without_recipes:
            last_mile_results = process_last_mile_apps(
                apps_without_recipes=apps_without_recipes,
                output_dir=output_dir,
                perplexity_api_key=PERPLEXITY_API_KEY,
                logger=logger
            )
            
            for updated in last_mile_results:
                for i, original in enumerate(results):
                    if original.get("Application") == updated.get("Application"):
                        results[i] = updated
                        break
    
    report = finder.generate_report()
    
    return {
        "results": results,
        "report": report,
        "success": report["statistics"]["errors"] == 0
    }


if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        input_csv_path = sys.argv[1]
        enable_last_mile = "--no-last-mile" not in sys.argv
        
        exit(main(input_csv_path, enable_last_mile))
    else:
        # Interactive mode
        exit(main())