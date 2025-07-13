#!/usr/bin/env python3
"""
AutoPkg Last Mile Handler
Uses Perplexity API to find download URLs and Recipe Robot to create custom recipes
for applications without existing AutoPkg recipes.
"""

import requests
import json
import subprocess
import re
import time
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# Configuration
PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
RECIPE_ROBOT_PATH = "/usr/local/bin/recipe-robot"
RECIPE_PREFIX = "com.github.anywhereops.anywhereops-recipes"
RECIPE_OUTPUT_DIR = "~/Library/AutoPkg/Recipe Robot Output"

class PerplexityDownloadFinder:
    """Find application download URLs using Perplexity API."""
    
    def __init__(self, api_key: str, logger: logging.Logger):
        self.api_key = api_key
        self.logger = logger
        self.base_url = "https://api.perplexity.ai/async/chat/completions"
    
    def find_download_url(self, app_name: str) -> Dict[str, any]:
        """Query Perplexity to find the best download URL for an application."""
        
        # Craft a specific prompt for finding download URLs
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
            
            # Wait for async completion
            result = response.json()
            request_id = result.get("id")
            
            # Poll for completion (usually takes 2-5 seconds)
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
            
            # Check if no direct download was found
            if content == "NO_DIRECT_DOWNLOAD" or not content:
                return {
                    "found": False,
                    "url": None,
                    "type": None,
                    "source": None
                }
            
            # Validate URL format
            url = content.strip()
            if not url.startswith(("http://", "https://")):
                return {"found": False, "url": None, "type": None, "source": None}
            
            # Determine download type
            download_type = "unknown"
            if url.endswith(".pkg"):
                download_type = "pkg"
            elif url.endswith(".dmg"):
                download_type = "dmg"
            elif url.endswith(".zip"):
                download_type = "zip"
            
            # Get source website
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
        
        # Ensure Recipe Robot is available
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
        
        # Prepare Recipe Robot command
        cmd = [
            str(self.recipe_robot_path),
            "--verbose",
            "--ignore-existing",
            f"--recipe-identifier-prefix={RECIPE_PREFIX}",
            download_url
        ]
        
        self.logger.info(f"Running Recipe Robot for {app_name}")
        self.logger.debug(f"Command: {' '.join(cmd)}")
        
        try:
            # Run Recipe Robot
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120  # 2 minute timeout
            )
            
            # Parse output
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
        
        # Look for created recipe paths in output
        recipe_pattern = r"Created recipe: (.+\.recipe)"
        for match in re.finditer(recipe_pattern, stdout):
            recipe_path = match.group(1)
            created_recipes.append(recipe_path)
            
            # Extract recipe directory
            if recipe_dir is None:
                recipe_dir = str(Path(recipe_path).parent)
        
        # Check for errors
        if returncode != 0 or "ERROR" in stderr:
            error_msg = stderr.strip() if stderr else "Unknown error"
            return {
                "success": False,
                "error": error_msg,
                "created_recipes": created_recipes,
                "recipe_directory": recipe_dir
            }
        
        # Look for specific Recipe Robot messages
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
        
        # Create target app directory
        safe_app_name = re.sub(r'[<>:"/\\|?*]', '_', app_name)
        app_target_dir = target_dir / RECIPE_PREFIX / safe_app_name
        app_target_dir.mkdir(parents=True, exist_ok=True)
        
        moved_files = []
        
        # Move all recipe files
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
        
        # Step 1: Find download URL via Perplexity
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
        
        # Step 2: Create recipes with Recipe Robot
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
        
        # Step 3: Move recipes to target directory
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
        
        # Add last mile processing results
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
        
        # Update main recipe fields to reflect custom recipes
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
    """
    Process applications without AutoPkg recipes through last mile workflow.
    
    Args:
        apps_without_recipes: List of apps that need last mile processing
        output_dir: Where to save created recipes
        perplexity_api_key: API key for Perplexity
        logger: Logger instance
        
    Returns:
        Updated list with last mile processing results
    """
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
        
        # Rate limiting
        time.sleep(2)
    
    return results


# Integration with main AutoPkg finder
def enhance_autopkg_workflow(csv_path: str, perplexity_api_key: str = None):
    """
    Enhanced workflow that includes last mile processing.
    
    1. Run standard AutoPkg recipe finder
    2. Identify apps without recipes
    3. Process them through Perplexity + Recipe Robot
    4. Update CSV with all results
    """
    from autopkg_recipe_finder import main as autopkg_main, read_csv
    
    # Run standard AutoPkg finder first
    exit_code = autopkg_main(csv_path)
    
    if exit_code != 0:
        return exit_code
    
    # Read the results
    results_csv = Path(csv_path).with_name(Path(csv_path).stem + "-autopkg-recipes.csv")
    
    with open(results_csv, 'r', encoding='utf-8') as f:
        all_results = list(csv.DictReader(f))
    
    # Find apps without recipes
    apps_without_recipes = [
        app for app in all_results 
        if not app.get("found") or app.get("recipe_name") == "Not Found"
    ]
    
    if apps_without_recipes and perplexity_api_key:
        print(f"\nFound {len(apps_without_recipes)} apps without recipes. Starting last mile processing...")
        
        # Setup logging
        log_dir = Path(csv_path).parent / "logs"
        log_dir.mkdir(exist_ok=True)
        logger = logging.getLogger(__name__)
        
        # Process last mile apps
        output_dir = Path(csv_path).parent / "autopkg-recipes"
        last_mile_results = process_last_mile_apps(
            apps_without_recipes,
            output_dir,
            perplexity_api_key,
            logger
        )
        
        # Update results
        for original, updated in zip(apps_without_recipes, last_mile_results):
            # Find and update the original entry
            for i, result in enumerate(all_results):
                if result.get("Application") == original.get("Application"):
                    all_results[i] = updated
                    break
        
        # Write updated CSV
        enhanced_csv = results_csv.with_name(results_csv.stem + "-enhanced.csv")
        
        if all_results:
            keys = list(all_results[0].keys())
            with open(enhanced_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(all_results)
            
            print(f"\nEnhanced results written to: {enhanced_csv}")
    
    return 0


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python last_mile_handler.py <csv_path> [perplexity_api_key]")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    api_key = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("PERPLEXITY_API_KEY")
    
    if not api_key:
        print("Warning: No Perplexity API key provided. Set PERPLEXITY_API_KEY environment variable.")
    
    sys.exit(enhance_autopkg_workflow(csv_path, api_key))