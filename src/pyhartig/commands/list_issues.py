import sys
import tempfile
import json
import urllib.request
import urllib.parse
import re
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import List, Dict, Any

from pyhartig.commands.base import BaseCommand, logger
from pyhartig.commands.run import RunCommand


class ListIssuesCommand(BaseCommand):
    """
    Command to aggregate issues from multiple GitHub/GitLab repositories into a Knowledge Graph.
    1. Fetch issues from provided repository URLs.
    2. Merge the issues into temporary JSON files.
    3. Inject the temporary files into an RML mapping template.
    4. Delegate execution to the existing RunCommand.
    5. Clean up temporary files after execution.
    """
    name = "list-issues"
    help = "Aggregate issues from multiple GitHub/GitLab repositories into a Knowledge Graph"

    def configure_parser(self, parser: ArgumentParser) -> None:
        """
        Configure command-line arguments for the 'list-issues' command.
        :param parser: The argparse subparser for this command.
        :return: None
        """
        parser.add_argument(
            "repos",
            nargs="+",
            help="List of repository URLs (e.g., https://github.com/owner/repo or https://gitlab.com/owner/repo)"
        )
        parser.add_argument(
            "-m", "--mapping",
            required=True,
            help="Path to the RML mapping template. Must use placeholders {{GITHUB_SOURCE}} and/or {{GITLAB_SOURCE}}."
        )
        parser.add_argument(
            "-o", "--output",
            help="Path to output file (default: stdout)",
            default=None
        )
        parser.add_argument(
            "--explain",
            action="store_true",
            help="Print the algebraic execution plan structure instead of running it"
        )

    def _convert_repo_url_to_api(self, url: str) -> str:
        """
        Heuristic to convert a human-readable repo URL to an Issue API endpoint.
        :param url: Repository URL
        :return: API endpoint URL for fetching issues
        """
        url = url.rstrip("/")

        # GitHub Logic
        # Input: https://github.com/Owner/Repo
        # API:   https://api.github.com/repos/Owner/Repo/issues
        if "github.com" in url:
            match = re.search(r"github\.com/([^/]+)/([^/]+)", url)
            if match:
                owner, repo = match.groups()
                return f"https://api.github.com/repos/{owner}/{repo}/issues?state=all"

        # GitLab Logic
        # Input: https://gitlab.com/Owner/Repo
        # API:   https://gitlab.com/api/v4/projects/Owner%2FRepo/issues
        elif "gitlab" in url:
            # Extract domain and path
            parsed = urllib.parse.urlparse(url)
            domain = parsed.netloc  # e.g., gitlab.com or gitlab.univ-nantes.fr
            path = parsed.path.strip("/")  # e.g., Owner/Repo

            # URL Encode the path (GitLab API requirement for project IDs)
            encoded_path = urllib.parse.quote(path, safe="")
            return f"https://{domain}/api/v4/projects/{encoded_path}/issues"

        logger.warning(f"Could not automatically determine API endpoint for {url}. Using as-is.")
        return url

    def _fetch_and_merge(self, urls: List[str]) -> str:
        """
        Fetches JSON data from multiple URLs and merges them into a single temporary JSON file.
        Returns the path to the temporary file.
        """
        merged_data = []

        for url in urls:
            api_url = self._convert_repo_url_to_api(url)
            logger.info(f"Fetching issues from: {api_url}")

            try:
                # User-Agent is required by GitHub API
                req = urllib.request.Request(
                    api_url,
                    headers={'User-Agent': 'PyHartig-CLI'}
                )
                with urllib.request.urlopen(req) as response:
                    data = json.load(response)
                    if isinstance(data, list):
                        merged_data.extend(data)
                    else:
                        # Handle cases where API returns a single object or error dict
                        logger.warning(f"Unexpected response format from {api_url}, expecting a list.")
            except Exception as e:
                logger.error(f"Failed to fetch data from {api_url}: {e}")
                # We continue to try other repos instead of crashing

        # Write merged data to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode='w', encoding='utf-8') as tmp_file:
            json.dump(merged_data, tmp_file, ensure_ascii=False)
            return tmp_file.name

    def execute(self, args: Namespace) -> None:
        github_repos = [url for url in args.repos if "github.com" in url]
        gitlab_repos = [url for url in args.repos if "gitlab" in url]

        if not github_repos and not gitlab_repos:
            logger.error("No valid GitHub or GitLab URLs found in arguments.")
            sys.exit(1)

        temp_files_to_clean = []

        try:
            # 1. Fetch and Merge Data
            github_source_path = None
            if github_repos:
                logger.info(f"Detected {len(github_repos)} GitHub repositories.")
                github_source_path = self._fetch_and_merge(github_repos)
                temp_files_to_clean.append(github_source_path)

            gitlab_source_path = None
            if gitlab_repos:
                logger.info(f"Detected {len(gitlab_repos)} GitLab repositories.")
                gitlab_source_path = self._fetch_and_merge(gitlab_repos)
                temp_files_to_clean.append(gitlab_source_path)

            # 2. Inject into Mapping Template
            mapping_path = Path(args.mapping)
            if not mapping_path.exists():
                logger.error(f"Mapping template not found: {mapping_path}")
                sys.exit(1)

            with open(mapping_path, 'r', encoding='utf-8') as f:
                mapping_content = f.read()

            # Replace placeholders
            # If no data for a source, we should ideally remove that part of the mapping,
            # but for now we provide an empty dummy file to avoid crashes or just leave it.
            # Here we replace with the generated path if it exists.

            if github_source_path:
                mapping_content = mapping_content.replace("{{GITHUB_SOURCE}}", github_source_path)

            if gitlab_source_path:
                mapping_content = mapping_content.replace("{{GITLAB_SOURCE}}", gitlab_source_path)

            # Create final temporary mapping
            with tempfile.NamedTemporaryFile(delete=False, suffix=".ttl", mode='w', encoding='utf-8') as tmp_map:
                tmp_map.write(mapping_content)
                final_mapping_path = tmp_map.name
                temp_files_to_clean.append(final_mapping_path)

            logger.info("Executing pipeline with aggregated data...")

            # 3. Delegate to RunCommand
            run_cmd = RunCommand()
            # Construct a namespace object compatible with RunCommand args
            run_args = Namespace(
                mapping=final_mapping_path,
                output=args.output,
                explain=args.explain
            )
            run_cmd.execute(run_args)

        finally:
            # 4. Cleanup
            import os
            for f in temp_files_to_clean:
                if f and os.path.exists(f):
                    try:
                        os.remove(f)
                    except OSError:
                        pass