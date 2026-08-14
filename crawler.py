import asyncio
import json
import logging
import sys
from typing import List, Dict, Any, Optional
import aiohttp
from bs4 import BeautifulSoup

# Configure Industrial-Grade Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("AsyncCrawlerEngine")


class ConfigurationLoader:
    """Handles parsing and validation of the external JSON configuration."""
    
    @staticmethod
    def load_config(config_path: str = "config.json") -> Dict[str, Any]:
        try:
            with open(config_path, "r", encoding="utf-8") as file:
                config = json.load(file)
                logger.info(f"Successfully loaded runtime parameters from {config_path}")
                return config
        except FileNotFoundError:
            logger.error(f"Configuration file '{config_path}' missing. Aborting.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON in config file: {e}")
            sys.exit(1)


class AsyncDataExtractor:
    """Core Engine responsible for HTTP fetches, DOM parsing, and JSON generation."""
    
    def __init__(self, config: Dict[str, Any]):
        self.target_urls: List[str] = config.get("target_urls", [])
        self.max_concurrent: int = config.get("max_concurrent_requests", 5)
        self.timeout: int = config.get("timeout_seconds", 10)
        self.output_file: str = config.get("output_file", "results.json")
        self.headers: Dict[str, str] = {"User-Agent": config.get("user_agent", "Python/AsyncCrawler")}
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Fetches raw HTML with timeout handling and concurrency limits."""
        async with self.semaphore:
            try:
                logger.info(f"Requesting target: {url}")
                async with session.get(url, headers=self.headers, timeout=self.timeout) as response:
                    if response.status == 200:
                        logger.info(f"HTTP 200 OK -> {url}")
                        return await response.text()
                    else:
                        logger.warning(f"HTTP {response.status} encountered for -> {url}")
                        return None
            except asyncio.TimeoutError:
                logger.error(f"Connection timed out for -> {url}")
            except aiohttp.ClientError as e:
                logger.error(f"Transport layer exception for {url}: {e}")
            except Exception as e:
                logger.error(f"Unexpected operational failure on {url}: {e}")
            return None

    def parse_dom(self, html_content: str, source_url: str) -> Dict[str, Any]:
        """Extracts key DOM elements from raw HTML markup."""
        soup = BeautifulSoup(html_content, "lxml")
        title = soup.title.string.strip() if soup.title and soup.title.string else "N/A"
        
        # Extract headings and hyperlinks
        headings = [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2'])[:5]]
        links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].startswith('http')][:10]

        return {
            "source_url": source_url,
            "page_title": title,
            "top_headings": headings,
            "extracted_links": links
        }

    async def process_url(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
        """Orchestrates fetch and parse sequence for a single URL."""
        html = await self.fetch_page(session, url)
        if html:
            return self.parse_dom(html, url)
        return None

    async def run_pipeline(self) -> None:
        """Initializes async session and manages task gathering."""
        connector = aiohttp.TCPConnector(limit_per_host=self.max_concurrent)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.process_url(session, url) for url in self.target_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            processed_data = [res for res in results if isinstance(res, dict)]
            
            self.export_results(processed_data)

    def export_results(self, data: List[Dict[str, Any]]) -> None:
        """Writes clean extracted datasets to disk."""
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            logger.info(f"Pipeline Execution Complete. Saved {len(data)} payloads to '{self.output_file}'.")
        except IOError as e:
            logger.error(f"Disk Write Failure: {e}")


def main():
    """Main application entry point."""
    logger.info("Initializing Advanced Async Crawler Pipeline...")
    config = ConfigurationLoader.load_config()
    
    extractor = AsyncDataExtractor(config)
    asyncio.run(extractor.run_pipeline())


if __name__ == "__main__":
    main()
