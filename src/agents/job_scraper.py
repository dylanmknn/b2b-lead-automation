"""
LinkedIn Job Scraper for B2B Lead Pipeline

Scrapes LinkedIn job listings to find companies hiring decision-makers.
Companies hiring sales/growth roles = have budget = good B2B prospects.
"""

import os
from apify_client import ApifyClient
from typing import List, Dict
import time


class LinkedInJobScraper:
    def __init__(self):
        """Initialize Apify client"""
        api_key = os.getenv("APIFY_API_KEY")
        if not api_key:
            raise ValueError("Missing APIFY_API_KEY in environment")

        self.client = ApifyClient(api_key)

    def scrape_jobs(
        self,
        limit: int = 200,
        keywords: List[str] = None,
        location: str = "France",
        geo_id: str = "105015875",
    ) -> List[Dict]:
        """Scrape LinkedIn jobs for companies hiring sales/growth roles."""

        # Default B2B decision-maker keywords
        default_keywords = [
            "Sales",
            "Commercial",
            "Business Developer",
            "Growth",
            "Marketing",
            "RevOps",
            "Revenue Operations",
            "Demand Generation",
        ]

        # Use custom keywords if provided, otherwise use defaults
        search_keywords = keywords if keywords is not None else default_keywords

        print(f"  [INFO] Running {len(search_keywords)} separate searches...")
        print(f"  [INFO] Keywords: {', '.join(search_keywords)}")

        all_jobs = []

        for i, keyword in enumerate(search_keywords, 1):
            print(
                f"\n  [SEARCH] [{i}/{len(search_keywords)}] Searching: '{keyword}'..."
            )

            # Build LinkedIn search URL for this keyword
            linkedin_url = (
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={keyword.replace(' ', '+')}"
                f"&location={location.replace(' ', '+')}"
                f"&geoId={geo_id}"
                f"&f_TPR=r604800"  # Past 7 days (604800 seconds) - then filtered in pageFunction
                f"&start=0"
            )

            # Scrape this keyword
            jobs = self._scrape_single_keyword(linkedin_url, keyword)

            # Tag each job with its source keyword
            for job in jobs:
                job["source_keyword"] = keyword

            if jobs:
                print(f"    [OK] Found {len(jobs)} jobs for '{keyword}'")
                all_jobs.extend(jobs)
            else:
                print(f"    [WARN]  No jobs found for '{keyword}'")

            # Small delay between searches to be polite to LinkedIn
            if i < len(search_keywords):  # Don't sleep after last search
                time.sleep(2)

        print(f"\n  [STATS] Total jobs collected: {len(all_jobs)}")

        # ====================================================================
        # DEDUPLICATE by company name
        # ====================================================================
        seen_companies = set()
        unique_jobs = []

        for job in all_jobs:
            company_name = job.get("company_name", "")
            if company_name and company_name not in seen_companies:
                seen_companies.add(company_name)
                unique_jobs.append(job)

        print(f"  [OK] Unique companies: {len(unique_jobs)}")

        # Limit to requested amount
        unique_jobs = unique_jobs[:limit]

        print(f"  [OK] Scraped {len(unique_jobs)} unique companies (after limit)")

        return unique_jobs

    def _scrape_single_keyword(self, linkedin_url: str, keyword: str) -> List[Dict]:
        """Scrape a single LinkedIn search URL using Apify."""

        # Configure Apify Web Scraper with WORKING pageFunction
        run_input = {
            "startUrls": [{"url": linkedin_url}],
            "pageFunction": """
                async function pageFunction(context) {
                    const $ = context.jQuery;
                    const jobs = [];

                    $('.jobs-search__results-list li').each((index, element) => {
                        const $job = $(element);
                        const title = $job.find('.base-search-card__title').text().trim();
                        const company = $job.find('.base-search-card__subtitle').text().trim();
                        const location = $job.find('.job-search-card__location').text().trim();
                        const jobUrl = $job.find('.base-card__full-link').attr('href');

                        // Extract posted date text (e.g., "3 days ago", "2 weeks ago")
                        const postedText = $job.find('.job-search-card__listdate, .base-search-card__metadata time').text().trim();

                        // Parse age in days from posted text
                        let ageInDays = 0;
                        if (postedText) {
                            if (postedText.includes('hour') || postedText.includes('heure')) {
                                ageInDays = 0; // Same day
                            } else if (postedText.includes('day') || postedText.includes('jour')) {
                                const match = postedText.match(/(\\d+)/);
                                ageInDays = match ? parseInt(match[1]) : 1;
                            } else if (postedText.includes('week') || postedText.includes('semaine')) {
                                const match = postedText.match(/(\\d+)/);
                                ageInDays = match ? parseInt(match[1]) * 7 : 7;
                            } else if (postedText.includes('month') || postedText.includes('mois')) {
                                ageInDays = 30; // Treat as 30+ days
                            }
                        }

                        // ONLY include jobs posted in last 7 days
                        if (title && company && ageInDays <= 7) {
                            jobs.push({
                                job_title: title,
                                company_name: company,
                                location: location,
                                job_url: jobUrl,
                                posted_date: new Date().toISOString().split('T')[0],
                                posted_age_days: ageInDays
                            });
                        }
                    });

                    return jobs;
                }
            """,
            "maxPagesPerCrawl": 8,  # Scrape up to 8 pages per keyword
            "maxConcurrency": 1,
            "maxRequestRetries": 2,
        }

        try:
            # Run the scraper
            run = self.client.actor("apify/web-scraper").call(run_input=run_input)

            # Get results
            jobs = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                if isinstance(item, list):
                    jobs.extend(item)
                elif isinstance(item, dict):
                    jobs.append(item)

            return jobs

        except Exception as e:
            print(f"    [ERROR] Error scraping '{keyword}': {e}")
            return []
