"""
LinkedIn Profile Scraper for MilleMail Lead Pipeline

Uses Apify supreme_coder/linkedin-profile-scraper actor
- No cookies required
- $3 per 1000 profiles (pay per use from your $39/mo)
- Accepts LinkedIn search URLs

USE CASE: Scrape VP Sales, decision makers, new hires etc.
- Better than job postings: you get the BUYER, not HR
- Apply same filters as job scraper (BIG_CORPORATES, company size, B2C)
- Hunter credits only spent on filtered leads

WORKFLOW:
1. You search LinkedIn manually (VP Sales + France + filters)
2. Copy the search URL
3. Run this scraper with that URL
4. Profiles scraped → filtered → Hunter enriched → Supabase
"""

import os
from apify_client import ApifyClient
from typing import List, Dict

# Big corporates to filter out (same as main.py)
BIG_CORPORATES_FALLBACK = {
    "volkswagen",
    "coca-cola",
    "renault",
    "carrefour",
    "amazon",
    "apple",
    "google",
    "microsoft",
    "facebook",
    "meta",
    "ibm",
    "oracle",
    "sap",
    "salesforce",
    "auchan",
    "leclerc",
    "intermarché",
    "système u",
    "casino",
    "monoprix",
    "peugeot",
    "citroën",
    "nissan",
    "toyota",
    "bmw",
    "mercedes",
    "audi",
    "total",
    "engie",
    "edf",
    "orange",
    "bouygues",
    "vinci",
    "veolia",
    "lvmh",
    "l'oréal",
    "danone",
    "lactalis",
    "pernod ricard",
    "schneider electric",
    "airbus",
    "thales",
    "safran",
    "michelin",
    "saint-gobain",
    "legrand",
    "bnp paribas",
    "société générale",
    "crédit agricole",
    "axa",
    "allianz",
    "adidas",
    "nike",
    "puma",
    "decathlon",
    "fnac",
    "darty",
    # Additional from your scrape results
    "capgemini",
    "hewlett packard",
    "hpe",
    "air france",
    "worldline",
    "slack",
    "jll",
    "diageo",
    "thales",
    "ibm",
    "sap",
    "stripe",
    "servicenow",
    "snowflake",
    "uipath",
    "cloudera",
}


class LinkedInProfileScraper:
    def __init__(self):
        """Initialize Apify client"""
        api_key = os.getenv("APIFY_API_KEY")
        if not api_key:
            raise ValueError("Missing APIFY_API_KEY in environment")

        self.client = ApifyClient(api_key)
        # supreme_coder actor - no cookies, $3/1000 profiles
        self.actor_id = "supreme_coder/linkedin-profile-scraper"

    def scrape_profiles(self, search_url: str, max_profiles: int = 500) -> List[Dict]:
        """Scrape LinkedIn profiles from search URL using Apify."""
        print(f"\n{'='*60}")
        print("[SEARCH] LINKEDIN PROFILE SCRAPER")
        print(f"{'='*60}")
        print(f"  [URL] Search URL: {search_url[:80]}...")
        print(f"  [STATS] Max profiles: {max_profiles}")

        run_input = {
            "urls": [search_url],
            "maxProfiles": max_profiles,
        }

        try:
            print("\n  [WAIT] Starting Apify actor...")
            run = self.client.actor(self.actor_id).call(run_input=run_input)

            # Collect results
            profiles = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                # Skip failed profiles
                if item.get("error"):
                    continue
                profiles.append(item)

            print(f"  [OK] Scraped {len(profiles)} profiles")
            return profiles

        except Exception as e:
            print(f"  [ERROR] Error scraping profiles: {e}")
            return []

    def is_big_corporate(self, company_name: str) -> bool:
        """Check if company is a known big corporate"""
        if not company_name:
            return False
        company_lower = company_name.lower()
        return any(corp in company_lower for corp in BIG_CORPORATES_FALLBACK)

    def filter_profiles(self, profiles: List[Dict], email_enricher=None) -> List[Dict]:
        """Filter profiles to remove big corporates using list and Hunter API."""
        print(f"\n{'='*60}")
        print("[SEARCH] FILTERING PROFILES")
        print(f"{'='*60}")
        print(f"  [STATS] Input: {len(profiles)} profiles")

        filtered = []
        stats = {
            "big_corporate_list": 0,
            "big_corporate_api": 0,
            "no_company": 0,
            "passed": 0,
        }

        for profile in profiles:
            company_name = profile.get("companyName") or profile.get(
                "currentCompany", {}
            ).get("name", "")
            first_name = profile.get("firstName", "")
            last_name = profile.get("lastName", "")

            # Skip if no company
            if not company_name:
                stats["no_company"] += 1
                continue

            # Filter 1: Check BIG_CORPORATES list (FREE)
            if self.is_big_corporate(company_name):
                stats["big_corporate_list"] += 1
                print(
                    f"    [ERROR] {first_name} {last_name} @ {company_name} - BIG CORPORATE (list)"
                )
                continue

            # Filter 2: Check company size via Hunter API (FREE - doesn't use credits)
            if email_enricher:
                # First get domain from company name
                domain = email_enricher.find_company_domain(company_name)
                if domain:
                    size_info = email_enricher.get_company_size(domain)
                    if size_info.get("is_large_company"):
                        stats["big_corporate_api"] += 1
                        print(
                            f"    [ERROR] {first_name} {last_name} @ {company_name} - BIG CORPORATE ({size_info.get('employee_range')})"
                        )
                        continue

            # Profile passed all filters
            stats["passed"] += 1
            filtered.append(profile)

        print("\n  [STATS] Filter Results:")
        print(f"    [ERROR] Big corporate (list): {stats['big_corporate_list']}")
        print(f"    [ERROR] Big corporate (API): {stats['big_corporate_api']}")
        print(f"    [WARN]  No company name: {stats['no_company']}")
        print(f"    [OK] Passed filters: {stats['passed']}")

        return filtered

    def transform_for_pipeline(self, profiles: List[Dict]) -> List[Dict]:
        """Transform Apify profile data to pipeline format for Hunter enrichment."""
        leads = []

        for profile in profiles:
            company_name = profile.get("companyName") or profile.get(
                "currentCompany", {}
            ).get("name", "")

            lead = {
                "company_name": company_name,
                "company_domain": None,  # Will be enriched by Hunter
                "job_title": profile.get("headline") or profile.get("jobTitle", ""),
                "job_url": profile.get("profileUrl")
                or profile.get("publicIdentifier", ""),
                "first_name": profile.get("firstName", ""),
                "last_name": profile.get("lastName", ""),
                "linkedin_url": profile.get("profileUrl", ""),
                "location": profile.get("geoLocationName")
                or profile.get("geoCountryName", ""),
                "source": "linkedin_profile_scraper",
                "source_keyword": "VP Sales",  # Can be customized per search
            }

            leads.append(lead)

        return leads
