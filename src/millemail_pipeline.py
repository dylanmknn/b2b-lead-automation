#!/usr/bin/env python3
"""
MilleMail Lead Pipeline - Decision Maker Scraper

Scrapes LinkedIn profiles for decision makers at B2B companies:
- VP Sales, Head of Growth, CRO, CMO, etc.
- Filters out B2C companies
- Filters out big companies (500+ employees)
- Enriches emails with Hunter
- Saves to millemail_prospects table (separate from job application leads)

Usage:
    python3 src/millemail_pipeline.py --count 500
"""
import argparse
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from agents.job_scraper import LinkedInJobScraper
from agents.email_enricher import EmailEnricher
from agents.personalizer import Personalizer
from utils.millemail_supabase import MilleMailSupabaseClient


# 10 Decision-maker JOB TITLES for MilleMail prospects
# These are JOB POSTINGS (companies hiring these roles = have budget!)
DECISION_MAKER_KEYWORDS = [
    "VP Sales",
    "Head of Growth",
    "CRO",  # Chief Revenue Officer
    "CMO",  # Chief Marketing Officer
    "VP Marketing",
    "Head of Sales",
    "Director of Sales",
    "Revenue Operations",
    "Head of RevOps",
    "Demand Generation Manager",
]


def filter_duplicates(profiles: List[Dict], existing_contacts: set) -> List[Dict]:
    """
    Filter out profiles that already exist in database

    Args:
        profiles: Raw profiles from scraper
        existing_contacts: Set of (domain, email) tuples already in DB

    Returns:
        Filtered list of new profiles
    """
    filtered = []

    for profile in profiles:
        company_domain = profile.get("company_domain")
        email = profile.get("email")

        # Skip if we already have this contact
        if company_domain and email and (company_domain, email) in existing_contacts:
            continue

        filtered.append(profile)

    return filtered


def filter_cooldown(
    profiles: List[Dict], last_contact_dates: Dict[str, str], cooldown_days: int = 90
) -> List[Dict]:
    """
    Filter out companies contacted within cooldown period

    Args:
        profiles: Profiles to filter
        last_contact_dates: Dict of domain -> last contact date
        cooldown_days: Days to wait before re-contacting (default: 90)

    Returns:
        Filtered list of profiles not in cooldown
    """
    filtered = []
    from datetime import timezone

    now = datetime.now(timezone.utc)
    cooldown_threshold = now - timedelta(days=cooldown_days)

    for profile in profiles:
        company_domain = profile.get("company_domain")

        if not company_domain:
            filtered.append(profile)
            continue

        # Check if company was contacted recently
        last_contact = last_contact_dates.get(company_domain)
        if last_contact:
            last_contact_dt = datetime.fromisoformat(
                last_contact.replace("Z", "+00:00")
            )
            if last_contact_dt > cooldown_threshold:
                # Still in cooldown period
                continue

        filtered.append(profile)

    return filtered


def main():
    parser = argparse.ArgumentParser(
        description="MilleMail Pipeline - Scrape decision makers for cold email"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=500,
        help="Max profiles to scrape per keyword (default: 500)",
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=DECISION_MAKER_KEYWORDS,
        help="Keywords to scrape (default: 10 decision-maker keywords)",
    )
    parser.add_argument(
        "--location",
        type=str,
        default="France",
        help="Location filter (default: France)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 80)
    print("MILLEMAIL PIPELINE - DECISION MAKER SCRAPER")
    print("=" * 80)
    print(f"  Keywords: {len(args.keywords)} decision-maker titles")
    print(f"  Max per keyword: {args.count} profiles")
    print(f"  Location: {args.location}")
    print(
        "  Filters: B2C companies, Big companies (500+), Duplicates, Cooldown (90 days)"
    )

    # Initialize clients
    print("\n1. Initializing clients...")
    try:
        scraper = LinkedInJobScraper()
        enricher = EmailEnricher()
        personalizer = Personalizer()
        supabase = MilleMailSupabaseClient()
        print("  [OK] All clients initialized")
    except Exception as e:
        print(f"  [ERROR] Failed to initialize: {e}")
        sys.exit(1)

    # Get existing contacts for deduplication
    print("\n2. Loading existing contacts...")
    existing_contacts = supabase.get_existing_contacts()
    last_contact_dates = supabase.get_last_contact_dates()
    print(f"  [OK] {len(existing_contacts)} existing contacts")
    print(f"  [OK] {len(last_contact_dates)} companies in cooldown")

    # Scrape JOB LISTINGS for decision-maker roles
    # Companies hiring VP Sales/CRO = have budget = perfect for MilleMail!
    print(
        f"\n3. Scraping job listings for {len(args.keywords)} decision-maker roles..."
    )

    all_jobs = []

    for i, keyword in enumerate(args.keywords, 1):
        print(f"\n  [SEARCH] [{i}/{len(args.keywords)}] Searching: '{keyword}'...")

        linkedin_url = (
            f"https://www.linkedin.com/jobs/search/"
            f"?keywords={keyword.replace(' ', '+')}"
            f"&location=France"
            f"&geoId=105015875"
            f"&start=0"
        )

        # Use the job scraper's internal method directly
        jobs = scraper._scrape_single_keyword(linkedin_url, keyword)

        # Tag with source keyword
        for job in jobs:
            job["source_keyword"] = keyword

        if jobs:
            print(f"    [OK] Found {len(jobs)} jobs for '{keyword}'")
            all_jobs.extend(jobs)
        else:
            print(f"    [WARN]  No jobs found for '{keyword}'")

        # Delay between keywords
        if i < len(args.keywords):
            time.sleep(2)

    # Deduplicate by company
    print("\n4. Deduplicating companies...")
    seen_companies = set()
    unique_jobs = []
    for job in all_jobs:
        company_name = job.get("company_name", "")
        if company_name and company_name not in seen_companies:
            seen_companies.add(company_name)
            unique_jobs.append(job)

    print(f"  [OK] {len(unique_jobs)} unique companies hiring decision-makers")

    if not unique_jobs:
        print("\n[ERROR] No job listings found. Exiting.")
        sys.exit(0)

    # Transform jobs to leads format
    print("\n5. Extracting companies from job listings...")
    leads = []
    for job in unique_jobs[: args.count * len(args.keywords)]:  # Apply limit
        leads.append(
            {
                "company_name": job.get("company_name"),
                "job_title": job.get("job_title"),
                "job_url": job.get("job_url"),
                "location": job.get("location"),
                "source_keyword": job.get("source_keyword"),
                "posted_date": job.get("posted_date"),
            }
        )
    print(f"  [OK] {len(leads)} companies ready for enrichment")

    # Enrich emails with Hunter
    print("\n7. Enriching emails with Hunter...")
    enriched_leads = []
    b2c_skipped = 0
    no_email_found = 0

    for lead in leads:
        company_name = lead.get("company_name")

        # Find domain
        domain = enricher.find_company_domain(company_name)
        if not domain:
            no_email_found += 1
            continue

        lead["company_domain"] = domain

        # Get company size and info
        size_info = enricher.get_company_size(domain)
        industry = size_info.get("industry")
        description = size_info.get("description")

        # Check if B2C (MilleMail is B2B only)
        b2c_check = enricher.is_b2c_company(company_name, industry, description)
        if b2c_check.get("is_b2c"):
            b2c_skipped += 1
            print(f"  [WARN]  Skipped {company_name} - B2C company")
            continue

        lead["company_type"] = "b2b"

        # Find decision maker email
        contact = enricher.find_decision_maker(domain)
        if not contact:
            no_email_found += 1
            continue

        # Add contact info to lead
        lead["email"] = contact["email"]
        lead["first_name"] = contact.get("first_name", lead.get("first_name", ""))
        lead["last_name"] = contact.get("last_name", lead.get("last_name", ""))
        lead["title"] = contact.get("title", lead.get("job_title", ""))

        # Verify email
        verification = enricher.verify_email(contact["email"])
        if not verification.get("verified"):
            print(f"  [WARN]  Invalid email for {company_name}: {contact['email']}")
            continue

        lead["verification_status"] = verification.get("status")
        lead["verification_score"] = verification.get("score")

        enriched_leads.append(lead)

    print("\n  Results:")
    print(f"    [OK] Enriched: {len(enriched_leads)} leads")
    print(f"    [WARN]  B2C skipped: {b2c_skipped}")
    print(f"    [ERROR] No email found: {no_email_found}")

    if not enriched_leads:
        print("\n[ERROR] No leads enriched. Exiting.")
        sys.exit(0)

    # Filter duplicates
    print("\n8. Filtering duplicates...")
    new_leads = filter_duplicates(enriched_leads, existing_contacts)
    print(
        f"  [OK] {len(new_leads)} new leads (filtered {len(enriched_leads) - len(new_leads)} duplicates)"
    )

    # Filter cooldown
    print("\n9. Filtering cooldown...")
    ready_leads = filter_cooldown(new_leads, last_contact_dates, cooldown_days=90)
    print(
        f"  [OK] {len(ready_leads)} leads ready (filtered {len(new_leads) - len(ready_leads)} in cooldown)"
    )

    if not ready_leads:
        print(
            "\n[WARN]  No new leads after deduplication. All leads already contacted."
        )
        sys.exit(0)

    # Generate MilleMail-specific email sequences
    print(f"\n10. Generating MilleMail email sequences for {len(ready_leads)} leads...")
    prospects_with_sequences = []

    for lead in ready_leads:
        print(f"  Generating sequence for {lead.get('company_name')}...")
        sequence = personalizer.generate_millemail_sequence(lead)

        # Add sequence fields to lead
        lead["subject_line"] = sequence.get("subject_line")
        lead["email_1"] = sequence.get("email_1")
        lead["email_1_ps"] = sequence.get("email_1_ps")
        lead["email_2"] = sequence.get("email_2")
        lead["email_3"] = sequence.get("email_3")

        prospects_with_sequences.append(lead)

    print(f"  [OK] Generated {len(prospects_with_sequences)} email sequences")

    # Save to millemail_prospects table
    print(f"\n11. Saving {len(prospects_with_sequences)} prospects to Supabase...")
    inserted = supabase.insert_prospects(prospects_with_sequences)
    print(f"  [OK] Saved {inserted} prospects to millemail_prospects table")

    # Summary
    print("\n" + "=" * 80)
    print("MILLEMAIL PIPELINE COMPLETE")
    print("=" * 80)
    print(f"  [STATS] Total jobs scraped: {len(all_jobs)} job listings")
    print(f"  [COMPANIES] Unique companies: {len(unique_jobs)}")
    print(f"  [OK] Enriched with email: {len(enriched_leads)}")
    print(
        f"  [FILTERED] Duplicates/cooldown filtered: {len(enriched_leads) - len(ready_leads)}"
    )
    print(f"  [SAVED] Saved to millemail_prospects: {inserted}")
    print("\n  Next: Run send_millemail_to_smartlead.py to send to campaign")
    print()


if __name__ == "__main__":
    main()
