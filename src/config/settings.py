"""Configuration settings"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # API Keys
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
    ANYMAILFINDER_API_KEY = os.getenv("ANYMAILFINDER_API_KEY")
    HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")
    APIFY_API_KEY = os.getenv("APIFY_API_KEY")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SMARTLEAD_API_KEY = os.getenv("SMARTLEAD_API_KEY")
    SMARTLEAD_CAMPAIGN_ID = os.getenv("SMARTLEAD_CAMPAIGN_ID")

    # Lead generation settings
    DAILY_LEAD_LIMIT = 200
    TARGET_COUNTRY = "France"
    TARGET_JOB_TITLES = [
        "Commercial",
        "Business Developer",
        "SDR",
        "Sales Development Representative",
        "Chargé de développement commercial",
    ]

    # Apify actor IDs
    APIFY_LINKEDIN_SCRAPER = "curious_coder/linkedin-jobs-search-scraper"


settings = Settings()
