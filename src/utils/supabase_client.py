"""
Supabase client for B2B lead pipeline.
Handles database operations for lead storage and deduplication.
"""

import os
from supabase import create_client, Client
from typing import List, Dict
from datetime import datetime


class SupabaseClient:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in environment")

        self.client: Client = create_client(url, key)
        self.table_name = "leads"

    def check_domain_exists(self, company_domain: str) -> bool:
        """Check if a company domain already exists in the database."""
        try:
            result = (
                self.client.table(self.table_name)
                .select("company_domain")
                .eq("company_domain", company_domain)
                .limit(1)
                .execute()
            )

            return len(result.data) > 0

        except Exception as e:
            print(f"  [WARN] Error checking domain {company_domain}: {e}")
            return False

    def insert_leads(self, leads: List[Dict]) -> int:
        """Insert multiple leads into database."""
        if not leads:
            return 0

        try:
            for lead in leads:
                if "created_at" not in lead:
                    lead["created_at"] = datetime.now().isoformat()
                if "status" not in lead:
                    lead["status"] = "ready"

            result = self.client.table(self.table_name).insert(leads).execute()

            return len(result.data)

        except Exception as e:
            print(f"  [ERROR] Error inserting leads: {e}")
            return 0

    def get_ready_leads(self, limit: int = 100) -> List[Dict]:
        """Get leads with status='ready' for campaign sending."""
        try:
            result = (
                self.client.table(self.table_name)
                .select("*")
                .eq("status", "ready")
                .limit(limit)
                .execute()
            )

            return result.data

        except Exception as e:
            print(f"  [ERROR] Error fetching ready leads: {e}")
            return []

    def update_lead_status(self, lead_id: int, new_status: str) -> bool:
        """Update a lead's status."""
        try:
            self.client.table(self.table_name).update({"status": new_status}).eq(
                "id", lead_id
            ).execute()

            return True

        except Exception as e:
            print(f"  [ERROR] Error updating lead {lead_id}: {e}")
            return False

    def get_total_leads_count(self) -> int:
        """Get total number of leads in database."""
        try:
            result = (
                self.client.table(self.table_name).select("id", count="exact").execute()
            )

            return result.count if result.count else 0

        except Exception as e:
            print(f"  [ERROR] Error getting lead count: {e}")
            return 0

    def get_domains_in_database(self) -> List[str]:
        """Get all unique company domains already in database."""
        try:
            result = (
                self.client.table(self.table_name).select("company_domain").execute()
            )

            domains = set()
            for row in result.data:
                if row.get("company_domain"):
                    domains.add(row["company_domain"])

            return list(domains)

        except Exception as e:
            print(f"  [ERROR] Error fetching domains: {e}")
            return []

    def get_existing_lead_contacts(self) -> set:
        """Get (domain, email) tuples to avoid duplicate contacts."""
        try:
            result = (
                self.client.table(self.table_name)
                .select("company_domain, email")
                .execute()
            )

            return {
                (row["company_domain"], row["email"])
                for row in result.data
                if row.get("company_domain") and row.get("email")
            }

        except Exception as e:
            print(f"  [ERROR] Error fetching lead contacts: {e}")
            return set()

    def get_last_contact_dates(self) -> Dict[str, str]:
        """Get last contact date per company for cooldown period."""
        try:
            contacted_statuses = [
                "contacted",
                "replied",
                "interested",
                "bounced",
                "not_interested",
            ]

            result = (
                self.client.table(self.table_name)
                .select("company_domain, created_at")
                .in_("status", contacted_statuses)
                .order("created_at", desc=True)
                .execute()
            )

            last_contacts = {}
            for row in result.data:
                domain = row.get("company_domain")
                if domain and domain not in last_contacts:
                    last_contacts[domain] = row["created_at"]

            return last_contacts

        except Exception as e:
            print(f"  [ERROR] Error fetching contact dates: {e}")
            return {}
