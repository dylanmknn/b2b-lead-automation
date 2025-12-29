"""
Supabase client for MilleMail prospects pipeline
Handles millemail_prospects table operations

This is separate from the main leads table to keep MilleMail cold email prospects
distinct from job application leads.
"""

import os
from supabase import create_client, Client
from typing import List, Dict
from datetime import datetime


class MilleMailSupabaseClient:
    def __init__(self):
        """Initialize Supabase client for millemail_prospects table"""
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in environment")

        self.client: Client = create_client(url, key)
        self.table_name = "millemail_prospects"

    def get_existing_contacts(self) -> set:
        """Get existing (domain, email) tuples to avoid duplicates."""
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
            print(f"  [ERROR] Error fetching existing contacts: {e}")
            return set()

    def get_last_contact_dates(self) -> Dict[str, str]:
        """Get last contact date per company for cooldown period."""
        try:
            # Only get companies that were actually contacted
            contacted_statuses = [
                "sent",
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

            # Get most recent contact per domain
            last_contacts = {}
            for row in result.data:
                domain = row.get("company_domain")
                if domain and domain not in last_contacts:
                    last_contacts[domain] = row["created_at"]

            return last_contacts

        except Exception as e:
            print(f"  [ERROR] Error fetching contact dates: {e}")
            return {}

    def insert_prospects(self, prospects: List[Dict]) -> int:
        """Insert prospects into database. Returns number inserted."""
        if not prospects:
            return 0

        try:
            # Add timestamps and status if not present
            for prospect in prospects:
                if "created_at" not in prospect:
                    prospect["created_at"] = datetime.now().isoformat()
                if "status" not in prospect:
                    prospect["status"] = "ready"

            result = self.client.table(self.table_name).insert(prospects).execute()

            return len(result.data)

        except Exception as e:
            print(f"  [ERROR] Error inserting prospects: {e}")
            return 0

    def get_ready_prospects(self, limit: int = 100) -> List[Dict]:
        """Get B2B prospects with status='ready' and email sequence."""
        try:
            result = (
                self.client.table(self.table_name)
                .select("*")
                .eq("status", "ready")
                .eq("company_type", "b2b")
                .not_.is_("email", "null")
                .not_.is_("email_1", "null")
                .limit(limit)
                .execute()
            )

            return result.data

        except Exception as e:
            print(f"  [ERROR] Error fetching ready prospects: {e}")
            return []

    def update_prospect_status(self, prospect_id: int, new_status: str) -> bool:
        """Update prospect status in database."""
        try:
            self.client.table(self.table_name).update({"status": new_status}).eq(
                "id", prospect_id
            ).execute()

            return True

        except Exception as e:
            print(f"  [ERROR] Error updating prospect {prospect_id}: {e}")
            return False

    def get_total_prospects_count(self) -> int:
        """Get total number of prospects in database."""
        try:
            result = (
                self.client.table(self.table_name).select("id", count="exact").execute()
            )

            return result.count if result.count else 0

        except Exception as e:
            print(f"  [ERROR] Error getting prospect count: {e}")
            return 0

    def get_prospects_by_status(self, status: str) -> List[Dict]:
        """Get all prospects with specific status."""
        try:
            result = (
                self.client.table(self.table_name)
                .select("*")
                .eq("status", status)
                .execute()
            )

            return result.data

        except Exception as e:
            print(f"  [ERROR] Error fetching prospects by status: {e}")
            return []
