"""Campaign Manager - Smartlead Integration

Handles adding verified leads to Smartlead campaigns via API.
Maps lead fields to Smartlead custom variables for email sequences.
"""

import requests
from typing import List, Dict
from config.settings import settings


class CampaignManager:
    def __init__(self):
        """Initialize Smartlead campaign manager"""
        self.api_key = settings.SMARTLEAD_API_KEY
        self.campaign_id = settings.SMARTLEAD_CAMPAIGN_ID
        self.base_url = "https://server.smartlead.ai/api/v1"

        if not self.api_key:
            raise ValueError("SMARTLEAD_API_KEY not found in environment")
        if not self.campaign_id:
            raise ValueError("SMARTLEAD_CAMPAIGN_ID not found in environment")

    def add_leads_to_campaign(
        self, leads: List[Dict], ignore_duplicates: bool = True
    ) -> Dict:
        """Add leads to Smartlead campaign. Returns upload statistics."""
        if not leads:
            return {"total": 0, "added": 0, "duplicates": 0, "invalid": 0}

        # Build endpoint URL with API key
        endpoint = f"{self.base_url}/campaigns/{self.campaign_id}/leads"
        params = {"api_key": self.api_key}

        # Transform leads to Smartlead format
        smartlead_leads = []
        for lead in leads:
            smartlead_lead = self._transform_lead(lead)
            smartlead_leads.append(smartlead_lead)

        # Smartlead accepts max 100 leads per request
        # Split into batches if needed
        batch_size = 100
        total_stats = {"total": 0, "added": 0, "duplicates": 0, "invalid": 0}

        for i in range(0, len(smartlead_leads), batch_size):
            batch = smartlead_leads[i : i + batch_size]

            # Build request payload
            payload = {
                "lead_list": batch,
                "settings": {
                    "ignore_global_block_list": False,  # Respect Smartlead's block list
                    "ignore_unsubscribe_list": False,  # Respect unsubscribes
                    "ignore_duplicate_leads_in_other_campaign": ignore_duplicates,
                },
            }

            try:
                # Send to Smartlead
                response = requests.post(
                    endpoint, params=params, json=payload, timeout=30
                )

                response.raise_for_status()

                # Parse response statistics
                result = response.json()

                # Aggregate stats from Smartlead response
                total_stats["total"] += len(batch)

                # Smartlead API fields:
                # - total_leads = new leads added
                # - already_added_to_campaign = duplicates
                # - invalid_email_count = invalid emails
                leads_added = result.get("total_leads", 0)
                total_stats["added"] += leads_added

                duplicates = result.get("already_added_to_campaign", 0)
                total_stats["duplicates"] += duplicates

                invalid = result.get("invalid_email_count", 0)
                total_stats["invalid"] += invalid

                print(f"  [OK] Batch {i//batch_size + 1}: {leads_added} added")

            except requests.exceptions.HTTPError as e:
                print(f"  [ERROR] HTTP error adding batch {i//batch_size + 1}: {e}")
                print(f"  Response: {e.response.text}")
                # Continue with next batch

            except Exception as e:
                print(f"  [ERROR] Error adding batch {i//batch_size + 1}: {e}")
                # Continue with next batch

        return total_stats

    def _transform_lead(self, lead: Dict) -> Dict:
        """Transform Supabase lead to Smartlead format with custom fields."""
        smartlead_lead = {
            "email": lead.get("email", ""),
            "first_name": lead.get("first_name", ""),
            "last_name": lead.get("last_name", ""),
            "company_name": lead.get("company_name", ""),
            "custom_fields": {},
        }

        # Add custom fields for email sequence
        # These will be used in Smartlead's email templates with {{subject_line}}, {{email_1}}, etc.
        custom_field_mappings = {
            "subject_line": "subject_line",
            "email_1": "email_1",
            "email_1_ps": "email_1_ps",
            "email_2": "email_2",
            "email_3": "email_3",
        }

        for lead_field, smartlead_field in custom_field_mappings.items():
            value = lead.get(lead_field)
            if value:
                smartlead_lead["custom_fields"][smartlead_field] = value

        return smartlead_lead

    def get_campaign_stats(self) -> Dict:
        """Get campaign statistics from Smartlead API."""
        endpoint = f"{self.base_url}/campaigns/{self.campaign_id}"
        params = {"api_key": self.api_key}

        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            print(f"  [ERROR] Error fetching campaign stats: {e}")
            return {}
