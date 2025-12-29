#!/usr/bin/env python3
"""Send verified B2B leads to Smartlead campaign."""
import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from agents.campaign_manager import CampaignManager
from utils.supabase_client import SupabaseClient


def get_leads_for_campaign(supabase: SupabaseClient, limit: int) -> list:
    """Get verified B2B leads with email sequence ready to send."""
    try:
        result = (
            supabase.client.table(supabase.table_name)
            .select("*")
            .eq("status", "ready")
            .eq("company_type", "b2b")
            .not_.is_("email_1", "null")
            .limit(limit)
            .execute()
        )

        return result.data

    except Exception as e:
        print(f"[ERROR] Error fetching leads: {e}")
        return []


def update_lead_status_to_sent(supabase: SupabaseClient, lead_ids: list):
    """Update lead status to 'sent' after adding to Smartlead."""
    if not lead_ids:
        return

    try:
        supabase.client.table(supabase.table_name).update({"status": "sent"}).in_(
            "id", lead_ids
        ).execute()

        print(f"  [OK] Updated {len(lead_ids)} leads to status='sent'")

    except Exception as e:
        print(f"  [ERROR] Error updating lead statuses: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Send verified B2B leads to Smartlead campaign"
    )
    parser.add_argument(
        "--count", type=int, default=50, help="Number of leads to send (default: 50)"
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("[EMAIL] SMARTLEAD CAMPAIGN SENDER")
    print("=" * 60)

    # Initialize clients
    print("\n1. Initializing clients...")
    try:
        supabase = SupabaseClient()
        campaign_manager = CampaignManager()
        print("  [OK] Connected to Supabase")
        print(
            f"  [OK] Connected to Smartlead (Campaign ID: {campaign_manager.campaign_id})"
        )
    except Exception as e:
        print(f"  [ERROR] Failed to initialize: {e}")
        sys.exit(1)

    # Fetch leads
    print(f"\n2. Fetching {args.count} verified B2B leads...")
    print("  Filters: status='ready', company_type='b2b', email_1 IS NOT NULL")
    leads = get_leads_for_campaign(supabase, args.count)

    if not leads:
        print("  [WARN]  No leads found matching criteria")
        print("\n  Make sure leads have:")
        print("    - status = 'ready'")
        print("    - company_type = 'b2b'")
        print("    - email_1 field populated (full sequence generated)")
        sys.exit(0)

    print(f"  [OK] Found {len(leads)} leads ready to send")

    # Show sample lead
    if leads:
        sample = leads[0]
        print("\n  Sample lead:")
        print(f"    Company: {sample.get('company_name')}")
        print(f"    Contact: {sample.get('first_name')} {sample.get('last_name')}")
        print(f"    Email: {sample.get('email')}")
        print(f"    Has sequence: {'[OK]' if sample.get('email_1') else '[ERROR]'}")

    # Confirm before sending
    print(f"\n[WARN]  About to send {len(leads)} leads to Smartlead campaign")
    response = input("  Continue? (yes/no): ").lower().strip()

    if response not in ["yes", "y"]:
        print("\n  [ERROR] Cancelled by user")
        sys.exit(0)

    # Send to Smartlead
    print(f"\n3. Adding {len(leads)} leads to Smartlead campaign...")
    stats = campaign_manager.add_leads_to_campaign(leads)

    print("\n  Results:")
    print(f"    Total processed: {stats['total']}")
    print(f"    [OK] Successfully added: {stats['added']}")
    print(f"    [WARN]  Duplicates (skipped): {stats['duplicates']}")
    print(f"    [ERROR] Invalid emails: {stats['invalid']}")

    # Update Supabase status for successfully added leads
    if stats["added"] > 0:
        print("\n4. Updating lead statuses to 'sent'...")
        # Update only the leads that were successfully added
        # (those not marked as duplicates or invalid)
        successfully_added_ids = [lead["id"] for lead in leads[: stats["added"]]]
        update_lead_status_to_sent(supabase, successfully_added_ids)

    print("\n" + "=" * 60)
    print("[OK] CAMPAIGN SEND COMPLETE")
    print("=" * 60)
    print(f"\n  {stats['added']} leads added to Smartlead campaign")
    print(f"  {stats['added']} leads marked as 'sent' in Supabase")
    print("\n  Next: Check Smartlead dashboard to verify leads and start campaign")
    print()


if __name__ == "__main__":
    main()
