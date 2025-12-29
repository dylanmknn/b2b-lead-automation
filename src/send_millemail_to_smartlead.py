#!/usr/bin/env python3
"""Send MilleMail B2B prospects to Smartlead campaign."""
import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from agents.campaign_manager import CampaignManager
from utils.millemail_supabase import MilleMailSupabaseClient


def main():
    parser = argparse.ArgumentParser(
        description="Send MilleMail prospects to Smartlead campaign"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Number of prospects to send (default: 50)",
    )
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    print("\n" + "=" * 80)
    print("[EMAIL] MILLEMAIL → SMARTLEAD CAMPAIGN SENDER")
    print("=" * 80)

    # Initialize clients
    print("\n1. Initializing clients...")
    try:
        supabase = MilleMailSupabaseClient()
        campaign_manager = CampaignManager()
        print("  [OK] Connected to Supabase (millemail_prospects table)")
        print(
            f"  [OK] Connected to Smartlead (Campaign ID: {campaign_manager.campaign_id})"
        )
    except Exception as e:
        print(f"  [ERROR] Failed to initialize: {e}")
        sys.exit(1)

    # Fetch ready prospects
    print(f"\n2. Fetching {args.count} ready MilleMail prospects...")
    print("  Filters: status='ready', company_type='b2b', has email sequence")
    prospects = supabase.get_ready_prospects(args.count)

    if not prospects:
        print("  [WARN]  No prospects found matching criteria")
        print("\n  Make sure prospects have:")
        print("    - status = 'ready'")
        print("    - company_type = 'b2b'")
        print("    - email, email_1, email_2, email_3 fields populated")
        sys.exit(0)

    print(f"  [OK] Found {len(prospects)} prospects ready to send")

    # Show sample prospect
    if prospects:
        sample = prospects[0]
        print("\n  Sample prospect:")
        print(f"    Company: {sample.get('company_name')}")
        print(f"    Contact: {sample.get('first_name')} {sample.get('last_name')}")
        print(f"    Email: {sample.get('email')}")
        print(f"    Title: {sample.get('title')}")
        print(f"    Has sequence: {'[OK]' if sample.get('email_1') else '[ERROR]'}")

    # Confirm before sending
    print(f"\n[WARN]  About to send {len(prospects)} prospects to Smartlead campaign")

    if not args.yes:
        response = input("  Continue? (yes/no): ").lower().strip()
        if response not in ["yes", "y"]:
            print("\n  [ERROR] Cancelled by user")
            sys.exit(0)
    else:
        print("  [OK] Auto-confirmed (--yes flag)")

    # Send to Smartlead
    print(f"\n3. Adding {len(prospects)} prospects to Smartlead campaign...")
    stats = campaign_manager.add_leads_to_campaign(prospects)

    print("\n  Results:")
    print(f"    Total processed: {stats['total']}")
    print(f"    [OK] Successfully added: {stats['added']}")
    print(f"    [WARN]  Duplicates (skipped): {stats['duplicates']}")
    print(f"    [ERROR] Invalid emails: {stats['invalid']}")

    # Update Supabase status for successfully added prospects
    if stats["added"] > 0:
        print("\n4. Updating prospect statuses to 'sent'...")
        # Update only the prospects that were successfully added
        successfully_added_ids = [
            prospect["id"] for prospect in prospects[: stats["added"]]
        ]

        updated_count = 0
        for prospect_id in successfully_added_ids:
            if supabase.update_prospect_status(prospect_id, "sent"):
                updated_count += 1

        print(f"  [OK] Updated {updated_count} prospects to status='sent'")

    print("\n" + "=" * 80)
    print("[OK] MILLEMAIL CAMPAIGN SEND COMPLETE")
    print("=" * 80)
    print(f"\n  {stats['added']} prospects added to Smartlead campaign")
    print(f"  {stats['added']} prospects marked as 'sent' in millemail_prospects table")
    print("\n  Next: Check Smartlead dashboard to verify prospects and start campaign")
    print()


if __name__ == "__main__":
    main()
