"""Tests for millemail_pipeline functions."""

import pytest
from datetime import datetime, timedelta, timezone
from src.millemail_pipeline import filter_duplicates, filter_cooldown


class TestFilterDuplicates:
    """Test suite for filter_duplicates function."""

    def test_filter_duplicates_no_existing_contacts(self):
        """Test filtering with no existing contacts."""
        profiles = [
            {"company_domain": "example.com", "email": "user@example.com"},
            {"company_domain": "test.com", "email": "user@test.com"},
        ]
        existing_contacts = set()

        result = filter_duplicates(profiles, existing_contacts)

        assert len(result) == 2
        assert result == profiles

    def test_filter_duplicates_all_duplicates(self):
        """Test filtering when all profiles are duplicates."""
        profiles = [
            {"company_domain": "example.com", "email": "user@example.com"},
            {"company_domain": "test.com", "email": "user@test.com"},
        ]
        existing_contacts = {
            ("example.com", "user@example.com"),
            ("test.com", "user@test.com"),
        }

        result = filter_duplicates(profiles, existing_contacts)

        assert len(result) == 0

    def test_filter_duplicates_partial_duplicates(self):
        """Test filtering with some duplicates."""
        profiles = [
            {"company_domain": "example.com", "email": "user@example.com"},
            {"company_domain": "test.com", "email": "user@test.com"},
            {"company_domain": "new.com", "email": "user@new.com"},
        ]
        existing_contacts = {("example.com", "user@example.com")}

        result = filter_duplicates(profiles, existing_contacts)

        assert len(result) == 2
        assert {"company_domain": "test.com", "email": "user@test.com"} in result
        assert {"company_domain": "new.com", "email": "user@new.com"} in result

    def test_filter_duplicates_missing_domain(self):
        """Test filtering profiles with missing domain."""
        profiles = [
            {"company_domain": None, "email": "user@example.com"},
            {"company_domain": "test.com", "email": "user@test.com"},
        ]
        existing_contacts = set()

        result = filter_duplicates(profiles, existing_contacts)

        assert len(result) == 2

    def test_filter_duplicates_missing_email(self):
        """Test filtering profiles with missing email."""
        profiles = [
            {"company_domain": "example.com", "email": None},
            {"company_domain": "test.com", "email": "user@test.com"},
        ]
        existing_contacts = set()

        result = filter_duplicates(profiles, existing_contacts)

        assert len(result) == 2

    def test_filter_duplicates_empty_profiles(self):
        """Test filtering empty profiles list."""
        profiles = []
        existing_contacts = {("example.com", "user@example.com")}

        result = filter_duplicates(profiles, existing_contacts)

        assert len(result) == 0

    def test_filter_duplicates_case_sensitive(self):
        """Test that filtering is case-sensitive."""
        profiles = [
            {"company_domain": "Example.com", "email": "user@example.com"},
            {"company_domain": "example.com", "email": "User@example.com"},
        ]
        existing_contacts = {("example.com", "user@example.com")}

        result = filter_duplicates(profiles, existing_contacts)

        assert len(result) == 2


class TestFilterCooldown:
    """Test suite for filter_cooldown function."""

    def test_filter_cooldown_no_last_contacts(self):
        """Test filtering with no previous contacts."""
        profiles = [{"company_domain": "example.com"}, {"company_domain": "test.com"}]
        last_contact_dates = {}

        result = filter_cooldown(profiles, last_contact_dates, cooldown_days=90)

        assert len(result) == 2
        assert result == profiles

    def test_filter_cooldown_all_in_cooldown(self):
        """Test filtering when all companies are in cooldown."""
        now = datetime.now(timezone.utc)
        recent_date = (now - timedelta(days=30)).isoformat()

        profiles = [{"company_domain": "example.com"}, {"company_domain": "test.com"}]
        last_contact_dates = {"example.com": recent_date, "test.com": recent_date}

        result = filter_cooldown(profiles, last_contact_dates, cooldown_days=90)

        assert len(result) == 0

    def test_filter_cooldown_partial_cooldown(self):
        """Test filtering with some companies in cooldown."""
        now = datetime.now(timezone.utc)
        recent_date = (now - timedelta(days=30)).isoformat()
        old_date = (now - timedelta(days=120)).isoformat()

        profiles = [
            {"company_domain": "recent.com"},
            {"company_domain": "old.com"},
            {"company_domain": "new.com"},
        ]
        last_contact_dates = {"recent.com": recent_date, "old.com": old_date}

        result = filter_cooldown(profiles, last_contact_dates, cooldown_days=90)

        assert len(result) == 2
        assert {"company_domain": "old.com"} in result
        assert {"company_domain": "new.com"} in result

    def test_filter_cooldown_exact_boundary(self):
        """Test cooldown at exact boundary (90 days)."""
        now = datetime.now(timezone.utc)
        exactly_90_days = (now - timedelta(days=90, hours=1)).isoformat()

        profiles = [{"company_domain": "example.com"}]
        last_contact_dates = {"example.com": exactly_90_days}

        result = filter_cooldown(profiles, last_contact_dates, cooldown_days=90)

        assert len(result) == 1

    def test_filter_cooldown_custom_days(self):
        """Test cooldown with custom number of days."""
        now = datetime.now(timezone.utc)
        date_45_days_ago = (now - timedelta(days=45)).isoformat()

        profiles = [{"company_domain": "example.com"}]
        last_contact_dates = {"example.com": date_45_days_ago}

        result_30 = filter_cooldown(profiles, last_contact_dates, cooldown_days=30)
        result_60 = filter_cooldown(profiles, last_contact_dates, cooldown_days=60)

        assert len(result_30) == 1
        assert len(result_60) == 0

    def test_filter_cooldown_missing_domain(self):
        """Test filtering profiles with missing domain."""
        profiles = [{"company_domain": None}, {"company_domain": "test.com"}]
        last_contact_dates = {}

        result = filter_cooldown(profiles, last_contact_dates)

        assert len(result) == 2

    def test_filter_cooldown_empty_profiles(self):
        """Test filtering empty profiles list."""
        profiles = []
        last_contact_dates = {"example.com": datetime.now(timezone.utc).isoformat()}

        result = filter_cooldown(profiles, last_contact_dates)

        assert len(result) == 0

    def test_filter_cooldown_iso_format_with_z(self):
        """Test cooldown with ISO format ending in Z - regression test for timezone bug."""
        now = datetime.now(timezone.utc)
        recent_date = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"

        profiles = [{"company_domain": "example.com"}]
        last_contact_dates = {"example.com": recent_date}

        result = filter_cooldown(profiles, last_contact_dates, cooldown_days=90)

        assert len(result) == 0

    def test_filter_cooldown_preserves_profile_data(self):
        """Test that cooldown filter preserves all profile data."""
        now = datetime.now(timezone.utc)
        old_date = (now - timedelta(days=120)).isoformat()

        profiles = [
            {
                "company_domain": "example.com",
                "email": "user@example.com",
                "first_name": "John",
                "last_name": "Doe",
            }
        ]
        last_contact_dates = {"example.com": old_date}

        result = filter_cooldown(profiles, last_contact_dates, cooldown_days=90)

        assert len(result) == 1
        assert result[0]["email"] == "user@example.com"
        assert result[0]["first_name"] == "John"
        assert result[0]["last_name"] == "Doe"


class TestCombinedFiltering:
    """Test combining both filter functions."""

    def test_combined_filtering(self):
        """Test applying both filters in sequence."""
        now = datetime.now(timezone.utc)
        recent_date = (now - timedelta(days=30)).isoformat()
        old_date = (now - timedelta(days=120)).isoformat()

        profiles = [
            {"company_domain": "duplicate.com", "email": "user@duplicate.com"},
            {"company_domain": "cooldown.com", "email": "user@cooldown.com"},
            {"company_domain": "valid.com", "email": "user@valid.com"},
        ]

        existing_contacts = {("duplicate.com", "user@duplicate.com")}
        last_contact_dates = {"cooldown.com": recent_date}

        after_duplicates = filter_duplicates(profiles, existing_contacts)
        final_result = filter_cooldown(
            after_duplicates, last_contact_dates, cooldown_days=90
        )

        assert len(final_result) == 1
        assert final_result[0]["company_domain"] == "valid.com"
