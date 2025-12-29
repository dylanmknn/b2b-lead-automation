"""Tests for SupabaseClient class."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from src.utils.supabase_client import SupabaseClient


class TestSupabaseClient:
    """Test suite for SupabaseClient."""

    @pytest.fixture
    def mock_supabase(self):
        """Create mocked Supabase client."""
        with patch("src.utils.supabase_client.create_client") as mock_create:
            with patch.dict(
                "os.environ",
                {
                    "SUPABASE_URL": "https://test.supabase.co",
                    "SUPABASE_KEY": "test_key",
                },
            ):
                mock_client = MagicMock()
                mock_create.return_value = mock_client
                client = SupabaseClient()
                client.client = mock_client
                yield client

    def test_init_success(self):
        """Test SupabaseClient initialization with valid credentials."""
        with patch("src.utils.supabase_client.create_client") as mock_create:
            with patch.dict(
                "os.environ",
                {
                    "SUPABASE_URL": "https://test.supabase.co",
                    "SUPABASE_KEY": "test_key",
                },
            ):
                client = SupabaseClient()

                mock_create.assert_called_once_with(
                    "https://test.supabase.co", "test_key"
                )
                assert client.table_name == "leads"

    def test_init_missing_credentials(self):
        """Test SupabaseClient initialization without credentials raises error."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(
                ValueError, match="Missing SUPABASE_URL or SUPABASE_KEY"
            ):
                SupabaseClient()

    def test_check_domain_exists_found(self, mock_supabase):
        """Test checking domain that exists in database."""
        mock_response = Mock()
        mock_response.data = [{"company_domain": "example.com"}]

        mock_query = MagicMock()
        mock_query.select.return_value.eq.return_value.limit.return_value.execute.return_value = (
            mock_response
        )
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.check_domain_exists("example.com")

        assert result is True
        mock_supabase.client.table.assert_called_once_with("leads")

    def test_check_domain_exists_not_found(self, mock_supabase):
        """Test checking domain that doesn't exist."""
        mock_response = Mock()
        mock_response.data = []

        mock_query = MagicMock()
        mock_query.select.return_value.eq.return_value.limit.return_value.execute.return_value = (
            mock_response
        )
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.check_domain_exists("notfound.com")

        assert result is False

    def test_check_domain_exists_error(self, mock_supabase):
        """Test checking domain when database error occurs."""
        mock_query = MagicMock()
        mock_query.select.side_effect = Exception("Database error")
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.check_domain_exists("example.com")

        assert result is False

    def test_insert_leads_success(self, mock_supabase):
        """Test inserting leads successfully."""
        mock_response = Mock()
        mock_response.data = [
            {"id": 1, "company_name": "Test Co"},
            {"id": 2, "company_name": "Another Co"},
        ]

        mock_query = MagicMock()
        mock_query.insert.return_value.execute.return_value = mock_response
        mock_supabase.client.table.return_value = mock_query

        leads = [
            {"company_name": "Test Co", "email": "test@test.com"},
            {"company_name": "Another Co", "email": "another@test.com"},
        ]

        result = mock_supabase.insert_leads(leads)

        assert result == 2
        mock_query.insert.assert_called_once()

    def test_insert_leads_adds_defaults(self, mock_supabase):
        """Test that insert_leads adds created_at and status defaults."""
        mock_response = Mock()
        mock_response.data = [{"id": 1}]

        mock_query = MagicMock()
        mock_query.insert.return_value.execute.return_value = mock_response
        mock_supabase.client.table.return_value = mock_query

        leads = [{"company_name": "Test"}]
        mock_supabase.insert_leads(leads)

        inserted_leads = mock_query.insert.call_args[0][0]
        assert "created_at" in inserted_leads[0]
        assert "status" in inserted_leads[0]
        assert inserted_leads[0]["status"] == "ready"

    def test_insert_leads_empty_list(self, mock_supabase):
        """Test inserting empty list returns 0."""
        result = mock_supabase.insert_leads([])
        assert result == 0

    def test_insert_leads_error(self, mock_supabase):
        """Test inserting leads when database error occurs."""
        mock_query = MagicMock()
        mock_query.insert.side_effect = Exception("Insert failed")
        mock_supabase.client.table.return_value = mock_query

        leads = [{"company_name": "Test"}]
        result = mock_supabase.insert_leads(leads)

        assert result == 0

    def test_get_ready_leads(self, mock_supabase):
        """Test getting leads with status='ready'."""
        mock_response = Mock()
        mock_response.data = [
            {"id": 1, "status": "ready"},
            {"id": 2, "status": "ready"},
        ]

        mock_query = MagicMock()
        mock_query.select.return_value.eq.return_value.limit.return_value.execute.return_value = (
            mock_response
        )
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.get_ready_leads(limit=100)

        assert len(result) == 2
        assert result[0]["status"] == "ready"

    def test_get_ready_leads_error(self, mock_supabase):
        """Test getting ready leads when error occurs."""
        mock_query = MagicMock()
        mock_query.select.side_effect = Exception("Query failed")
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.get_ready_leads()

        assert result == []

    def test_update_lead_status_success(self, mock_supabase):
        """Test updating lead status successfully."""
        mock_query = MagicMock()
        mock_query.update.return_value.eq.return_value.execute.return_value = Mock()
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.update_lead_status(1, "contacted")

        assert result is True
        mock_query.update.assert_called_once_with({"status": "contacted"})

    def test_update_lead_status_error(self, mock_supabase):
        """Test updating lead status when error occurs."""
        mock_query = MagicMock()
        mock_query.update.side_effect = Exception("Update failed")
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.update_lead_status(1, "contacted")

        assert result is False

    def test_get_total_leads_count(self, mock_supabase):
        """Test getting total leads count."""
        mock_response = Mock()
        mock_response.count = 150

        mock_query = MagicMock()
        mock_query.select.return_value.execute.return_value = mock_response
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.get_total_leads_count()

        assert result == 150

    def test_get_total_leads_count_none(self, mock_supabase):
        """Test getting total leads count when count is None."""
        mock_response = Mock()
        mock_response.count = None

        mock_query = MagicMock()
        mock_query.select.return_value.execute.return_value = mock_response
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.get_total_leads_count()

        assert result == 0

    def test_get_domains_in_database(self, mock_supabase):
        """Test getting unique company domains."""
        mock_response = Mock()
        mock_response.data = [
            {"company_domain": "example.com"},
            {"company_domain": "test.com"},
            {"company_domain": "example.com"},
            {"company_domain": None},
        ]

        mock_query = MagicMock()
        mock_query.select.return_value.execute.return_value = mock_response
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.get_domains_in_database()

        assert len(result) == 2
        assert "example.com" in result
        assert "test.com" in result
        assert None not in result

    def test_get_existing_lead_contacts(self, mock_supabase):
        """Test getting existing lead contact tuples."""
        mock_response = Mock()
        mock_response.data = [
            {"company_domain": "example.com", "email": "user1@example.com"},
            {"company_domain": "test.com", "email": "user2@test.com"},
            {"company_domain": None, "email": "user3@null.com"},
            {"company_domain": "valid.com", "email": None},
        ]

        mock_query = MagicMock()
        mock_query.select.return_value.execute.return_value = mock_response
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.get_existing_lead_contacts()

        assert len(result) == 2
        assert ("example.com", "user1@example.com") in result
        assert ("test.com", "user2@test.com") in result

    def test_get_last_contact_dates(self, mock_supabase):
        """Test getting last contact dates per company."""
        mock_response = Mock()
        mock_response.data = [
            {"company_domain": "example.com", "created_at": "2024-01-15T10:00:00Z"},
            {"company_domain": "test.com", "created_at": "2024-01-10T10:00:00Z"},
            {"company_domain": "example.com", "created_at": "2024-01-05T10:00:00Z"},
        ]

        mock_query = MagicMock()
        mock_query.select.return_value.in_.return_value.order.return_value.execute.return_value = (
            mock_response
        )
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.get_last_contact_dates()

        assert len(result) == 2
        assert result["example.com"] == "2024-01-15T10:00:00Z"
        assert result["test.com"] == "2024-01-10T10:00:00Z"

    def test_get_last_contact_dates_error(self, mock_supabase):
        """Test getting last contact dates when error occurs."""
        mock_query = MagicMock()
        mock_query.select.side_effect = Exception("Query failed")
        mock_supabase.client.table.return_value = mock_query

        result = mock_supabase.get_last_contact_dates()

        assert result == {}
