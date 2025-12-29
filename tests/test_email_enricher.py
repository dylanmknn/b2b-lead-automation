"""Tests for EmailEnricher class."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.agents.email_enricher import EmailEnricher


class TestEmailEnricher:
    """Test suite for EmailEnricher."""

    @pytest.fixture
    def enricher(self):
        """Create EmailEnricher instance with mocked settings."""
        with patch("src.agents.email_enricher.settings") as mock_settings:
            mock_settings.HUNTER_API_KEY = "test_hunter_key"
            mock_settings.ANTHROPIC_API_KEY = "test_anthropic_key"
            return EmailEnricher()

    def test_find_company_domain_success(self, enricher):
        """Test finding company domain returns domain string."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"domain": "doctolib.fr"}}

        with patch("requests.get", return_value=mock_response):
            result = enricher.find_company_domain("Doctolib")

        assert result == "doctolib.fr"

    def test_find_company_domain_no_company_name(self, enricher):
        """Test finding domain with empty company name returns None."""
        result = enricher.find_company_domain("")
        assert result is None

    def test_find_company_domain_api_error(self, enricher):
        """Test finding domain when API returns error."""
        mock_response = Mock()
        mock_response.status_code = 404

        with patch("requests.get", return_value=mock_response):
            result = enricher.find_company_domain("Unknown Company")

        assert result is None

    def test_find_company_domain_no_data(self, enricher):
        """Test finding domain when API returns no data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {}}

        with patch("requests.get", return_value=mock_response):
            result = enricher.find_company_domain("Company")

        assert result is None

    def test_find_company_domain_exception(self, enricher):
        """Test finding domain when exception occurs."""
        with patch("requests.get", side_effect=Exception("Network error")):
            result = enricher.find_company_domain("Company")

        assert result is None

    def test_get_company_size_large_company(self, enricher):
        """Test detecting large company (500+ employees)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "metrics": {"employees": "501-1K"},
                "industry": "Software",
                "description": "A large tech company",
            }
        }

        with patch("requests.get", return_value=mock_response):
            result = enricher.get_company_size("example.com")

        assert result["is_large_company"] is True
        assert result["employee_range"] == "501-1K"
        assert result["industry"] == "Software"

    def test_get_company_size_small_company(self, enricher):
        """Test detecting small company."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"metrics": {"employees": "11-50"}, "industry": "Consulting"}
        }

        with patch("requests.get", return_value=mock_response):
            result = enricher.get_company_size("small-company.com")

        assert result["is_large_company"] is False
        assert result["employee_range"] == "11-50"

    def test_get_company_size_no_domain(self, enricher):
        """Test getting company size with no domain."""
        result = enricher.get_company_size("")

        assert result["employee_range"] is None
        assert result["is_large_company"] is False
        assert result["is_b2c"] is False

    def test_is_b2c_company_b2c_classification(self, enricher):
        """Test B2C company classification."""
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="B2C")]

        with patch.object(
            enricher.claude_client.messages, "create", return_value=mock_message
        ):
            result = enricher.is_b2c_company(
                "McDonald's", "Fast Food", "Restaurant chain"
            )

        assert result["is_b2c"] is True
        assert result["reason"] == "AI classification"

    def test_is_b2c_company_b2b_classification(self, enricher):
        """Test B2B company classification."""
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="B2B")]

        with patch.object(
            enricher.claude_client.messages, "create", return_value=mock_message
        ):
            result = enricher.is_b2c_company("Salesforce", "Software", "CRM platform")

        assert result["is_b2c"] is False

    def test_is_b2c_company_no_data(self, enricher):
        """Test B2C check with no industry or description."""
        result = enricher.is_b2c_company("Company", None, None)

        assert result["is_b2c"] is False
        assert result["reason"] == "No data available"

    def test_is_b2c_company_api_error(self, enricher):
        """Test B2C check when Claude API fails."""
        with patch.object(
            enricher.claude_client.messages,
            "create",
            side_effect=Exception("API error"),
        ):
            result = enricher.is_b2c_company("Company", "Tech", "Description")

        assert result["is_b2c"] is False
        assert "Error" in result["reason"]

    def test_verify_email_valid(self, enricher):
        """Test verifying valid email."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"status": "valid", "score": 95, "result": "deliverable"}
        }

        with patch("requests.get", return_value=mock_response):
            result = enricher.verify_email("user@example.com")

        assert result["verified"] is True
        assert result["status"] == "valid"
        assert result["score"] == 95

    def test_verify_email_accept_all_high_score(self, enricher):
        """Test verifying accept_all email with high score."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"status": "accept_all", "score": 85, "result": "accept_all"}
        }

        with patch("requests.get", return_value=mock_response):
            result = enricher.verify_email("user@example.com")

        assert result["verified"] is True
        assert result["status"] == "accept_all"

    def test_verify_email_accept_all_low_score(self, enricher):
        """Test verifying accept_all email with low score."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"status": "accept_all", "score": 50}
        }

        with patch("requests.get", return_value=mock_response):
            result = enricher.verify_email("user@example.com")

        assert result["verified"] is False

    def test_verify_email_invalid(self, enricher):
        """Test verifying invalid email."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"status": "invalid", "score": 0}}

        with patch("requests.get", return_value=mock_response):
            result = enricher.verify_email("invalid@fake.com")

        assert result["verified"] is False
        assert result["status"] == "invalid"

    def test_verify_email_empty_email(self, enricher):
        """Test verifying empty email."""
        result = enricher.verify_email("")

        assert result["verified"] is False
        assert result["status"] == "invalid"
        assert result["score"] == 0

    def test_verify_email_api_error(self, enricher):
        """Test email verification when API fails."""
        mock_response = Mock()
        mock_response.status_code = 500

        with patch("requests.get", return_value=mock_response):
            result = enricher.verify_email("user@example.com")

        assert result["verified"] is False
        assert result["status"] == "unknown"

    def test_find_decision_maker_success(self, enricher):
        """Test finding decision maker email."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "emails": [
                    {
                        "first_name": "John",
                        "last_name": "Doe",
                        "value": "john@example.com",
                        "position": "CEO",
                        "confidence": 95,
                    }
                ]
            }
        }

        with patch("requests.get", return_value=mock_response):
            result = enricher.find_decision_maker("example.com")

        assert result is not None
        assert result["email"] == "john@example.com"
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["title"] == "CEO"
        assert result["confidence"] == 95

    def test_find_decision_maker_no_domain(self, enricher):
        """Test finding decision maker with no domain."""
        result = enricher.find_decision_maker("")
        assert result is None

    def test_find_decision_maker_no_emails_found(self, enricher):
        """Test finding decision maker when no emails found."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"emails": []}}

        with patch("requests.get", return_value=mock_response):
            result = enricher.find_decision_maker("example.com")

        assert result is None
