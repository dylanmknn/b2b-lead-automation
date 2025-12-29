"""Agent 2: Email Enrichment with Hunter.io"""

import requests
import anthropic
from config.settings import settings


class EmailEnricher:
    def __init__(self):
        self.api_key = settings.HUNTER_API_KEY
        self.base_url = "https://api.hunter.io/v2"
        self.claude_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    def find_company_domain(self, company_name):
        """Find company domain from company name using Hunter."""
        if not company_name:
            return None

        # Hunter domain search by company name
        url = f"{self.base_url}/domain-search"

        params = {"company": company_name, "api_key": self.api_key}

        try:
            response = requests.get(url, params=params)

            if response.status_code != 200:
                return None

            data = response.json()

            # Extract domain from response
            if data.get("data") and data["data"].get("domain"):
                domain = data["data"]["domain"]
                return domain

            return None

        except Exception:
            return None

    def get_company_size(self, domain: str) -> dict:
        """Get company size using Hunter API. Returns employee_range, is_large_company (500+)."""
        if not domain:
            return {
                "employee_range": None,
                "is_large_company": False,
                "is_b2c": False,
                "industry": None,
            }

        url = f"{self.base_url}/companies/find"

        params = {"domain": domain, "api_key": self.api_key}

        # Large company ranges (500+ employees)
        # Hunter returns formats like: "1-10", "11-50", "51-200", "201-500", "501-1K", "1K-5K", "5K-10K", "10K-50K", "50K-100K", "100K+"
        LARGE_COMPANY_RANGES = {
            "501-1K",
            "1K-5K",
            "5K-10K",
            "10K-50K",
            "50K-100K",
            "100K+",  # Hunter K format
            "501-1000",
            "1001-5000",
            "5001-10000",
            "10001+",  # Alternative formats
        }

        try:
            response = requests.get(url, params=params)

            if response.status_code != 200:
                # If API fails, don't filter (assume small B2B company)
                return {
                    "employee_range": None,
                    "is_large_company": False,
                    "is_b2c": False,
                    "industry": None,
                }

            data = response.json()
            company_data = data.get("data", {})

            # Extract employee range from data.metrics.employees
            metrics = company_data.get("metrics", {})
            employee_range = metrics.get("employees")

            is_large = employee_range in LARGE_COMPANY_RANGES

            # Extract company info for B2C detection
            industry = company_data.get("industry", "")
            sector = company_data.get("sector", "")
            description = company_data.get("description", "")

            return {
                "employee_range": employee_range,
                "is_large_company": is_large,
                "industry": industry or sector or None,
                "description": description,
            }

        except Exception:
            # On error, don't filter (assume small B2B company)
            return {
                "employee_range": None,
                "is_large_company": False,
                "industry": None,
                "description": None,
            }

    def is_b2c_company(
        self, company_name: str, industry: str, description: str
    ) -> dict:
        """Use Claude to determine if company is B2C or B2B."""
        # Build context from available info
        context_parts = [f"Company: {company_name}"]
        if industry:
            context_parts.append(f"Industry: {industry}")
        if description:
            context_parts.append(
                f"Description: {description[:500]}"
            )  # Limit description length

        context = "\n".join(context_parts)

        # If we have no info at all, assume B2B (don't filter)
        if not industry and not description:
            return {"is_b2c": False, "reason": "No data available"}

        prompt = f"""Analyze this company and determine if it's B2B or B2C.

{context}

B2B = sells products/services to OTHER BUSINESSES (software, consulting, enterprise tools, professional services, etc.)
B2C = sells products/services directly to CONSUMERS (retail, restaurants, consumer apps, e-commerce to individuals, etc.)

Important: Some companies do BOTH (like Amazon, Apple). If the company primarily serves businesses OR has significant B2B operations, classify as B2B.

Respond with ONLY one word: B2B or B2C"""

        try:
            message = self.claude_client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}],
            )

            response = message.content[0].text.strip().upper()
            is_b2c = "B2C" in response

            return {"is_b2c": is_b2c, "reason": "AI classification"}

        except Exception as e:
            print(f"  [WARN] B2C check failed: {str(e)}")
            # On error, don't filter (assume B2B)
            return {"is_b2c": False, "reason": f"Error: {str(e)}"}

    def find_decision_maker(self, company_domain):
        """Find decision-maker email using Hunter domain search."""
        if not company_domain:
            return None

        print(f"  [SEARCH] Searching decision-maker at {company_domain}...")

        # Hunter domain search endpoint
        url = f"{self.base_url}/domain-search"

        params = {
            "domain": company_domain,
            "api_key": self.api_key,
            "limit": 1,
            "seniority": "senior",  # Target senior people (CEO, Founder, etc)
            "type": "personal",  # Only personal emails, not generic ones
        }

        try:
            response = requests.get(url, params=params)

            if response.status_code != 200:
                print(f"  [ERROR] Status {response.status_code}: {response.text}")
                return None

            data = response.json()

            # Check if we found any emails
            if (
                data.get("data")
                and data["data"].get("emails")
                and len(data["data"]["emails"]) > 0
            ):
                contact = data["data"]["emails"][0]

                return {
                    "first_name": contact.get("first_name", ""),
                    "last_name": contact.get("last_name", ""),
                    "email": contact.get("value"),
                    "title": contact.get("position", "Decision Maker"),
                    "confidence": contact.get("confidence", 0),
                }

            print(f"  [WARN] No email found for {company_domain}")
            return None

        except Exception as e:
            print(f"  [ERROR] Error: {str(e)}")
            return None

    def verify_email(self, email: str) -> dict:
        """Verify email using Hunter API. Returns status, score, and verified boolean."""
        if not email:
            return {"status": "invalid", "score": 0, "verified": False}

        url = f"{self.base_url}/email-verifier"

        params = {"email": email, "api_key": self.api_key}

        try:
            response = requests.get(url, params=params)

            if response.status_code != 200:
                print(
                    f"  [ERROR] Verification failed for {email}: {response.status_code}"
                )
                return {"status": "unknown", "score": 0, "verified": False}

            data = response.json()
            result = data.get("data", {})

            status = result.get("status", "unknown")
            score = result.get("score", 0)

            # Consider valid or accept_all with high score as verified
            # accept_all = catch-all domain (can't verify mailbox exists)
            verified = status == "valid" or (status == "accept_all" and score >= 80)

            return {
                "status": status,
                "score": score,
                "verified": verified,
                "result": result.get("result", "unknown"),
            }

        except Exception as e:
            print(f"  [ERROR] Verification error for {email}: {str(e)}")
            return {"status": "unknown", "score": 0, "verified": False}
