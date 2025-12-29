# B2B Lead Automation

Scrapes LinkedIn for companies hiring sales/growth roles, enriches contacts via Hunter.io, generates personalized cold email sequences with Claude.

Built this to automate my own outbound lead gen - was spending 5+ hours/week doing it manually.

## How it works

1. Finds companies hiring VP Sales, CRO, Head of Growth, etc. (hiring = they have budget)
2. Filters out B2C companies and large enterprises (500+ employees)
3. Gets verified emails via Hunter.io
4. Claude writes personalized 3-email sequences based on company/role context
5. Exports to Smartlead for drip campaigns

## Tech stack

Python, Apify (LinkedIn scraping), Hunter.io, Claude API, Supabase (Postgres), Smartlead

## Setup

```bash
git clone https://github.com/dylanmknn/b2b-lead-automation.git
cd b2b-lead-automation
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.template .env  # add your API keys
```

## Database setup

Create a Supabase project at https://supabase.com, then run `schema.sql` in the SQL Editor to create the required table.

## Usage

```bash
# test run
python src/millemail_pipeline.py --keywords "VP Sales" "CRO" --count 10

# full run (all keywords, 50 jobs each)
python src/millemail_pipeline.py --count 50

# send to campaign
python src/send_millemail_to_smartlead.py
```

## Running with Docker

```bash
docker pull dylanmknn/b2b-lead-automation:latest
docker run --env-file .env dylanmknn/b2b-lead-automation:latest
```

Environment variables required (see .env.template):
- APIFY_API_KEY
- ANTHROPIC_API_KEY
- HUNTER_API_KEY
- SUPABASE_URL
- SUPABASE_KEY

Or build locally:

```bash
docker build -t b2b-lead-automation .
docker run --env-file .env b2b-lead-automation --count 50
```

## Project structure

```
src/
  millemail_pipeline.py     # main script
  agents/
    job_scraper.py          # LinkedIn via Apify
    email_enricher.py       # Hunter.io
    personalizer.py         # Claude sequences
    campaign_manager.py     # Smartlead export
  utils/
    supabase_client.py      # database
```

## License

MIT
