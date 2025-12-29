-- Supabase schema for b2b-lead-automation
-- Run this ENTIRE script in Supabase SQL Editor

DROP TABLE IF EXISTS millemail_prospects;

CREATE TABLE millemail_prospects (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- From job scraping
    company_name TEXT,
    job_title TEXT,
    job_url TEXT,
    location TEXT,
    source_keyword TEXT,
    posted_date TEXT,

    -- Added during enrichment
    company_domain TEXT,
    company_type TEXT DEFAULT 'b2b',
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    title TEXT,
    verification_status TEXT,
    verification_score INTEGER,

    -- Email sequence from Claude
    subject_line TEXT,
    email_1 TEXT,
    email_1_ps TEXT,
    email_2 TEXT,
    email_3 TEXT,

    -- Status
    status TEXT DEFAULT 'ready',
    sent_at TIMESTAMP WITH TIME ZONE,

    UNIQUE(company_domain)
);

CREATE INDEX IF NOT EXISTS idx_status ON millemail_prospects(status);
CREATE INDEX IF NOT EXISTS idx_domain ON millemail_prospects(company_domain);
