# Rekrute Jobs Scraper

Scrape job listings from Rekrute.com, the leading job portal in Morocco and Africa. This powerful scraper extracts comprehensive job data including titles, companies, locations, descriptions, and posting dates from Rekrute's extensive database of employment opportunities.

## Overview

Rekrute.com is Morocco's premier job board, featuring thousands of job postings across various industries, sectors, and experience levels. This scraper automates the collection of job data, making it ideal for job market analysis, recruitment agencies, HR departments, and researchers tracking employment trends in Morocco and the African job market.

## Features

- **Comprehensive Job Extraction**: Collects detailed job information including titles, companies, locations, and full descriptions
- **Flexible Search Options**: Search by keywords, locations, categories, and date filters
- **Pagination Handling**: Automatically navigates through multiple result pages
- **Detail Page Scraping**: Optionally fetches complete job descriptions from individual job pages
- **Date Filtering**: Filter jobs by publication date (24 hours, 3 days, 7 days, 30 days, or any date)
- **Structured Output**: Saves data in a clean, consistent JSON format
- **Anti-Bot Measures**: Built-in mechanisms to handle website protections
- **Proxy Support**: Compatible with proxy services for reliable scraping

## Input Parameters

The scraper accepts the following input parameters:

### Basic Search Parameters
- **startUrl** (string): Start scraping from a specific Rekrute.com search URL. Overrides other search parameters when provided.
- **keyword** (string): Job search keywords (e.g., "software engineer", "marketing manager"). Leave empty for general listings.
- **location** (string): Location filter (e.g., "Casablanca", "Rabat", "Marrakech"). Leave empty for all locations.
- **dateFilter** (string): Filter jobs by publication date. Options: "24 hours", "3 days", "7 days", "30 days", "any date". Default: "any date".

### Scraping Configuration
- **collectDetails** (boolean): Whether to visit individual job pages for full descriptions. Default: true.
- **results_wanted** (integer): Maximum number of jobs to collect. Default: 100.
- **max_pages** (integer): Maximum number of search result pages to process. Default: 20.

### Advanced Options
- **proxyConfiguration** (object): Proxy settings for enhanced reliability and anti-detection.
- **cookies** (string): Custom cookies as raw header string.
- **cookiesJson** (string): Custom cookies in JSON format.
- **dedupe** (boolean): Remove duplicate job URLs. Default: true.

## Output Data

Each scraped job is saved as a JSON object with the following structure:

```json
{
  "title": "Job Title",
  "company": "Company Name",
  "location": "City, Country",
  "date_posted": "Publication Date",
  "description_html": "<p>Full job description in HTML format</p>",
  "description_text": "Plain text version of the job description",
  "url": "https://www.rekrute.com/job-url"
}
```

### Output Fields Description
- **title**: The job position title
- **company**: Hiring company name
- **location**: Job location (city and country)
- **date_posted**: When the job was posted
- **description_html**: Complete job description with HTML formatting
- **description_text**: Plain text version for easy processing
- **url**: Direct link to the job posting on Rekrute.com

## Usage Examples

### Basic Usage
Run the scraper with default settings to collect recent job listings:

```json
{
  "results_wanted": 50,
  "collectDetails": true
}
```

### Keyword Search
Search for specific job types in a particular location:

```json
{
  "keyword": "data analyst",
  "location": "Casablanca",
  "results_wanted": 25
}
```

### Recent Jobs Only
Collect only jobs posted in the last 7 days:

```json
{
  "dateFilter": "7 days",
  "results_wanted": 100
}
```

### Custom Search URL
Start from a pre-configured Rekrute.com search page:

```json
{
  "startUrl": "https://www.rekrute.com/offres.html?keyword=marketing",
  "collectDetails": false
}
```

## Configuration

### Proxy Setup
For best results, configure proxy settings:

```json
{
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

### Performance Tuning
Adjust concurrency and limits based on your needs:

```json
{
  "results_wanted": 500,
  "max_pages": 50
}
```

## Use Cases

- **Job Market Research**: Analyze employment trends in Morocco
- **Recruitment Agencies**: Build comprehensive job databases
- **HR Departments**: Monitor competitor hiring patterns
- **Career Platforms**: Aggregate job listings from multiple sources
- **Academic Research**: Study labor market dynamics in Africa

## Data Quality

- Extracts data directly from Rekrute.com's job listings
- Handles both list view and detailed job pages
- Maintains data integrity with deduplication
- Provides both HTML and text formats for descriptions
- Includes publication dates for recency filtering

## Limitations

- Subject to Rekrute.com's terms of service
- May require proxy configuration for large-scale scraping
- Job availability depends on Rekrute.com's current listings
- Some jobs may have incomplete information

## Support

For issues or feature requests, please check the actor's documentation or contact support through the Apify platform.

---

**Keywords**: Rekrute.com scraper, Morocco jobs, Africa employment, job listings scraper, recruitment data, Moroccan job market, Casablanca jobs, Rabat careers, Marrakech employment, African job board