import os
import re
import json
import logging
import threading
from datetime import datetime, timedelta
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import requests
import schedule
import time
import pytz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN")
AFFINITY_API_KEY = os.environ.get("AFFINITY_API_KEY")
AFFINITY_LIST_ID = os.environ.get("AFFINITY_LIST_ID")
NUDGE_CHANNEL_ID = os.environ.get("NUDGE_CHANNEL_ID")  # #deal-nudges channel ID
BRAVE_SEARCH_API_KEY = os.environ.get("BRAVE_SEARCH_API_KEY")

# Owner name to Slack ID mapping
OWNER_SLACK_MAP = {
    "Emma McDonagh": "U02SC43GEH4",
    "Shomik Dutta": "U03HP4WKP62",
    "Allison Hinckley": "U07S6CLHPL1",
    "Leila Pirbay": "U08840SFVN1",
}

# Slack ID to Affinity Person ID mapping
SLACK_TO_AFFINITY_PERSON = {
    "U02SC43GEH4": 217635093,   # Emma McDonagh
    "U03HP4WKP62": 217635937,   # Shomik Dutta
    "U07S6CLHPL1": 217637423,   # Allison Hinckley
    "U08840SFVN1": 217635950,   # Leila Pirbay
}

# Stage nudge thresholds (in days)
STAGE_THRESHOLDS = {
    "First Meeting": 14,   # 2 weeks
    "Engaged": 21,         # 3 weeks
    "Need to Pass": 14,    # 2 weeks
    "On Hold": 84,         # 12 weeks
}

# Affinity field IDs
STATUS_FIELD_ID = 4927710
OWNERS_FIELD_ID = 4927712
PASS_REASON_FIELD_ID = 4944316
MISSED_STATUS_VALUE_ID = 20689035

# Overture sector keywords — used to rank candidate websites found via search.
# Matched case-insensitively against candidate homepage text.
KEYWORDS = {
    "energy": [
        "renewable", "clean energy", "solar", "wind", "geothermal", "nuclear",
        "smr", "fusion", "hydrogen", "battery", "energy storage", "bess",
        "grid", "microgrid", "transmission", "electrification", "decarbonization",
        "decarbonize", "net zero", "net-zero", "emissions", "utility", "kilowatt",
        "megawatt", "gigawatt", "ev charging", "heat pump", "biofuel", "rng",
        "carbon capture", "ccus", "dac", "demand response", "virtual power plant",
        "vpp",
    ],
    "ai": [
        "artificial intelligence", "machine learning", "llm", "foundation model",
        "generative ai", "agent", "agentic", "neural network", "transformer",
        "computer vision", "nlp", "mlops", "fine-tuning", "inference", "rag",
        "vector database", "gpu", "accelerator", "autonomous", "ai infrastructure",
        "ai platform", "ai safety", "alignment",
    ],
    "industry": [
        "manufacturing", "advanced manufacturing", "factory", "robotics", "cobots",
        "automation", "supply chain", "logistics", "additive manufacturing",
        "3d printing", "cnc", "industrial iot", "iiot", "digital twin", "sensors",
        "advanced materials", "composites", "construction tech", "mining",
        "critical minerals", "rare earths", "semiconductor", "fab", "steel",
        "cement", "chemicals", "warehouse", "defense tech", "dual-use",
        "aerospace", "space", "satellites",
    ],
    "resilience": [
        "climate resilience", "climate adaptation", "disaster response",
        "emergency management", "wildfire", "flood", "hurricane", "extreme weather",
        "critical infrastructure", "grid hardening", "water infrastructure",
        "drought", "parametric insurance", "climate insurance", "national security",
        "homeland security", "food security", "agtech", "precision agriculture",
        "earth observation", "geospatial", "remote sensing", "weather forecasting",
        "supply chain resilience", "reshoring",
    ],
    "crosscutting": [
        "hard tech", "deep tech", "climate tech", "cleantech", "greentech",
        "hardware", "pilot", "commercial-scale", "series a", "series b", "seed",
        "founder", "ceo",
    ],
}

# Domains to exclude from URL search candidates — these are never the "real" company site
EXCLUDED_DOMAINS = {
    # Social / pro networks
    "linkedin.com", "twitter.com", "x.com", "facebook.com", "instagram.com",
    "youtube.com", "tiktok.com", "threads.net", "mastodon.social", "bsky.app",
    # Directories / data providers (competitors of the CRM view, not actual company sites)
    "crunchbase.com", "pitchbook.com", "cbinsights.com", "zoominfo.com",
    "rocketreach.co", "dnb.com", "growjo.com", "owler.com", "ventureradar.com",
    "apollo.io", "lusha.com", "signalhire.com", "clearbit.com",
    # Job boards / company review sites
    "glassdoor.com", "indeed.com", "monster.com", "ziprecruiter.com",
    # Reference / encyclopedia
    "wikipedia.org", "wikidata.org",
    # PR wires
    "prnewswire.com", "globenewswire.com", "businesswire.com", "accesswire.com",
    "prweb.com", "einnews.com", "einpresswire.com", "newswire.com", "prlog.org",
    # Financial news / data
    "bloomberg.com", "reuters.com", "marketwatch.com", "seekingalpha.com",
    "yahoo.com", "finance.yahoo.com", "cnbc.com", "fool.com", "morningstar.com",
    "wsj.com", "ft.com", "barrons.com", "investors.com",
    # General news
    "nytimes.com", "washingtonpost.com", "apnews.com", "cnn.com", "nbcnews.com",
    "foxnews.com", "npr.org", "bbc.com", "bbc.co.uk", "theguardian.com",
    "usatoday.com", "axios.com", "politico.com", "reuters.co.uk",
    # Tech / business press
    "techcrunch.com", "forbes.com", "fortune.com", "businessinsider.com",
    "venturebeat.com", "theverge.com", "wired.com", "fastcompany.com",
    "zdnet.com", "arstechnica.com", "engadget.com", "theinformation.com",
    "cio.com", "computerworld.com", "inc.com", "entrepreneur.com",
    "protocol.com", "theregister.com", "digitaltrends.com", "gizmodo.com",
    # Content / community platforms (not companies)
    "medium.com", "substack.com", "reddit.com", "ycombinator.com",
    "news.ycombinator.com", "producthunt.com", "quora.com", "stackexchange.com",
    "stackoverflow.com", "github.com", "gitlab.com",
    # Search engines
    "bing.com", "google.com", "duckduckgo.com", "brave.com",
    # App stores
    "apps.apple.com", "play.google.com",
}

# Minimum characters in a no-URL message before we trigger the URL-search poll
MIN_POLL_MESSAGE_LENGTH = 3

app = App(token=SLACK_BOT_TOKEN)

AFFINITY_BASE_URL = "https://api.affinity.co"


class AffinityClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.auth = ("", api_key)
        self.session.headers.update({"Content-Type": "application/json"})

    def search_organization(self, term):
        """Search for an organization by name or domain."""
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/organizations",
            params={"term": term}
        )
        response.raise_for_status()
        data = response.json()
        return data.get("organizations", [])

    def get_list_entries(self, list_id):
        """Get all list entries for a list."""
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/lists/{list_id}/list-entries"
        )
        response.raise_for_status()
        return response.json()

    def get_field_values(self, organization_id):
        """Get field values for an organization."""
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/field-values",
            params={"organization_id": organization_id}
        )
        response.raise_for_status()
        return response.json()

    def get_list_fields(self, list_id):
        """Get all fields for a list to find the stage field."""
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/lists/{list_id}"
        )
        response.raise_for_status()
        return response.json().get("fields", [])

    def create_organization(self, name, domain=None):
        """Create a new organization in Affinity."""
        data = {"name": name}
        if domain:
            data["domain"] = domain
        logger.info(f"Creating organization with data: {data}")
        response = self.session.post(
            f"{AFFINITY_BASE_URL}/organizations",
            json=data
        )
        response.raise_for_status()
        return response.json()

    def add_to_list(self, list_id, organization_id):
        """Add an organization to a list (deal pipeline)."""
        logger.info(f"Adding org {organization_id} to list {list_id}")
        response = self.session.post(
            f"{AFFINITY_BASE_URL}/lists/{list_id}/list-entries",
            json={"entity_id": organization_id}
        )
        response.raise_for_status()
        return response.json()

    def get_organization(self, org_id):
        """Get a specific organization by ID."""
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/organizations/{org_id}"
        )
        response.raise_for_status()
        return response.json()

    def get_field_value_changes(self, field_id):
        """Get field value changes for tracking when stages changed."""
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/field-value-changes",
            params={"field_id": field_id}
        )
        response.raise_for_status()
        return response.json()

    def get_list_entry_field_values(self, list_entry_id):
        """Get field values for a specific list entry."""
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/field-values",
            params={"list_entry_id": list_entry_id}
        )
        response.raise_for_status()
        return response.json()

    def set_field_value(self, field_id, entity_id, list_entry_id, value):
        """Set a field value for a list entry."""
        logger.info(f"Setting field {field_id} to {value} for entity {entity_id}, list entry {list_entry_id}")
        payload = {
            "field_id": field_id,
            "entity_id": entity_id,
            "list_entry_id": list_entry_id,
            "value": value
        }
        logger.info(f"Payload: {payload}")
        response = self.session.post(
            f"{AFFINITY_BASE_URL}/field-values",
            json=payload
        )
        if not response.ok:
            logger.error(f"Affinity error response: {response.text}")
        response.raise_for_status()
        return response.json()

    def create_note(self, organization_id, content):
        """Attach a note to an organization."""
        payload = {
            "organization_ids": [organization_id],
            "content": content,
        }
        response = self.session.post(
            f"{AFFINITY_BASE_URL}/notes",
            json=payload
        )
        response.raise_for_status()
        return response.json()

    def create_person_note(self, person_id, content):
        """Attach a note to a person."""
        payload = {
            "person_ids": [person_id],
            "content": content,
        }
        response = self.session.post(
            f"{AFFINITY_BASE_URL}/notes",
            json=payload
        )
        response.raise_for_status()
        return response.json()

    def search_person(self, term):
        """Search for a person by name or email."""
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/persons",
            params={"term": term}
        )
        response.raise_for_status()
        data = response.json()
        return data.get("persons", [])

    def create_person(self, first_name, last_name, emails=None, organization_ids=None):
        """Create a new person in Affinity."""
        data = {
            "first_name": first_name,
            "last_name": last_name,
            "emails": emails or [],
        }
        if organization_ids:
            data["organization_ids"] = organization_ids
        logger.info(f"Creating person with data: {data}")
        response = self.session.post(
            f"{AFFINITY_BASE_URL}/persons",
            json=data
        )
        response.raise_for_status()
        return response.json()


affinity = AffinityClient(AFFINITY_API_KEY)


def extract_company_info(text):
    """Extract company name and domain from message text."""
    # Try to extract URL/domain
    url_pattern = r'https?://(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+)'
    domain_pattern = r'\b([a-zA-Z0-9-]+\.(?:com|io|co|ai|org|net|app|vc|xyz|tech|dev))\b'

    domain = None
    url_match = re.search(url_pattern, text)
    if url_match:
        domain = url_match.group(1)
    else:
        domain_match = re.search(domain_pattern, text)
        if domain_match:
            domain = domain_match.group(1)

    # Clean up the company name
    company_name = text.strip()
    company_name = re.sub(r'https?://(?:www\.)?', '', company_name)
    company_name = re.sub(r'\([^)]*\)', '', company_name)
    # Remove missed/miss/missing keywords
    company_name = re.sub(r'\b(missed|miss|missing|we|this|one|was|a)\b', '', company_name, flags=re.IGNORECASE)
    company_name = company_name.strip(' -–—:/')

    # If we have a domain, use it as the search term
    if domain:
        # Remove trailing paths from domain
        domain = domain.split('/')[0]
        # Use domain as company name if text is just a URL
        if not company_name or company_name == domain:
            company_name = domain.split('.')[0].title()

    return company_name, domain


def strip_urls(text):
    """Remove URLs from text, leaving only plain words — used to build a search seed."""
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'\b[a-zA-Z0-9-]+\.(?:com|io|co|ai|org|net|app|vc|xyz|tech|dev)\S*', '', text)
    return text.strip()


def clean_seed_text(text):
    """Strip filler words and 'missed' keywords to build a cleaner search seed."""
    text = strip_urls(text)
    text = re.sub(r'\b(missed|miss|missing|we|this|one|was|a|the)\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'[\(\)\[\]<>]', '', text)
    return " ".join(text.split()).strip()


# ========================================
# LinkedIn URL parsing
# ========================================

LINKEDIN_URL_RE = re.compile(
    r'https?://(?:[a-zA-Z0-9-]+\.)?linkedin\.com/(in|company|pub|school|showcase)/([a-zA-Z0-9%._-]+)/?',
    re.IGNORECASE,
)
LINKEDIN_SHORT_RE = re.compile(r'https?://lnkd\.in/\S+', re.IGNORECASE)
GENERIC_LINKEDIN_RE = re.compile(r'https?://(?:[a-zA-Z0-9-]+\.)?linkedin\.com/\S*', re.IGNORECASE)


def parse_person_slug(slug):
    """Parse a LinkedIn /in/ slug into (first_name, last_name).

    LinkedIn often appends a random alphanumeric dedup token (e.g. 'jane-doe-12a3b4').
    If the trailing hyphen-separated token contains any digits, drop it.
    """
    slug = slug.strip("/").lower()
    parts = slug.split("-")
    # Drop trailing dedup tokens: any tail segment with digits
    while len(parts) > 1 and any(ch.isdigit() for ch in parts[-1]):
        parts.pop()
    # Drop single-letter trailing middle initials leftover
    parts = [p for p in parts if p]
    if not parts:
        return ("", "")
    # Capitalize
    parts = [p.capitalize() for p in parts]
    if len(parts) == 1:
        return (parts[0], "")
    # First token = first name, rest = last name
    return (parts[0], " ".join(parts[1:]))


def parse_company_slug(slug):
    """Parse a LinkedIn /company/ slug into a readable company name."""
    slug = slug.strip("/").lower()
    parts = [p for p in slug.split("-") if p]
    if not parts:
        return ""
    # Upper-case only tech-style acronyms. Title-case Inc/Llc/Ltd and everything else.
    tech_upper = {"ai", "io", "ev", "ml", "ar", "vr", "iot", "api", "saas", "ui", "ux"}
    out = []
    for p in parts:
        if p in tech_upper:
            out.append(p.upper())
        else:
            out.append(p.capitalize())
    return " ".join(out)


def extract_linkedin_info(text):
    """Find the first LinkedIn URL in the text and classify it.

    Returns dict with keys: type ('person'|'company'|'other'), url, slug, name
    or None if no LinkedIn URL found.
    """
    m = LINKEDIN_URL_RE.search(text)
    if m:
        kind, slug = m.group(1).lower(), m.group(2)
        url = m.group(0)
        if kind == "in":
            first, last = parse_person_slug(slug)
            name = (first + " " + last).strip()
            return {"type": "person", "url": url, "slug": slug, "name": name,
                    "first_name": first, "last_name": last}
        if kind == "company":
            return {"type": "company", "url": url, "slug": slug,
                    "name": parse_company_slug(slug)}
        return {"type": "other", "url": url, "slug": slug, "name": ""}

    # Generic LinkedIn URL that didn't match our patterns (e.g., pulse, jobs)
    m = GENERIC_LINKEDIN_RE.search(text)
    if m:
        return {"type": "other", "url": m.group(0), "slug": "", "name": ""}

    # lnkd.in shortener
    m = LINKEDIN_SHORT_RE.search(text)
    if m:
        return {"type": "other", "url": m.group(0), "slug": "", "name": ""}

    return None


def strip_linkedin_urls(text):
    """Remove any LinkedIn URL from text so it isn't treated as the company domain."""
    text = LINKEDIN_URL_RE.sub(" ", text)
    text = GENERIC_LINKEDIN_RE.sub(" ", text)
    text = LINKEDIN_SHORT_RE.sub(" ", text)
    return " ".join(text.split()).strip()


def process_linkedin_person(linkedin_info, poster_id, client, channel_id, thread_ts):
    """Add a LinkedIn /in/ person as a Person lead in Affinity."""
    first = linkedin_info.get("first_name", "")
    last = linkedin_info.get("last_name", "")
    name = linkedin_info.get("name", "").strip()
    url = linkedin_info["url"]

    if not name:
        client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=(
                f"<@{poster_id}> I couldn't parse a name from that LinkedIn URL. "
                f"Please share the person's name and I'll add them as a lead."
            ),
            unfurl_links=False,
            unfurl_media=False,
        )
        return

    try:
        # Check if person already exists
        existing = affinity.search_person(name)
        match = None
        for p in existing:
            p_first = (p.get("first_name") or "").lower()
            p_last = (p.get("last_name") or "").lower()
            if p_first == first.lower() and p_last == last.lower():
                match = p
                break

        if match:
            person_id = match["id"]
            # Still attach a note if the LinkedIn URL isn't already recorded
            try:
                affinity.create_person_note(
                    person_id,
                    f"LinkedIn: {url} (shared by <@{poster_id}> via dealflow-bot)",
                )
            except Exception as e:
                logger.warning(f"Could not add note to existing person: {e}")
            client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=(
                    f"<@{poster_id}> *{name}* is already in Affinity as a person. "
                    f"I added the LinkedIn URL as a note.\n"
                    f"🔗 {url}"
                ),
                unfurl_links=False,
                unfurl_media=False,
            )
            return

        # Create new person
        person = affinity.create_person(first_name=first, last_name=last or "")
        person_id = person.get("id")
        try:
            affinity.create_person_note(
                person_id,
                f"LinkedIn: {url} (shared by <@{poster_id}> via dealflow-bot)",
            )
        except Exception as e:
            logger.warning(f"Could not add note to new person: {e}")

        client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=(
                f"<@{poster_id}> ✅ Added *{name}* as a person lead in Affinity. "
                f"LinkedIn URL saved as a note.\n🔗 {url}"
            ),
            unfurl_links=False,
            unfurl_media=False,
        )
    except requests.exceptions.HTTPError as e:
        error_text = e.response.text if hasattr(e.response, "text") else str(e)
        logger.error(f"Affinity person error: {error_text}")
        client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=f"<@{poster_id}> ❌ Error adding person: {e.response.status_code} — {error_text}",
            unfurl_links=False,
            unfurl_media=False,
        )
    except Exception as e:
        logger.error(f"Error processing LinkedIn person: {e}")
        client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=f"<@{poster_id}> ❌ Error adding person: {e}",
            unfurl_links=False,
            unfurl_media=False,
        )


def _split_query_and_context(seed_text, query_max=120, context_max=1500):
    """Split a long seed into a focused search query and surrounding context.

    The first line (or first ~80 chars ending at a word boundary) becomes the query.
    The full seed (up to context_max) is passed as context so the model knows sectors,
    founders, etc. Keeps the web-search query targeted.
    """
    seed_text = (seed_text or "").strip()
    if not seed_text:
        return "", ""

    # Prefer first line as the query
    first_line = seed_text.split("\n", 1)[0].strip()
    # If first line is too long, truncate at word boundary near 80 chars
    if len(first_line) > 80:
        truncated = first_line[:80]
        last_space = truncated.rfind(" ")
        query = truncated[:last_space] if last_space > 20 else truncated
    else:
        query = first_line

    # Hard cap on query length
    if len(query) > query_max:
        query = query[:query_max]

    # Context = the full seed, truncated
    context = seed_text[:context_max]
    return query, context


def _extract_json_array(raw):
    """Best-effort JSON array extraction from a model response.

    Handles common failure modes: code fences, leading/trailing prose, embedded
    citations, or the model returning a single object instead of an array.
    Returns (list_or_None, error_message).
    """
    if not raw or not isinstance(raw, str):
        return None, "empty response"

    text = raw.strip()

    # Strip code fences
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
        text = text.strip()

    # Try direct parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed, None
        if isinstance(parsed, dict):
            # Sometimes model returns {"candidates": [...]} or {"results": [...]}
            for key in ("candidates", "results", "websites", "urls"):
                if isinstance(parsed.get(key), list):
                    return parsed[key], None
            # Or a single candidate object — wrap it
            if "url" in parsed:
                return [parsed], None
    except Exception:
        pass

    # Find first JSON array via greedy regex
    m = re.search(r"\[.*\]", text, flags=re.DOTALL)
    if m:
        try:
            parsed = json.loads(m.group(0))
            if isinstance(parsed, list):
                return parsed, None
        except Exception as e:
            return None, f"JSON array parse error: {e}"

    return None, "no JSON array found in response"


def search_urls_with_brave(seed_text, max_candidates=3):
    """
    Use Brave Search API to find candidate company URLs for a given seed.
    Returns a dict: {"candidates": list, "error": str|None, "raw": str}
    """
    if not BRAVE_SEARCH_API_KEY:
        logger.error("BRAVE_SEARCH_API_KEY not set — cannot run URL search")
        return {"candidates": [], "error": "BRAVE_SEARCH_API_KEY not configured", "raw": ""}

    query, context = _split_query_and_context(seed_text)
    if not query:
        return {"candidates": [], "error": "empty query", "raw": ""}

    raw = ""
    try:
        logger.info(f"Brave web search — query='{query}' (context {len(context)} chars)")

        # Brave prefers short, focused queries. We pass the truncated query only.
        brave_query = query
        if len(brave_query) > 400:
            brave_query = brave_query[:400]

        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": BRAVE_SEARCH_API_KEY,
        }
        params = {
            "q": brave_query,
            "count": 20,          # fetch 20, filter down to top candidates (widens pool for seed-stage cos)
            "safesearch": "off",
            "result_filter": "web",
            "country": "us",
        }

        # Up to 2 attempts to handle the 1 req/sec rate limit
        last_err = None
        response = None
        for attempt in range(2):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                if response.status_code == 429:
                    last_err = "rate_limited (429)"
                    time.sleep(1.2)
                    continue
                if not response.ok:
                    last_err = f"HTTP {response.status_code}: {response.text[:200]}"
                    break
                break
            except Exception as e:
                last_err = f"request exception: {e}"
                time.sleep(1.2)

        if response is None or not response.ok:
            return {"candidates": [], "error": f"Brave call failed: {last_err}", "raw": ""}

        raw = response.text or ""
        logger.info(f"Brave raw response ({len(raw)} chars): {raw[:800]}")

        try:
            data = response.json()
        except Exception as e:
            return {"candidates": [], "error": f"could not parse Brave JSON ({e})", "raw": raw}

        web_results = (data.get("web") or {}).get("results") or []
        if not web_results:
            return {"candidates": [], "error": None, "raw": raw}

        # Filter excluded domains and dedupe by domain; preserve Brave's ranking
        cleaned = []
        seen_domains = set()
        for r in web_results:
            result_url = r.get("url") if isinstance(r, dict) else None
            if not result_url or not isinstance(result_url, str):
                continue
            m = re.search(r"https?://(?:www\.)?([^/]+)", result_url)
            domain = m.group(1).lower() if m else result_url.lower()
            if any(ex in domain for ex in EXCLUDED_DOMAINS):
                continue
            if domain in seen_domains:
                continue
            seen_domains.add(domain)
            title = r.get("title") or ""
            description = r.get("description") or ""
            why = (description or title).strip()
            # strip HTML bold tags Brave sometimes includes
            why = re.sub(r"<[^>]+>", "", why)[:200]
            cleaned.append({
                "url": result_url,
                "name": re.sub(r"<[^>]+>", "", title).strip() or domain.split(".")[0].title(),
                "why": why,
            })
            # Keep up to 10 raw candidates; rank_candidates() narrows to top 3 using the scorer.
            # Wider pool means the ranker has more material to find the real homepage among news.
            if len(cleaned) >= max(10, max_candidates):
                break

        return {"candidates": cleaned, "error": None, "raw": raw}

    except Exception as e:
        logger.error(f"Brave URL search failed: {e}")
        return {"candidates": [], "error": f"Brave call failed: {e}", "raw": raw}


def fetch_page_text(url, timeout=8):
    """Fetch a URL's homepage and return plaintext (rough, no HTML parser)."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Overture dealflow-bot)"}
        r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
        if not r.ok:
            return ""
        text = r.text
        text = re.sub(r"<script.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text[:8000].lower()
    except Exception as e:
        logger.warning(f"Could not fetch {url}: {e}")
        return ""


def score_candidate(url):
    """
    Fetch candidate homepage and score by Overture keyword matches.
    Returns (total_hits, {sector: hits}).
    """
    text = fetch_page_text(url)
    if not text:
        return 0, {}
    sector_hits = {}
    for sector, words in KEYWORDS.items():
        hits = 0
        for w in words:
            if w in text:
                hits += 1
        if hits > 0:
            sector_hits[sector] = hits
    total = sum(sector_hits.values())
    return total, sector_hits


_NAME_STOPWORDS = {
    "ai", "io", "labs", "lab", "inc", "llc", "ltd", "co", "corp", "corporation",
    "company", "technologies", "technology", "tech", "systems", "solutions",
    "group", "the", "and",
}


def _name_tokens_for_match(query):
    """Extract meaningful name tokens from the query for domain-match scoring.

    Lowercased alphanumeric tokens, with common company suffixes dropped. Tokens
    shorter than 3 chars are skipped to avoid false positives (e.g. "ai" matching
    half the internet).
    """
    if not query:
        return []
    q = query.lower()
    # Split on non-alphanumeric
    tokens = re.split(r"[^a-z0-9]+", q)
    out = []
    for t in tokens:
        if len(t) < 3:
            continue
        if t in _NAME_STOPWORDS:
            continue
        out.append(t)
    return out


def rank_candidates(candidates, query=""):
    """Score each candidate and sort by descending match.

    Ranking combines:
      - keyword-match score from the candidate page content (sector fit)
      - a large boost when the company name appears in the candidate's hostname
        (this is what keeps news / PR articles about the company from out-ranking
        the company's actual homepage)
      - a small penalty for URL paths that look like news articles or blog posts

    Ties preserve input order (Brave's own ranking).
    """
    name_tokens = _name_tokens_for_match(query)

    # Match any URL path segment that contains a news/article/blog word with word
    # boundaries. Catches "/news/", "/startup-news/", "/press-releases/",
    # "/news-and-events/", "/media-coverage/", etc. without false-matching
    # "/blogger/" or "/products/".
    _news_words = r"(?:news|press|article|articles|story|stories|blog|posts?|media|newsroom|announcements?|insights|coverage|releases?)"
    article_path_re = re.compile(
        rf"/[^/]*\b{_news_words}\b[^/]*/",
        re.IGNORECASE,
    )
    date_path_re = re.compile(r"/20[12]\d/\d{1,2}/")  # /2024/03/, /2026/11/

    scored = []
    for idx, c in enumerate(candidates):
        url = c["url"]
        m = re.search(r"https?://(?:www\.)?([^/]+)", url)
        hostname = (m.group(1) if m else url).lower()
        # Drop TLD suffixes for matching (e.g. extellis.com → extellis)
        hostname_core = re.split(r"[^a-z0-9]+", hostname)

        # Name-match boost: +1000 if any meaningful name token appears in the hostname
        name_boost = 0
        for t in name_tokens:
            if any(t == seg or t in seg for seg in hostname_core):
                name_boost = 1000
                break

        # URL-path penalty for news/blog/article paths
        path_penalty = 0
        if article_path_re.search(url) or date_path_re.search(url):
            path_penalty = 500

        total_keyword, _sectors = score_candidate(url)

        final_score = name_boost + total_keyword - path_penalty
        scored.append((final_score, -idx, c))

    scored.sort(reverse=True)
    return [c for _, _, c in scored]


def guess_company_domains(query, timeout=5):
    """
    Probe plausible domain patterns for a company name and return any that
    resolve with the name appearing in the homepage text.

    Called as a fallback when Brave's top results don't include any hostname
    containing the company name — common for seed-stage companies whose sites
    aren't well-indexed yet.

    Returns a list of candidate dicts in the same shape as search_urls_with_brave.
    """
    tokens = _name_tokens_for_match(query)
    if not tokens:
        return []
    # Use only the first meaningful token for the domain guess. For a query like
    # "Extellis" -> "extellis". For "DeepWeave AI" -> "deepweave".
    name = tokens[0]

    tlds = [".com", ".ai", ".io", ".co", ".xyz", ".tech", ".net"]
    prefixes = ["", "get", "join", "use", "try", "hello", "meet"]
    suffixes = ["", "hq", "app", "labs", "co"]

    guesses = []
    seen = set()
    for prefix in prefixes:
        for suffix in suffixes:
            stem = f"{prefix}{name}{suffix}"
            for tld in tlds:
                host = f"{stem}{tld}"
                if host in seen:
                    continue
                seen.add(host)
                guesses.append(host)

    # Cap probes so we don't hammer DNS / take forever. Prefer the simpler
    # patterns (no prefix/suffix, shorter TLDs) — they're listed first above.
    guesses = guesses[:20]

    logger.info(f"Domain-guess fallback probing {len(guesses)} patterns for '{name}'")

    found = []
    headers = {"User-Agent": "Mozilla/5.0 (Overture dealflow-bot)"}
    name_lower = name.lower()

    for host in guesses:
        if any(ex in host for ex in EXCLUDED_DOMAINS):
            continue
        url = f"https://{host}/"
        try:
            r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
            if not r.ok:
                continue
            # Check that the final URL isn't a parked-domain/registrar page
            final_host = ""
            mm = re.search(r"https?://(?:www\.)?([^/]+)", r.url or "")
            if mm:
                final_host = mm.group(1).lower()
            if any(ex in final_host for ex in EXCLUDED_DOMAINS):
                continue
            # Strip HTML, check the name appears in the page body
            body = r.text or ""
            body = re.sub(r"<script.*?</script>", " ", body, flags=re.DOTALL | re.IGNORECASE)
            body = re.sub(r"<style.*?</style>", " ", body, flags=re.DOTALL | re.IGNORECASE)
            body = re.sub(r"<[^>]+>", " ", body)
            body_lower = body.lower()
            if name_lower not in body_lower:
                continue
            # Grab a rough description: first ~180 chars of visible text
            cleaned_body = re.sub(r"\s+", " ", body).strip()
            why = cleaned_body[:180]
            found.append({
                "url": r.url or url,
                "name": query,
                "why": why,
            })
            logger.info(f"Domain-guess HIT: {host} -> {r.url}")
        except Exception as e:
            # Silent fail on individual probes — most won't resolve, that's expected
            logger.debug(f"Domain-guess miss: {host} ({e})")
            continue

    return found


def build_poll_blocks(seed_text, candidates, poster_id, is_missed, linkedin_url=None):
    """Build Slack Block Kit blocks for the URL-choice poll.

    Layout: each candidate is its own section row (URL + description) with a
    Select button accessory. Two bottom buttons: Write in URL / Stealth.
    """
    if linkedin_url:
        header_text = (
            f"<@{poster_id}> shared a LinkedIn company page. "
            f"Are any of these their actual website?"
        )
    else:
        header_text = (
            f"I couldn't find a URL in <@{poster_id}>'s message. "
            f"Are any of these right?"
        )

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header_text}},
    ]

    # One section per candidate, with a Select button accessory
    for idx, c in enumerate(candidates):
        value = json.dumps({
            "url": c["url"],
            "name": c["name"],
            "poster_id": poster_id,
            "is_missed": is_missed,
            "seed": seed_text,
            "linkedin_url": linkedin_url,
        })[:1900]  # Slack value limit is 2000

        why = (c.get("why") or "").strip()
        why = re.sub(r"\s+", " ", why)[:180]
        url_display = c["url"]
        if why:
            row_text = f"*{idx+1}.* <{url_display}|{url_display}>\n_{why}_"
        else:
            row_text = f"*{idx+1}.* <{url_display}|{url_display}>"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": row_text},
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "Select", "emoji": True},
                "action_id": f"url_pick_{idx}",
                "value": value,
            },
        })

    blocks.append({"type": "divider"})

    # Bottom row: Write in URL + Stealth
    reply_value = json.dumps({"poster_id": poster_id})[:1900]
    stealth_value = json.dumps({
        "poster_id": poster_id,
        "is_missed": is_missed,
        "seed": seed_text,
        "linkedin_url": linkedin_url,
    })[:1900]

    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "✍️ Write in URL", "emoji": True},
                "action_id": "url_reply_later",
                "value": reply_value,
            },
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "🕶 Stealth / no website", "emoji": True},
                "action_id": "url_stealth",
                "value": stealth_value,
                "style": "primary",
            },
        ],
    })

    return blocks


def post_url_poll(client, channel, thread_ts, poster_id, seed_text, is_missed, linkedin_url=None):
    """Search for candidates and post the poll in-channel (not threaded)."""
    logger.info(f"Running URL search poll for seed='{seed_text[:120]}' (linkedin={linkedin_url})")

    result = search_urls_with_brave(seed_text, max_candidates=3)
    candidates = result["candidates"]
    err = result["error"]

    # Build a display name: the focused query (first line / truncated)
    display_query, _ = _split_query_and_context(seed_text)
    display_name = display_query or seed_text[:80]
    query_for_rank = display_query or seed_text
    name_tokens = _name_tokens_for_match(query_for_rank)

    def _hostname_contains_name(url):
        m = re.search(r"https?://(?:www\.)?([^/]+)", url)
        host = (m.group(1) if m else "").lower()
        host_segs = re.split(r"[^a-z0-9]+", host)
        return any(any(t == seg or t in seg for seg in host_segs) for t in name_tokens)

    # Domain-guess fallback fires when Brave returned no results OR when none of
    # its results contain the company name in the hostname (common for seed-stage
    # companies whose sites aren't well-indexed).
    needs_guess = name_tokens and (
        not candidates or not any(_hostname_contains_name(c["url"]) for c in candidates)
    )
    if needs_guess:
        logger.info(
            f"No Brave candidate hostname contains '{name_tokens[0]}' — "
            f"running domain-guess fallback"
        )
        guessed = guess_company_domains(query_for_rank)
        if guessed:
            # Dedupe by domain against existing candidates
            existing_domains = set()
            for c in candidates:
                m = re.search(r"https?://(?:www\.)?([^/]+)", c["url"])
                if m:
                    existing_domains.add(m.group(1).lower())
            for g in guessed:
                m = re.search(r"https?://(?:www\.)?([^/]+)", g["url"])
                if m and m.group(1).lower() not in existing_domains:
                    candidates.append(g)

    if not candidates:
        if err:
            # Surface the actual error so we can debug in Slack, not just Railway logs
            fallback = (
                f"<@{poster_id}> ⚠️ URL search failed for *{display_name}* — `{err}`.\n"
                f"Please reply with the URL, or say `stealth` and I'll add it as a stealth lead."
            )
        else:
            fallback = (
                f"<@{poster_id}> I couldn't find any likely websites for *{display_name}*. "
                f"Please reply with the URL, or say `stealth` and I'll add it as a stealth lead."
            )
        if linkedin_url:
            fallback += f"\n🔗 LinkedIn: {linkedin_url}"
        client.chat_postMessage(
            channel=channel,
            text=fallback,
            unfurl_links=False,
            unfurl_media=False,
        )
        return

    # Cap to top 3 after ranking. Pass the query so the ranker can boost
    # domains that match the company name over news/PR articles that merely mention it.
    candidates = rank_candidates(candidates, query=query_for_rank)[:3]

    blocks = build_poll_blocks(display_name, candidates, poster_id, is_missed, linkedin_url=linkedin_url)
    client.chat_postMessage(
        channel=channel,
        text=f"URL guesses for {display_name}",
        blocks=blocks,
        unfurl_links=False,
        unfurl_media=False,
    )


def disable_poll_message(client, channel, ts, resolved_text):
    """Replace the poll blocks with a resolved-status message so buttons can't be re-clicked.

    Also suppresses link unfurls so the final confirmation stays compact.
    """
    try:
        client.chat_update(
            channel=channel,
            ts=ts,
            text=resolved_text,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": resolved_text}}],
            unfurl_links=False,
            unfurl_media=False,
        )
    except Exception as e:
        logger.warning(f"Could not update poll message: {e}")


def get_stage_name(organization_id, list_id):
    """Get the current stage name for an organization in a list."""
    try:
        # Get list fields to find the stage/status field
        fields = affinity.get_list_fields(list_id)
        stage_field_id = None
        stage_options = {}

        for field in fields:
            field_name = field.get("name", "").lower()
            if field_name in ["stage", "status", "deal stage"]:
                stage_field_id = field.get("id")
                # Build mapping of dropdown option IDs to names
                for option in field.get("dropdown_options", []):
                    stage_options[option["id"]] = option["text"]
                break

        if not stage_field_id:
            return "Unknown"

        # Get field values for this organization
        field_values = affinity.get_field_values(organization_id)

        for fv in field_values:
            if fv.get("field_id") == stage_field_id:
                value = fv.get("value")
                if isinstance(value, dict) and "text" in value:
                    return value["text"]
                elif isinstance(value, int) and value in stage_options:
                    return stage_options[value]
                return str(value) if value else "Not set"

        return "Not set"
    except Exception as e:
        logger.error(f"Error getting stage: {e}")
        return "Unknown"


def check_org_in_list(organization_id, list_id):
    """Check if an organization is already in the specified list."""
    try:
        org = affinity.get_organization(organization_id)
        list_entries = org.get("list_entries", [])
        for entry in list_entries:
            if entry.get("list_id") == int(list_id):
                return True, entry
        return False, None
    except Exception as e:
        logger.error(f"Error checking org in list: {e}")
        return False, None


def get_list_entry_details(org_id, list_id):
    """Get owner names and pass reason for an org in a list."""
    try:
        org = affinity.get_organization(org_id)
        list_entries = org.get("list_entries", [])

        for entry in list_entries:
            if entry.get("list_id") == int(list_id):
                list_entry_id = entry.get("id")
                field_values = affinity.get_list_entry_field_values(list_entry_id)

                owners = []
                pass_reasons = []

                for fv in field_values:
                    # Get owners
                    if fv.get("field_id") == OWNERS_FIELD_ID:
                        person_id = fv.get("value")
                        if person_id:
                            owner_name = get_owner_name_from_id(person_id)
                            if owner_name:
                                owners.append(owner_name)

                    # Get pass reason
                    if fv.get("field_id") == PASS_REASON_FIELD_ID:
                        value = fv.get("value")
                        if isinstance(value, dict) and "text" in value:
                            pass_reasons.append(value["text"])
                        elif value:
                            pass_reasons.append(str(value))

                return owners, pass_reasons

        return [], []
    except Exception as e:
        logger.error(f"Error getting list entry details: {e}")
        return [], []


def process_company(search_term, domain=None, is_missed=False, slack_user_id=None, note=None):
    """Check if company exists in deal pipeline. If yes, return current stage. If no, add it."""
    try:
        # Search using domain if available, otherwise use name
        term = domain if domain else search_term
        logger.info(f"Searching for: {term}")

        orgs = affinity.search_organization(term)
        logger.info(f"Found {len(orgs)} organizations")

        organization = None
        if orgs:
            # Find best match
            for org in orgs:
                org_domain = org.get("domain", "")
                org_name = org.get("name", "").lower()
                if domain and org_domain and domain.lower() in org_domain.lower():
                    organization = org
                    break
                if search_term.lower() in org_name:
                    organization = org
                    break
            if not organization:
                organization = orgs[0]

        if organization:
            org_id = organization["id"]
            org_name = organization["name"]
            logger.info(f"Found organization: {org_name} (ID: {org_id})")

            # Check if already in deal pipeline
            in_list, entry = check_org_in_list(org_id, AFFINITY_LIST_ID)

            if in_list:
                # Already in pipeline - get current stage, owner, and pass reason
                stage = get_stage_name(org_id, AFFINITY_LIST_ID)
                owners, pass_reasons = get_list_entry_details(org_id, AFFINITY_LIST_ID)

                message = f"*{org_name}* is already in the deal pipeline.\n📊 Current stage: *{stage}*"

                if owners:
                    message += f"\n👤 Owner: {', '.join(owners)}"

                if stage == "Passed" and pass_reasons:
                    message += f"\n❌ Pass reason: {', '.join(pass_reasons)}"

                return {
                    "status": "exists",
                    "company": org_name,
                    "stage": stage,
                    "message": message
                }
            else:
                # Org exists but not in pipeline - add it
                list_entry = affinity.add_to_list(AFFINITY_LIST_ID, org_id)

                # Set owner if we have a slack user mapping
                if slack_user_id and slack_user_id in SLACK_TO_AFFINITY_PERSON:
                    try:
                        affinity_person_id = SLACK_TO_AFFINITY_PERSON[slack_user_id]
                        affinity.set_field_value(OWNERS_FIELD_ID, org_id, list_entry["id"], affinity_person_id)
                        logger.info(f"Set owner to person {affinity_person_id}")
                    except Exception as e:
                        logger.error(f"Error setting owner: {e}")

                # If marked as missed, set the status
                if is_missed:
                    try:
                        affinity.set_field_value(STATUS_FIELD_ID, org_id, list_entry["id"], MISSED_STATUS_VALUE_ID)
                        if note:
                            try:
                                affinity.create_note(org_id, note)
                            except Exception as e:
                                logger.error(f"Error creating note: {e}")
                        return {
                            "status": "added",
                            "company": org_name,
                            "message": f"😢 Added *{org_name}* to the deal pipeline as *Missed*."
                        }
                    except Exception as e:
                        logger.error(f"Error setting missed status: {e}")

                if note:
                    try:
                        affinity.create_note(org_id, note)
                    except Exception as e:
                        logger.error(f"Error creating note: {e}")

                return {
                    "status": "added",
                    "company": org_name,
                    "message": f"✅ Added *{org_name}* to the deal pipeline as a new lead."
                }
        else:
            # Create new organization and add to pipeline
            logger.info(f"Creating new organization: {search_term}")
            new_org = affinity.create_organization(search_term, domain)
            org_id = new_org["id"]
            org_name = new_org["name"]
            logger.info(f"Created organization: {org_name} (ID: {org_id})")

            list_entry = affinity.add_to_list(AFFINITY_LIST_ID, org_id)

            # Set owner if we have a slack user mapping
            if slack_user_id and slack_user_id in SLACK_TO_AFFINITY_PERSON:
                try:
                    affinity_person_id = SLACK_TO_AFFINITY_PERSON[slack_user_id]
                    affinity.set_field_value(OWNERS_FIELD_ID, org_id, list_entry["id"], affinity_person_id)
                    logger.info(f"Set owner to person {affinity_person_id}")
                except Exception as e:
                    logger.error(f"Error setting owner: {e}")

            # If marked as missed, set the status
            if is_missed:
                try:
                    affinity.set_field_value(STATUS_FIELD_ID, org_id, list_entry["id"], MISSED_STATUS_VALUE_ID)
                    if note:
                        try:
                            affinity.create_note(org_id, note)
                        except Exception as e:
                            logger.error(f"Error creating note: {e}")
                    return {
                        "status": "created",
                        "company": org_name,
                        "message": f"😢 Created *{org_name}* and added to the deal pipeline as *Missed*."
                    }
                except Exception as e:
                    logger.error(f"Error setting missed status: {e}")

            if note:
                try:
                    affinity.create_note(org_id, note)
                except Exception as e:
                    logger.error(f"Error creating note: {e}")

            return {
                "status": "created",
                "company": org_name,
                "message": f"✅ Created *{org_name}* and added to the deal pipeline as a new lead."
            }

    except requests.exceptions.HTTPError as e:
        error_text = e.response.text if hasattr(e.response, 'text') else str(e)
        logger.error(f"Affinity API error: {error_text}")
        return {
            "status": "error",
            "message": f"❌ Error processing company: {e.response.status_code} - {error_text}"
        }
    except Exception as e:
        logger.error(f"Error processing company: {e}")
        return {
            "status": "error",
            "message": f"❌ Error processing company: {str(e)}"
        }


def get_deals_needing_nudge():
    """Get all deals that have been in a stage longer than the threshold."""
    try:
        # Get list fields to find Status and Owners fields
        fields = affinity.get_list_fields(AFFINITY_LIST_ID)
        status_field_id = None
        owners_field_id = None
        status_options = {}

        for field in fields:
            field_name = field.get("name", "").lower()
            if field_name in ["status", "stage"]:
                status_field_id = field.get("id")
                for option in field.get("dropdown_options", []):
                    status_options[option["id"]] = option["text"]
            elif field_name == "owners":
                owners_field_id = field.get("id")

        if not status_field_id:
            logger.error("Could not find Status field")
            return []

        # Get all list entries
        list_entries = affinity.get_list_entries(AFFINITY_LIST_ID)

        deals_to_nudge = []
        now = datetime.now(pytz.UTC)

        for entry in list_entries:
            entity_id = entry.get("entity_id")
            list_entry_id = entry.get("id")
            created_at = entry.get("created_at")

            # Get field values for this entry
            field_values = affinity.get_list_entry_field_values(list_entry_id)

            current_status = None
            status_updated_at = None
            owners = []

            for fv in field_values:
                if fv.get("field_id") == status_field_id:
                    value = fv.get("value")
                    if isinstance(value, dict) and "text" in value:
                        current_status = value["text"]
                    elif isinstance(value, int) and value in status_options:
                        current_status = status_options[value]
                    status_updated_at = fv.get("updated_at") or fv.get("created_at")

                elif fv.get("field_id") == owners_field_id:
                    # Owner field value is a person ID, need to resolve name
                    owner_value = fv.get("value")
                    if owner_value:
                        owners.append(owner_value)

            # Check if this status needs a nudge
            if current_status and current_status in STAGE_THRESHOLDS:
                threshold_days = STAGE_THRESHOLDS[current_status]

                # Parse the date when status was set
                if status_updated_at:
                    try:
                        status_date = datetime.fromisoformat(status_updated_at.replace('Z', '+00:00'))
                    except:
                        status_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                else:
                    status_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))

                days_in_stage = (now - status_date).days

                if days_in_stage >= threshold_days:
                    # Get org details
                    try:
                        org = affinity.get_organization(entity_id)
                        org_name = org.get("name", "Unknown")

                        weeks_in_stage = days_in_stage // 7
                        week_text = f"{weeks_in_stage} week" + ("s" if weeks_in_stage != 1 else "")

                        deals_to_nudge.append({
                            "org_id": entity_id,
                            "org_name": org_name,
                            "status": current_status,
                            "days_in_stage": days_in_stage,
                            "week_text": week_text,
                            "owners": owners,
                            "link": f"https://overture.affinity.co/companies/{entity_id}"
                        })
                    except Exception as e:
                        logger.error(f"Error getting org {entity_id}: {e}")

        return deals_to_nudge

    except Exception as e:
        logger.error(f"Error getting deals needing nudge: {e}")
        return []


def get_owner_name_from_id(person_id):
    """Get person name from Affinity person ID."""
    try:
        response = affinity.session.get(f"{AFFINITY_BASE_URL}/persons/{person_id}")
        response.raise_for_status()
        person = response.json()
        first_name = person.get("first_name", "")
        last_name = person.get("last_name", "")
        return f"{first_name} {last_name}".strip()
    except:
        return None


def send_nudge_messages():
    """Check for deals needing nudges and send Slack messages."""
    logger.info("Running nudge check...")

    if not NUDGE_CHANNEL_ID:
        logger.error("NUDGE_CHANNEL_ID not set")
        return

    deals = get_deals_needing_nudge()
    logger.info(f"Found {len(deals)} deals needing nudges")

    for deal in deals:
        # Determine who to tag
        slack_mention = ""

        if deal["owners"]:
            # Get first owner's name and map to Slack ID
            owner_name = get_owner_name_from_id(deal["owners"][0])
            if owner_name and owner_name in OWNER_SLACK_MAP:
                slack_id = OWNER_SLACK_MAP[owner_name]
                slack_mention = f"<@{slack_id}> "

        message = f"{slack_mention}{deal['org_name']} has been in \"{deal['status']}\" for {deal['week_text']}. Link: {deal['link']}"

        try:
            app.client.chat_postMessage(
                channel=NUDGE_CHANNEL_ID,
                text=message
            )
            logger.info(f"Sent nudge for {deal['org_name']}")
        except Exception as e:
            logger.error(f"Error sending nudge for {deal['org_name']}: {e}")


def run_scheduler():
    """Run the scheduler in a separate thread."""
    # Schedule nudge check at 9am PT on Tuesdays
    pacific = pytz.timezone('America/Los_Angeles')
    schedule.every().tuesday.at("09:00").do(send_nudge_messages)

    logger.info("Scheduler started - nudges will run Tuesdays at 9am PT")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


@app.event("message")
def handle_message(event, say, client):
    """Handle messages posted to #dealflow channel."""
    if event.get("subtype") in ["bot_message", "message_changed", "message_deleted"]:
        return

    channel_id = event.get("channel")
    text = event.get("text", "").strip()
    ts = event.get("ts")
    thread_ts = event.get("thread_ts") or ts

    # Check for manual nudge test command
    if text.lower() == "!nudge-test":
        say(text="🔄 Running nudge check...")
        send_nudge_messages()
        say(text="✅ Nudge check complete!")
        return

    try:
        channel_info = client.conversations_info(channel=channel_id)
        channel_name = channel_info["channel"]["name"]

        if channel_name != "dealflow":
            return
    except Exception as e:
        logger.error(f"Error getting channel info: {e}")
        return

    if not text:
        return

    user_id = event.get("user")

    # Check if this is a "missed" deal BEFORE branching
    missed_pattern = r'\b(missed|miss|missing)\b'
    is_missed = bool(re.search(missed_pattern, text.lower()))

    # --- LinkedIn pre-check ---
    # Priority rule: if the message also contains a NON-LinkedIn URL, that URL wins as the
    # company domain; the LinkedIn URL is saved as a note. Only if there is no real URL do we
    # route to the dedicated LinkedIn handlers (person lead, or company-name poll).
    linkedin_info = extract_linkedin_info(text)
    linkedin_url_to_attach = None

    if linkedin_info:
        text_without_linkedin = strip_linkedin_urls(text)
        has_other_url = bool(re.search(r'https?://[^\s]+', text_without_linkedin))

        if has_other_url:
            # Real URL takes priority — strip LinkedIn, keep it as a note, fall through.
            linkedin_url_to_attach = linkedin_info["url"]
            text = text_without_linkedin
        elif linkedin_info["type"] == "person":
            logger.info(f"LinkedIn person URL detected: {linkedin_info['url']}")
            process_linkedin_person(linkedin_info, user_id, client, channel_id, thread_ts)
            return
        elif linkedin_info["type"] == "company":
            logger.info(f"LinkedIn company URL detected: {linkedin_info['url']}")
            seed = linkedin_info["name"] or clean_seed_text(text_without_linkedin)
            if not seed or len(seed) < MIN_POLL_MESSAGE_LENGTH:
                say(
                    text=(
                        f"<@{user_id}> I saw a LinkedIn company page but couldn't parse the name. "
                        f"Please reply with the company name or their website URL."
                    ),
                    unfurl_links=False,
                    unfurl_media=False,
                )
                return
            post_url_poll(
                client=client,
                channel=channel_id,
                thread_ts=thread_ts,
                poster_id=user_id,
                seed_text=seed,
                is_missed=is_missed,
                linkedin_url=linkedin_info["url"],
            )
            return
        else:
            # "other" LinkedIn URL (pulse, jobs, shortener) with no real URL alongside —
            # strip it, keep as a note, and fall through to the no-URL poll flow.
            linkedin_url_to_attach = linkedin_info["url"]
            text = text_without_linkedin

    # --- Branch 1: message contains a (non-LinkedIn) URL ---
    url_pattern = r'https?://[^\s]+'
    if re.search(url_pattern, text):
        logger.info(f"Processing message with URL: {text} (is_missed: {is_missed})")

        company_name, domain = extract_company_info(text)
        logger.info(f"Extracted - Name: {company_name}, Domain: {domain}")

        if not company_name and not domain:
            return

        note = None
        if linkedin_url_to_attach:
            note = f"LinkedIn: {linkedin_url_to_attach} (shared by <@{user_id}>)"

        result = process_company(
            company_name, domain,
            is_missed=is_missed,
            slack_user_id=user_id,
            note=note,
        )
        say(
            text=f"<@{user_id}> {result['message']}",
            unfurl_links=False,
            unfurl_media=False,
        )
        return

    # --- Branch 2: no URL — run search + poll flow ---
    seed = clean_seed_text(text)
    if len(seed) < MIN_POLL_MESSAGE_LENGTH:
        # Too short / probably a greeting — ignore
        return

    logger.info(f"No URL detected — launching URL search poll for seed='{seed}' (is_missed: {is_missed})")

    # Post the poll in-thread under the original message
    post_url_poll(
        client=client,
        channel=channel_id,
        thread_ts=thread_ts,
        poster_id=user_id,
        seed_text=seed,
        is_missed=is_missed,
        linkedin_url=linkedin_url_to_attach,
    )


# ========================================
# Poll button action handlers
# ========================================

@app.action(re.compile(r"^url_pick_\d+$"))
def handle_url_pick(ack, body, client):
    """User picked one of the candidate URLs from the poll."""
    ack()
    try:
        clicker_id = body["user"]["id"]
        channel_id = body["channel"]["id"]
        message_ts = body["message"]["ts"]
        action = body["actions"][0]
        payload = json.loads(action["value"])

        url = payload["url"]
        poster_id = payload.get("poster_id")
        is_missed = payload.get("is_missed", False)
        seed = payload.get("seed", "")
        linkedin_url = payload.get("linkedin_url")

        # Extract domain from the URL
        m = re.search(r"https?://(?:www\.)?([^/]+)", url)
        domain = m.group(1) if m else url
        # Use seed as company name fallback
        company_name = payload.get("name") or seed or domain.split(".")[0].title()

        # Build note text if we have a LinkedIn URL to attach
        note = None
        if linkedin_url:
            note = f"LinkedIn: {linkedin_url} (shared by <@{poster_id}>, resolved by <@{clicker_id}>)"

        # Owner = original poster (not the clicker)
        result = process_company(
            search_term=company_name,
            domain=domain,
            is_missed=is_missed,
            slack_user_id=poster_id,
            note=note,
        )

        resolved = (
            f"✅ <@{clicker_id}> picked *{domain}* for <@{poster_id}>'s post.\n"
            f"{result['message']}"
        )
        if linkedin_url:
            resolved += f"\n🔗 LinkedIn URL saved as a note on the org."
        disable_poll_message(client, channel_id, message_ts, resolved)
    except Exception as e:
        logger.error(f"Error in url_pick handler: {e}")
        try:
            client.chat_postMessage(
                channel=body["channel"]["id"],
                text=f"❌ Error processing that pick: {e}",
            )
        except Exception:
            pass


@app.action("url_stealth")
def handle_url_stealth(ack, body, client):
    """User marked the company as Stealth / no website."""
    ack()
    try:
        clicker_id = body["user"]["id"]
        channel_id = body["channel"]["id"]
        message_ts = body["message"]["ts"]
        payload = json.loads(body["actions"][0]["value"])

        poster_id = payload.get("poster_id")
        is_missed = payload.get("is_missed", False)
        seed = payload.get("seed", "").strip()
        linkedin_url = payload.get("linkedin_url")

        if not seed:
            disable_poll_message(
                client, channel_id, message_ts,
                "❌ Couldn't resolve as Stealth — no company name in the original message."
            )
            return

        note = f"Stealth — no website. Marked by <@{clicker_id}> via dealflow-bot."
        if linkedin_url:
            note += f"\nLinkedIn: {linkedin_url}"

        result = process_company(
            search_term=seed,
            domain=None,
            is_missed=is_missed,
            slack_user_id=poster_id,
            note=note,
        )

        resolved = (
            f"🕶️ <@{clicker_id}> marked *{seed}* as Stealth (no website) for <@{poster_id}>'s post.\n"
            f"{result['message']}"
        )
        if linkedin_url:
            resolved += f"\n🔗 LinkedIn URL saved as a note."
        disable_poll_message(client, channel_id, message_ts, resolved)
    except Exception as e:
        logger.error(f"Error in url_stealth handler: {e}")


@app.action("url_reply_later")
def handle_url_reply_later(ack, body, client):
    """User will reply in thread with the URL."""
    ack()
    try:
        clicker_id = body["user"]["id"]
        channel_id = body["channel"]["id"]
        message_ts = body["message"]["ts"]
        payload = json.loads(body["actions"][0]["value"])
        poster_id = payload.get("poster_id")

        resolved = (
            f"✍️ <@{clicker_id}> will post the URL for <@{poster_id}>'s lead. "
            f"No Affinity entry created yet — drop the URL in this channel and I'll pick it up."
        )
        disable_poll_message(client, channel_id, message_ts, resolved)
    except Exception as e:
        logger.error(f"Error in url_reply_later handler: {e}")


@app.event("app_mention")
def handle_mention(event, say):
    """Handle direct mentions of the bot."""
    say(
        text="👋 I'm monitoring #dealflow for company names. Post a company name there and I'll automatically add it to Affinity!",
        thread_ts=event.get("ts")
    )


if __name__ == "__main__":
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    logger.info("Starting Slack bot with nudge scheduler...")
    handler.start()
