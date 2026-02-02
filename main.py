import os
import re
import logging
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN")
AFFINITY_API_KEY = os.environ.get("AFFINITY_API_KEY")
AFFINITY_LIST_ID = os.environ.get("AFFINITY_LIST_ID")

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
    company_name = company_name.strip(' -–—:/')
    
    # If we have a domain, use it as the search term
    if domain:
        # Remove trailing paths from domain
        domain = domain.split('/')[0]
        # Use domain as company name if text is just a URL
        if not company_name or company_name == domain:
            company_name = domain.split('.')[0].title()
    
    return company_name, domain


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


def process_company(search_term, domain=None):
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
                # Already in pipeline - get current stage
                stage = get_stage_name(org_id, AFFINITY_LIST_ID)
                return {
                    "status": "exists",
                    "company": org_name,
                    "stage": stage,
                    "message": f"*{org_name}* is already in the deal pipeline.\n📊 Current stage: *{stage}*"
                }
            else:
                # Org exists but not in pipeline - add it
                affinity.add_to_list(AFFINITY_LIST_ID, org_id)
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
            
            affinity.add_to_list(AFFINITY_LIST_ID, org_id)
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


@app.event("message")
def handle_message(event, say, client):
    """Handle messages posted to #dealflow channel."""
    if event.get("subtype") in ["bot_message", "message_changed", "message_deleted"]:
        return
    
    channel_id = event.get("channel")
    try:
        channel_info = client.conversations_info(channel=channel_id)
        channel_name = channel_info["channel"]["name"]
        
        if channel_name != "dealflow":
            return
    except Exception as e:
        logger.error(f"Error getting channel info: {e}")
        return
    
    text = event.get("text", "").strip()
    if not text:
        return
    
    logger.info(f"Processing message: {text}")
    company_name, domain = extract_company_info(text)
    logger.info(f"Extracted - Name: {company_name}, Domain: {domain}")
    
    if not company_name and not domain:
        return
    
    result = process_company(company_name, domain)
    
    say(text=result["message"])


@app.event("app_mention")
def handle_mention(event, say):
    """Handle direct mentions of the bot."""
    say(
        text="👋 I'm monitoring #dealflow for company names. Post a company name there and I'll automatically add it to Affinity!",
        thread_ts=event.get("ts")
    )


if __name__ == "__main__":
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    logger.info("Starting Slack bot...")
    handler.start()
