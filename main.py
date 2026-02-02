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

    def search_organization(self, company_name):
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/organizations",
            params={"term": company_name}
        )
        response.raise_for_status()
        return response.json().get("organizations", [])

    def get_list_entries(self, list_id, organization_id):
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/lists/{list_id}/list-entries",
            params={"organization_id": organization_id}
        )
        response.raise_for_status()
        return response.json().get("list_entries", [])

    def get_field_values(self, list_entry_id):
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/field-values",
            params={"list_entry_id": list_entry_id}
        )
        response.raise_for_status()
        return response.json().get("field_values", [])

    def get_list_fields(self, list_id):
        response = self.session.get(
            f"{AFFINITY_BASE_URL}/lists/{list_id}/fields"
        )
        response.raise_for_status()
        return response.json()

    def create_organization(self, name, domain=None):
        data = {"name": name}
        if domain:
            data["domain"] = domain
        response = self.session.post(
            f"{AFFINITY_BASE_URL}/organizations",
            json=data
        )
        response.raise_for_status()
        return response.json()

    def add_to_list(self, list_id, organization_id):
        response = self.session.post(
            f"{AFFINITY_BASE_URL}/lists/{list_id}/list-entries",
            json={"entity_id": organization_id}
        )
        response.raise_for_status()
        return response.json()


affinity = AffinityClient(AFFINITY_API_KEY)


def extract_company_info(text):
    url_pattern = r'https?://(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z]{2,})+)'
    domain_pattern = r'\b([a-zA-Z0-9-]+\.(?:com|io|co|ai|org|net|app))\b'
    
    domain = None
    url_match = re.search(url_pattern, text)
    if url_match:
        domain = url_match.group(1)
    else:
        domain_match = re.search(domain_pattern, text)
        if domain_match:
            domain = domain_match.group(1)
    
    company_name = text.strip()
    company_name = re.sub(url_pattern, '', company_name)
    company_name = re.sub(r'https?://\S+', '', company_name)
    company_name = re.sub(r'\([^)]*\)', '', company_name)
    company_name = re.sub(r'\s*[-–—]\s*\S+\.\w+', '', company_name)
    company_name = company_name.strip(' -–—:')
    
    if not company_name and domain:
        company_name = domain.split('.')[0].title()
    
    return company_name, domain


def get_stage_name(list_entry_id, list_id):
    try:
        fields = affinity.get_list_fields(list_id)
        stage_field_id = None
        stage_options = {}
        
        for field in fields:
            if field.get("name", "").lower() in ["stage", "status", "deal stage"]:
                stage_field_id = field.get("id")
                for option in field.get("dropdown_options", []):
                    stage_options[option["id"]] = option["text"]
                break
        
        if not stage_field_id:
            return "Unknown"
        
        field_values = affinity.get_field_values(list_entry_id)
        
        for fv in field_values:
            if fv.get("field_id") == stage_field_id:
                value = fv.get("value")
                if isinstance(value, int) and value in stage_options:
                    return stage_options[value]
                return str(value) if value else "Not set"
        
        return "Not set"
    except Exception as e:
        logger.error(f"Error getting stage: {e}")
        return "Unknown"


def process_company(company_name, domain=None):
    try:
        orgs = affinity.search_organization(company_name)
        
        organization = None
        if orgs:
            for org in orgs:
                if org["name"].lower() == company_name.lower():
                    organization = org
                    break
            if not organization:
                organization = orgs[0]
        
        if organization:
            org_id = organization["id"]
            list_entries = affinity.get_list_entries(AFFINITY_LIST_ID, org_id)
            
            if list_entries:
                entry = list_entries[0]
                stage = get_stage_name(entry["id"], AFFINITY_LIST_ID)
                return {
                    "status": "exists",
                    "company": organization["name"],
                    "stage": stage,
                    "message": f"*{organization['name']}* is already in the deal pipeline.\n📊 Current stage: *{stage}*"
                }
            else:
                affinity.add_to_list(AFFINITY_LIST_ID, org_id)
                return {
                    "status": "added",
                    "company": organization["name"],
                    "message": f"✅ Added *{organization['name']}* to the deal pipeline as a new lead."
                }
        else:
            new_org = affinity.create_organization(company_name, domain)
            affinity.add_to_list(AFFINITY_LIST_ID, new_org["id"])
            return {
                "status": "created",
                "company": new_org["name"],
                "message": f"✅ Created *{new_org['name']}* and added to the deal pipeline as a new lead."
            }
            
    except requests.exceptions.HTTPError as e:
        logger.error(f"Affinity API error: {e.response.text}")
        return {
            "status": "error",
            "message": f"❌ Error processing company: {e.response.status_code}"
        }
    except Exception as e:
        logger.error(f"Error processing company: {e}")
        return {
            "status": "error",
            "message": f"❌ Error processing company: {str(e)}"
        }


@app.event("message")
def handle_message(event, say, client):
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
    
    company_name, domain = extract_company_info(text)
    
    if not company_name:
        return
    
    result = process_company(company_name, domain)
    
    say(
        text=result["message"],
        thread_ts=event.get("ts")
    )


@app.event("app_mention")
def handle_mention(event, say):
    say(
        text="👋 I'm monitoring #dealflow for company names. Post a company name there and I'll automatically add it to Affinity!",
        thread_ts=event.get("ts")
    )


if __name__ == "__main__":
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    logger.info("Starting Slack bot...")
    handler.start()
