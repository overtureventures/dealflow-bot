/**
 * Deals Email Handler
 * 
 * Automatically processes emails sent to deals@overture.eco and adds
 * companies to the Affinity deal pipeline.
 * 
 * Flow:
 * 1. Fetch new emails from Gmail
 * 2. Extract company URL/name using AI
 * 3. Search Affinity for existing organization
 * 4. Create org if not found, then add to pipeline
 */

const { google } = require('googleapis');
const OpenAI = require('openai');

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Affinity API configuration
const AFFINITY_API_KEY = process.env.AFFINITY_API_KEY;
const AFFINITY_BASE_URL = 'https://api.affinity.co';

// Affinity Field IDs
const AFFINITY_CONFIG = {
  DEAL_LIST_ID: process.env.AFFINITY_DEAL_LIST_ID,
  SOURCE_FIELD_ID: process.env.AFFINITY_SOURCE_FIELD_ID,
  SOURCE_NOTE_FIELD_ID: process.env.AFFINITY_SOURCE_NOTE_FIELD_ID,
  SOURCE_INBOUND_VALUE_ID: process.env.AFFINITY_SOURCE_INBOUND_VALUE_ID,
};

// Gmail configuration
const PROCESSED_LABEL = 'Processed-Deals';
const DEALS_ALIAS = 'deals@overture.eco';

/**
 * Initialize Gmail API client using OAuth
 */
async function getGmailClient() {
  const oauth2Client = new google.auth.OAuth2(
    process.env.GMAIL_CLIENT_ID,
    process.env.GMAIL_CLIENT_SECRET
  );

  oauth2Client.setCredentials({
    refresh_token: process.env.GMAIL_REFRESH_TOKEN,
  });

  return google.gmail({ version: 'v1', auth: oauth2Client });
}

/**
 * Fetch unprocessed emails sent to deals@overture.eco
 */
async function fetchNewEmails(gmail) {
  try {
    // Search for unread emails sent to deals@ alias
    const response = await gmail.users.messages.list({
      userId: 'me',
      q: `to:${DEALS_ALIAS} is:unread -label:${PROCESSED_LABEL}`,
      maxResults: 50,
    });

    if (!response.data.messages) {
      console.log('No new emails to process');
      return [];
    }

    const emails = [];
    for (const message of response.data.messages) {
      const email = await gmail.users.messages.get({
        userId: 'me',
        id: message.id,
        format: 'full',
      });
      emails.push(email.data);
    }

    return emails;
  } catch (error) {
    console.error('Error fetching emails:', error);
    throw error;
  }
}

/**
 * Extract email content (subject, body, sender)
 */
function parseEmailContent(email) {
  const headers = email.payload.headers;
  const subject = headers.find(h => h.name.toLowerCase() === 'subject')?.value || '';
  const from = headers.find(h => h.name.toLowerCase() === 'from')?.value || '';
  const date = headers.find(h => h.name.toLowerCase() === 'date')?.value || '';

  // Extract body
  let body = '';
  if (email.payload.body?.data) {
    body = Buffer.from(email.payload.body.data, 'base64').toString('utf-8');
  } else if (email.payload.parts) {
    const textPart = email.payload.parts.find(
      p => p.mimeType === 'text/plain' || p.mimeType === 'text/html'
    );
    if (textPart?.body?.data) {
      body = Buffer.from(textPart.body.data, 'base64').toString('utf-8');
    }
  }

  // Clean HTML if present
  body = body.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

  return { subject, from, body, date, messageId: email.id };
}

/**
 * Use GPT-4o-mini to extract company information from email
 */
async function extractCompanyInfo(emailContent) {
  const prompt = `Analyze this email and extract the company information being pitched or mentioned.

Email Subject: ${emailContent.subject}
From: ${emailContent.from}
Body: ${emailContent.body.substring(0, 3000)}

Extract and return a JSON object with:
1. "company_url": The company's website URL if mentioned (e.g., "https://example.com"). Look for URLs in email signatures, body text, or the sender's domain. Return null if not found.
2. "company_name": The company name. If no explicit name, infer from the domain or email content.
3. "company_domain": Just the domain (e.g., "example.com") extracted from URL or email
4. "sender_name": Name of the person who sent the email
5. "sender_email": Email address of sender
6. "brief_description": A one-sentence description of what the company does based on the email

Respond ONLY with valid JSON, no markdown or explanation.`;

  try {
    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.1,
      max_tokens: 500,
    });

    const content = response.choices[0].message.content;
    return JSON.parse(content);
  } catch (error) {
    console.error('Error extracting company info:', error);
    // Fallback: try to extract domain from sender email
    const emailMatch = emailContent.from.match(/<(.+@(.+))>/) || emailContent.from.match(/(.+@(.+))/);
    if (emailMatch) {
      return {
        company_url: null,
        company_name: emailMatch[2].split('.')[0],
        company_domain: emailMatch[2],
        sender_email: emailMatch[1],
        sender_name: emailContent.from.split('<')[0].trim() || emailMatch[1],
        brief_description: 'Extracted from email sender',
      };
    }
    throw error;
  }
}

/**
 * Make authenticated request to Affinity API
 */
async function affinityRequest(endpoint, method = 'GET', body = null) {
  const url = `${AFFINITY_BASE_URL}${endpoint}`;
  const headers = {
    'Authorization': `Basic ${Buffer.from(':' + AFFINITY_API_KEY).toString('base64')}`,
    'Content-Type': 'application/json',
  };

  const options = { method, headers };
  if (body) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(url, options);
  
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Affinity API error: ${response.status} - ${errorText}`);
  }

  return response.json();
}

/**
 * Search for organization in Affinity by domain or name
 */
async function searchAffinityOrganization(companyInfo) {
  // First, try to search by domain
  if (companyInfo.company_domain) {
    try {
      const results = await affinityRequest(
        `/organizations?term=${encodeURIComponent(companyInfo.company_domain)}`
      );
      if (results.organizations?.length > 0) {
        console.log(`Found organization by domain: ${results.organizations[0].name}`);
        return results.organizations[0];
      }
    } catch (error) {
      console.log('Domain search failed, trying name search');
    }
  }

  // Fallback: search by company name
  if (companyInfo.company_name) {
    try {
      const results = await affinityRequest(
        `/organizations?term=${encodeURIComponent(companyInfo.company_name)}`
      );
      if (results.organizations?.length > 0) {
        console.log(`Found organization by name: ${results.organizations[0].name}`);
        return results.organizations[0];
      }
    } catch (error) {
      console.log('Name search failed');
    }
  }

  return null;
}

/**
 * Create new organization in Affinity
 */
async function createAffinityOrganization(companyInfo) {
  const orgData = {
    name: companyInfo.company_name,
    domain: companyInfo.company_domain,
  };

  console.log(`Creating new organization: ${companyInfo.company_name}`);
  const org = await affinityRequest('/organizations', 'POST', orgData);
  return org;
}

/**
 * Check if organization is already in the deal pipeline
 */
async function isOrgInDealPipeline(organizationId) {
  try {
    const listEntries = await affinityRequest(
      `/list-entries?list_id=${AFFINITY_CONFIG.DEAL_LIST_ID}&entity_id=${organizationId}`
    );
    return listEntries.list_entries?.length > 0;
  } catch (error) {
    console.log('Error checking list entries:', error.message);
    return false;
  }
}

/**
 * Add organization to deal pipeline list
 */
async function addToDealPipeline(organizationId) {
  // Check if already in pipeline
  const alreadyInPipeline = await isOrgInDealPipeline(organizationId);
  if (alreadyInPipeline) {
    console.log(`Organization ${organizationId} already in deal pipeline, skipping`);
    return null;
  }

  const listEntryData = {
    list_id: parseInt(AFFINITY_CONFIG.DEAL_LIST_ID),
    entity_id: organizationId,
    entity_type: 0, // 0 = Organization
  };

  console.log(`Adding organization ${organizationId} to deal pipeline`);
  const listEntry = await affinityRequest('/list-entries', 'POST', listEntryData);
  return listEntry;
}

/**
 * Set field values on a list entry (Source and Source Note)
 */
async function setListEntryFields(listEntryId, companyInfo) {
  const fieldValues = [];

  // Set Source field to "inbound" (dropdown)
  if (AFFINITY_CONFIG.SOURCE_FIELD_ID && AFFINITY_CONFIG.SOURCE_INBOUND_VALUE_ID) {
    fieldValues.push({
      field_id: parseInt(AFFINITY_CONFIG.SOURCE_FIELD_ID),
      list_entry_id: listEntryId,
      value: parseInt(AFFINITY_CONFIG.SOURCE_INBOUND_VALUE_ID),
    });
  }

  // Set Source Note field to "deals@"
  if (AFFINITY_CONFIG.SOURCE_NOTE_FIELD_ID) {
    fieldValues.push({
      field_id: parseInt(AFFINITY_CONFIG.SOURCE_NOTE_FIELD_ID),
      list_entry_id: listEntryId,
      value: `deals@ - ${companyInfo.sender_name || 'Unknown'} (${companyInfo.sender_email || 'Unknown'})`,
    });
  }

  // Create field values
  for (const fieldValue of fieldValues) {
    try {
      await affinityRequest('/field-values', 'POST', fieldValue);
      console.log(`Set field ${fieldValue.field_id} for list entry ${listEntryId}`);
    } catch (error) {
      console.error(`Error setting field ${fieldValue.field_id}:`, error.message);
    }
  }
}

/**
 * Mark email as processed in Gmail
 */
async function markEmailProcessed(gmail, messageId) {
  try {
    // First, ensure the processed label exists
    let labelId;
    const labels = await gmail.users.labels.list({ userId: 'me' });
    const existingLabel = labels.data.labels.find(l => l.name === PROCESSED_LABEL);
    
    if (existingLabel) {
      labelId = existingLabel.id;
    } else {
      const newLabel = await gmail.users.labels.create({
        userId: 'me',
        requestBody: { name: PROCESSED_LABEL },
      });
      labelId = newLabel.data.id;
    }

    // Add processed label and mark as read
    await gmail.users.messages.modify({
      userId: 'me',
      id: messageId,
      requestBody: {
        addLabelIds: [labelId],
        removeLabelIds: ['UNREAD'],
      },
    });
    console.log(`Marked email ${messageId} as processed`);
  } catch (error) {
    console.error('Error marking email as processed:', error);
  }
}

/**
 * Process a single email
 */
async function processEmail(gmail, email) {
  const emailContent = parseEmailContent(email);
  console.log(`\nProcessing email: "${emailContent.subject}" from ${emailContent.from}`);

  try {
    // Step 1: Extract company information using AI
    const companyInfo = await extractCompanyInfo(emailContent);
    console.log('Extracted company info:', companyInfo);

    if (!companyInfo.company_name && !companyInfo.company_domain) {
      console.log('Could not extract company information, skipping');
      await markEmailProcessed(gmail, emailContent.messageId);
      return { success: false, reason: 'No company info extracted' };
    }

    // Step 2: Search for existing organization in Affinity
    let organization = await searchAffinityOrganization(companyInfo);

    // Step 3: Create organization if not found
    if (!organization) {
      organization = await createAffinityOrganization(companyInfo);
    }

    // Step 4: Add to deal pipeline
    const listEntry = await addToDealPipeline(organization.id);

    // Step 5: Set Source and Source Note fields (only if newly added)
    if (listEntry) {
      await setListEntryFields(listEntry.id, companyInfo);
    }

    // Step 6: Mark email as processed
    await markEmailProcessed(gmail, emailContent.messageId);

    console.log(`✓ Successfully processed: ${companyInfo.company_name}`);
    return {
      success: true,
      company: companyInfo.company_name,
      organizationId: organization.id,
      listEntryId: listEntry?.id,
      alreadyInPipeline: !listEntry,
    };
  } catch (error) {
    console.error(`Error processing email "${emailContent.subject}":`, error);
    return { success: false, reason: error.message };
  }
}

/**
 * Main handler - process all new emails
 */
async function processDealsEmails() {
  console.log('Starting deals email processing...');
  console.log('Timestamp:', new Date().toISOString());

  const gmail = await getGmailClient();
  const emails = await fetchNewEmails(gmail);

  console.log(`Found ${emails.length} new emails to process`);

  const results = {
    processed: 0,
    failed: 0,
    skipped: 0,
    details: [],
  };

  for (const email of emails) {
    const result = await processEmail(gmail, email);
    if (result.success) {
      if (result.alreadyInPipeline) {
        results.skipped++;
      } else {
        results.processed++;
      }
    } else {
      results.failed++;
    }
    results.details.push(result);

    // Small delay between processing to avoid rate limits
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  console.log(`\nProcessing complete: ${results.processed} added, ${results.skipped} skipped, ${results.failed} failed`);
  return results;
}

// Export for use in main app
module.exports = {
  processDealsEmails,
  extractCompanyInfo,
  searchAffinityOrganization,
  createAffinityOrganization,
  addToDealPipeline,
};
