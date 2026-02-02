#!/usr/bin/env node

/**
 * Affinity Field ID Finder
 * 
 * Run this script to find the field IDs you need for configuration.
 * 
 * Usage:
 *   node scripts/get-affinity-fields.js
 * 
 * Make sure AFFINITY_API_KEY is set in your environment or .env file
 */

require('dotenv').config();

const AFFINITY_API_KEY = process.env.AFFINITY_API_KEY;
const AFFINITY_BASE_URL = 'https://api.affinity.co';

async function affinityRequest(endpoint) {
  const url = `${AFFINITY_BASE_URL}${endpoint}`;
  const headers = {
    'Authorization': `Basic ${Buffer.from(':' + AFFINITY_API_KEY).toString('base64')}`,
    'Content-Type': 'application/json',
  };

  const response = await fetch(url, { method: 'GET', headers });
  
  if (!response.ok) {
    throw new Error(`Affinity API error: ${response.status}`);
  }

  return response.json();
}

async function main() {
  if (!AFFINITY_API_KEY) {
    console.error('Error: AFFINITY_API_KEY not set');
    console.error('Set it in your .env file or as an environment variable');
    process.exit(1);
  }

  console.log('Fetching Affinity lists...\n');

  // Get all lists
  const lists = await affinityRequest('/lists');
  
  console.log('='.repeat(60));
  console.log('YOUR AFFINITY LISTS');
  console.log('='.repeat(60));
  
  for (const list of lists) {
    console.log(`\nList: "${list.name}"`);
    console.log(`  ID: ${list.id}`);
    console.log(`  Type: ${list.type === 0 ? 'Organization' : list.type === 1 ? 'Person' : 'Opportunity'}`);
    
    // Get fields for this list
    const fields = await affinityRequest(`/lists/${list.id}/fields`);
    
    if (fields.length > 0) {
      console.log('  Fields:');
      for (const field of fields) {
        console.log(`    - "${field.name}" (ID: ${field.id}, Type: ${field.value_type})`);
        
        // If it's a dropdown, show the options
        if (field.dropdown_options && field.dropdown_options.length > 0) {
          console.log('      Dropdown options:');
          for (const option of field.dropdown_options) {
            console.log(`        * "${option.text}" (Value ID: ${option.id})`);
          }
        }
      }
    }
  }

  console.log('\n' + '='.repeat(60));
  console.log('WHAT YOU NEED FOR .env');
  console.log('='.repeat(60));
  console.log(`
Find your deal pipeline list above and note:
  1. AFFINITY_DEAL_LIST_ID = [The list ID]
  2. AFFINITY_SOURCE_FIELD_ID = [ID of your "Source" field]
  3. AFFINITY_SOURCE_NOTE_FIELD_ID = [ID of your "Source Note" field]
  4. AFFINITY_SOURCE_INBOUND_VALUE_ID = [Value ID for "inbound" if Source is a dropdown]
`);
}

main().catch(console.error);
