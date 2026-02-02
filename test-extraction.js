#!/usr/bin/env node

/**
 * Test the AI extraction logic
 * 
 * Usage:
 *   node scripts/test-extraction.js
 */

require('dotenv').config();
const { extractCompanyInfo } = require('../src/deals-email-handler');

const testEmails = [
  {
    name: 'Typical pitch email',
    subject: 'Partnership opportunity - CleanTech Solutions',
    from: 'John Smith <john@cleantech-solutions.com>',
    body: `Hi team,

I'm the founder of CleanTech Solutions, and I think we'd be a great fit for your portfolio.

We're building AI-powered energy management systems for commercial buildings. 
We've already saved our customers over $10M in energy costs.

Would love to chat about potential partnership or investment opportunities.

Best,
John Smith
CEO, CleanTech Solutions
www.cleantech-solutions.com
john@cleantech-solutions.com`,
  },
  {
    name: 'Email with URL in body',
    subject: 'Intro to Overture',
    from: 'Sarah Lee <sarah@gmail.com>',
    body: `Hello,

I wanted to introduce you to our company EcoWaste (https://ecowaste.io).

We're revolutionizing waste management through smart bin technology.

Check out our website: https://ecowaste.io

Thanks,
Sarah`,
  },
  {
    name: 'Minimal information email',
    subject: 'Quick question',
    from: 'info@mystery-startup.co',
    body: `Hi, we saw your portfolio and would love to connect. Can we schedule a call?`,
  },
];

async function runTests() {
  console.log('Testing AI extraction...\n');

  for (const testEmail of testEmails) {
    console.log('='.repeat(50));
    console.log(`Test: ${testEmail.name}`);
    console.log('='.repeat(50));
    console.log(`Subject: ${testEmail.subject}`);
    console.log(`From: ${testEmail.from}`);
    console.log('');

    try {
      const result = await extractCompanyInfo(testEmail);
      console.log('Extracted:');
      console.log(JSON.stringify(result, null, 2));
    } catch (error) {
      console.error('Error:', error.message);
    }
    console.log('\n');
  }
}

runTests().catch(console.error);
