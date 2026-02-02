/**
 * Deals Email Routes
 * 
 * Express routes for:
 * - Manual trigger of email processing
 * - Webhook endpoint for Gmail push notifications (optional)
 * - Health check
 */

const express = require('express');
const router = express.Router();
const { processDealsEmails, extractCompanyInfo } = require('./deals-email-handler');
const { getHealthStatus } = require('./deals-email-scheduler');

/**
 * GET /deals-email/health
 * Health check endpoint
 */
router.get('/health', (req, res) => {
  res.json(getHealthStatus());
});

/**
 * POST /deals-email/process
 * Manually trigger email processing
 */
router.post('/process', async (req, res) => {
  console.log('Manual processing triggered');
  
  try {
    const results = await processDealsEmails();
    res.json({
      success: true,
      message: 'Processing complete',
      results,
    });
  } catch (error) {
    console.error('Manual processing failed:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

/**
 * POST /deals-email/webhook
 * Gmail push notification webhook
 * 
 * If using Gmail push notifications instead of polling,
 * configure this endpoint in Google Cloud Console.
 */
router.post('/webhook', async (req, res) => {
  console.log('Gmail webhook received');
  
  // Gmail sends a base64-encoded message
  const message = req.body.message;
  if (message && message.data) {
    const data = JSON.parse(Buffer.from(message.data, 'base64').toString());
    console.log('Webhook data:', data);
  }

  // Acknowledge receipt immediately
  res.status(200).send('OK');

  // Process emails asynchronously
  try {
    await processDealsEmails();
  } catch (error) {
    console.error('Webhook processing failed:', error);
  }
});

/**
 * POST /deals-email/test-extraction
 * Test the AI extraction with sample email content
 */
router.post('/test-extraction', async (req, res) => {
  const { subject, from, body } = req.body;
  
  if (!subject || !from || !body) {
    return res.status(400).json({
      success: false,
      error: 'Missing required fields: subject, from, body',
    });
  }

  try {
    const result = await extractCompanyInfo({ subject, from, body });
    res.json({
      success: true,
      extracted: result,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

module.exports = router;
