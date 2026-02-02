/**
 * Deals Email Scheduler
 * 
 * Sets up scheduled processing of deals@overture.eco emails.
 * Can be run as a standalone service or integrated into existing dealflow-bot.
 */

const cron = require('node-cron');
const { processDealsEmails } = require('./deals-email-handler');

// Process interval (in minutes)
const PROCESS_INTERVAL = process.env.DEALS_EMAIL_INTERVAL || 5;

/**
 * Initialize the scheduler
 */
function startScheduler() {
  console.log('='.repeat(50));
  console.log('Deals Email Automation Started');
  console.log('='.repeat(50));
  console.log(`Processing interval: Every ${PROCESS_INTERVAL} minutes`);
  console.log(`Started at: ${new Date().toISOString()}`);
  console.log('');

  // Run immediately on startup
  console.log('Running initial processing...');
  processDealsEmails().catch(console.error);

  // Schedule recurring processing
  // Cron format: */5 * * * * = every 5 minutes
  cron.schedule(`*/${PROCESS_INTERVAL} * * * *`, async () => {
    console.log('\n' + '='.repeat(50));
    console.log('Scheduled run started');
    console.log('='.repeat(50));
    
    try {
      await processDealsEmails();
    } catch (error) {
      console.error('Scheduled processing failed:', error);
    }
  });

  console.log('Scheduler initialized. Waiting for next run...');
}

// Health check endpoint data
function getHealthStatus() {
  return {
    service: 'deals-email-automation',
    status: 'running',
    interval: `${PROCESS_INTERVAL} minutes`,
    timestamp: new Date().toISOString(),
  };
}

module.exports = {
  startScheduler,
  getHealthStatus,
};

// If run directly (not imported)
if (require.main === module) {
  startScheduler();
}
