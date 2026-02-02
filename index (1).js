/**
 * Dealflow Bot - Main Entry Point
 * 
 * This file integrates the deals email automation into the existing dealflow-bot.
 * If you have existing functionality, add the deals-email routes and scheduler.
 */

require('dotenv').config();
const express = require('express');
const dealsEmailRoutes = require('./deals-email-routes');
const { startScheduler } = require('./deals-email-scheduler');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());

// ============================================
// ROUTES
// ============================================

// Health check for the main app
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    services: {
      dealsEmail: 'active',
    },
  });
});

// Deals email automation routes
app.use('/deals-email', dealsEmailRoutes);

// ============================================
// ADD YOUR EXISTING ROUTES HERE
// ============================================
// Example:
// const existingRoutes = require('./existing-routes');
// app.use('/api', existingRoutes);

// ============================================
// START SERVER
// ============================================
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
  console.log(`Deals email health: http://localhost:${PORT}/deals-email/health`);
  console.log(`Manual trigger: POST http://localhost:${PORT}/deals-email/process`);
  console.log('');
  
  // Start the deals email scheduler
  startScheduler();
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('SIGINT received, shutting down gracefully');
  process.exit(0);
});
