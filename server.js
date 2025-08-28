// Rate Alerts & Market Orders Tool for Railway
// This tool monitors currency rates and triggers WhatsApp alerts or creates deals

const express = require('express');
const { Pool } = require('pg');
const cron = require('node-cron');
const axios = require('axios');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(express.static('public'));

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Initialize database table
async function initializeDatabase() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS rate_monitors (
      id SERIAL PRIMARY KEY,
      pd_id VARCHAR(255) NOT NULL,
      sell_currency VARCHAR(3) NOT NULL,
      buy_currency VARCHAR(3) NOT NULL,
      sell_amount DECIMAL(15,2),
      buy_amount DECIMAL(15,2),
      target_client_rate DECIMAL(10,6) NOT NULL,
      target_market_rate DECIMAL(10,6) NOT NULL,
      alert_or_order VARCHAR(10) NOT NULL CHECK (alert_or_order IN ('alert', 'order')),
      status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'triggered', 'cancelled')),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      triggered_at TIMESTAMP,
      current_rate DECIMAL(10,6),
      last_checked TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_currency_pair ON rate_monitors(sell_currency, buy_currency);
    CREATE INDEX IF NOT EXISTS idx_status ON rate_monitors(status);
  `;
  
  try {
    await pool.query(createTableQuery);
    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Database initialization error:', error);
  }
}

// Rate fetching service using OpenExchangeRates API
class RateService {
  constructor() {
    this.baseURL = 'https://openexchangerates.org/api/latest.json';
    this.apiKey = process.env.OPENEXCHANGERATES_API_KEY;
  }
  
  async getRate(fromCurrency, toCurrency) {
    try {
      // OpenExchangeRates uses USD as base currency
      const response = await axios.get(`${this.baseURL}?app_id=${this.apiKey}`);
      const rates = response.data.rates;
      
      if (fromCurrency === 'USD') {
        return rates[toCurrency] || null;
      } else if (toCurrency === 'USD') {
        return 1 / (rates[fromCurrency] || 1);
      } else {
        // For non-USD pairs, convert through USD
        const fromRate = rates[fromCurrency];
        const toRate = rates[toCurrency];
        
        if (!fromRate || !toRate) {
          console.error(`Rate not found for ${fromCurrency} or ${toCurrency}`);
          return null;
        }
        
        return toRate / fromRate;
      }
    } catch (error) {
      console.error(`Error fetching rate for ${fromCurrency}/${toCurrency}:`, error);
      if (error.response?.status === 401) {
        console.error('OpenExchangeRates API key invalid or missing');
      } else if (error.response?.status === 429) {
        console.error('OpenExchangeRates API rate limit exceeded');
      }
      return null;
    }
  }
  
  // Optional: Get multiple rates in one call for efficiency
  async getRates(currencyPairs) {
    try {
      const response = await axios.get(`${this.baseURL}?app_id=${this.apiKey}`);
      const rates = response.data.rates;
      const results = {};
      
      for (const pair of currencyPairs) {
        const { from, to } = pair;
        
        if (from === 'USD') {
          results[`${from}${to}`] = rates[to] || null;
        } else if (to === 'USD') {
          results[`${from}${to}`] = 1 / (rates[from] || 1);
        } else {
          const fromRate = rates[from];
          const toRate = rates[to];
          results[`${from}${to}`] = (fromRate && toRate) ? toRate / fromRate : null;
        }
      }
      
      return results;
    } catch (error) {
      console.error('Error fetching multiple rates:', error);
      return {};
    }
  }
}

// WhatsApp service integration
class WhatsAppService {
  constructor() {
    this.apiKey = process.env.WHATSAPP_API_KEY;
    this.baseURL = process.env.WHATSAPP_BASE_URL || 'https://graph.facebook.com/v17.0/';
  }
  
  async sendTemplateMessage(phoneNumber, templateName, parameters) {
    try {
      const payload = {
        messaging_product: "whatsapp",
        to: phoneNumber,
        type: "template",
        template: {
          name: templateName,
          language: { code: "en" },
          components: [{
            type: "body",
            parameters: parameters.map(param => ({ type: "text", text: param }))
          }]
        }
      };
      
      const response = await axios.post(
        `${this.baseURL}${process.env.WHATSAPP_PHONE_NUMBER_ID}/messages`,
        payload,
        {
          headers: {
            'Authorization': `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json'
          }
        }
      );
      
      return response.data;
    } catch (error) {
      console.error('WhatsApp message error:', error);
      throw error;
    }
  }
}

// CRM/Pipeline service (example for a generic CRM API)
class PipelineService {
  constructor() {
    this.apiKey = process.env.CRM_API_KEY;
    this.baseURL = process.env.CRM_BASE_URL;
  }
  
  async createDeal(dealData) {
    try {
      const payload = {
        title: `${dealData.sellCurrency}/${dealData.buyCurrency} Market Order`,
        person_id: dealData.pdId,
        value: dealData.sellAmount || dealData.buyAmount,
        currency: dealData.sellCurrency,
        stage_id: process.env.CRM_TRADES_STAGE_ID,
        custom_fields: {
          sell_currency: dealData.sellCurrency,
          buy_currency: dealData.buyCurrency,
          target_rate: dealData.targetClientRate,
          current_rate: dealData.currentRate
        }
      };
      
      const response = await axios.post(
        `${this.baseURL}/deals`,
        payload,
        {
          headers: {
            'Authorization': `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json'
          }
        }
      );
      
      return response.data;
    } catch (error) {
      console.error('CRM deal creation error:', error);
      throw error;
    }
  }
}

const rateService = new RateService();
const whatsappService = new WhatsAppService();
const pipelineService = new PipelineService();

// Core monitoring function
async function checkRates() {
  console.log('Starting rate check...', new Date().toISOString());
  
  try {
    const activeMonitors = await pool.query(
      'SELECT * FROM rate_monitors WHERE status = $1',
      ['active']
    );
    
    for (const monitor of activeMonitors.rows) {
      const currentRate = await rateService.getRate(
        monitor.sell_currency, 
        monitor.buy_currency
      );
      
      if (!currentRate) {
        console.log(`Could not fetch rate for ${monitor.sell_currency}/${monitor.buy_currency}`);
        continue;
      }
      
      // Update current rate in database
      await pool.query(
        'UPDATE rate_monitors SET current_rate = $1, last_checked = CURRENT_TIMESTAMP WHERE id = $2',
        [currentRate, monitor.id]
      );
      
      // Check if target rate is met
      const targetMet = currentRate <= monitor.target_market_rate;
      
      if (targetMet) {
        console.log(`Target met for monitor ${monitor.id}: ${currentRate} <= ${monitor.target_market_rate}`);
        
        if (monitor.alert_or_order === 'alert') {
          await handleAlert(monitor, currentRate);
        } else {
          await handleOrder(monitor, currentRate);
        }
        
        // Mark as triggered
        await pool.query(
          'UPDATE rate_monitors SET status = $1, triggered_at = CURRENT_TIMESTAMP WHERE id = $2',
          ['triggered', monitor.id]
        );
      }
    }
    
    console.log('Rate check completed');
  } catch (error) {
    console.error('Rate check error:', error);
  }
}

async function handleAlert(monitor, currentRate) {
  try {
    // Get client phone number (you'll need to modify this based on your client data structure)
    const clientData = await getClientData(monitor.pd_id);
    
    await whatsappService.sendTemplateMessage(
      clientData.phone,
      'rate_alert_template', // You'll need to create this template in WhatsApp Business
      [
        `${monitor.sell_currency}/${monitor.buy_currency}`,
        currentRate.toFixed(6),
        monitor.target_client_rate.toFixed(6)
      ]
    );
    
    console.log(`Alert sent for monitor ${monitor.id}`);
  } catch (error) {
    console.error(`Alert handling error for monitor ${monitor.id}:`, error);
  }
}

async function handleOrder(monitor, currentRate) {
  try {
    const dealData = {
      pdId: monitor.pd_id,
      sellCurrency: monitor.sell_currency,
      buyCurrency: monitor.buy_currency,
      sellAmount: monitor.sell_amount,
      buyAmount: monitor.buy_amount,
      targetClientRate: monitor.target_client_rate,
      currentRate: currentRate
    };
    
    await pipelineService.createDeal(dealData);
    console.log(`Deal created for monitor ${monitor.id}`);
  } catch (error) {
    console.error(`Order handling error for monitor ${monitor.id}:`, error);
  }
}

async function getClientData(pdId) {
  // Implement this based on your client data storage
  // This is a placeholder - you'll need to integrate with your client database
  return {
    phone: '+1234567890', // Replace with actual client phone lookup
    name: 'Client Name'
  };
}

// API Routes

// Get all monitors
app.get('/api/monitors', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM rate_monitors ORDER BY created_at DESC'
    );
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Create new monitor
app.post('/api/monitors', async (req, res) => {
  const {
    pdId,
    sellCurrency,
    buyCurrency,
    sellAmount,
    buyAmount,
    targetClientRate,
    targetMarketRate,
    alertOrOrder
  } = req.body;
  
  try {
    const result = await pool.query(
      `INSERT INTO rate_monitors 
       (pd_id, sell_currency, buy_currency, sell_amount, buy_amount, 
        target_client_rate, target_market_rate, alert_or_order)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *`,
      [pdId, sellCurrency, buyCurrency, sellAmount, buyAmount, 
       targetClientRate, targetMarketRate, alertOrOrder]
    );
    
    res.status(201).json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Update monitor
app.put('/api/monitors/:id', async (req, res) => {
  const { id } = req.params;
  const updates = req.body;
  
  try {
    const setClause = Object.keys(updates)
      .map((key, index) => `${key} = $${index + 2}`)
      .join(', ');
    
    const values = [id, ...Object.values(updates)];
    
    const result = await pool.query(
      `UPDATE rate_monitors SET ${setClause} WHERE id = $1 RETURNING *`,
      values
    );
    
    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Delete monitor
app.delete('/api/monitors/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM rate_monitors WHERE id = $1', [req.params.id]);
    res.status(204).send();
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Manual rate check trigger
app.post('/api/check-rates', async (req, res) => {
  try {
    await checkRates();
    res.json({ message: 'Rate check triggered successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get current rate for a currency pair
app.get('/api/rate/:from/:to', async (req, res) => {
  try {
    const rate = await rateService.getRate(req.params.from, req.params.to);
    res.json({ rate });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Schedule automatic rate checks every 15 minutes
cron.schedule('*/15 * * * *', () => {
  console.log('Scheduled rate check triggered');
  checkRates();
});

// Initialize and start server
async function start() {
  await initializeDatabase();
  
  const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(`Rate monitoring service running on port ${port}`);
    console.log('Automated checks will run every 15 minutes');
  });
}

start().catch(console.error);
