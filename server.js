// Rate Alerts & Market Orders Tool for Railway
// This tool monitors currency rates and triggers WhatsApp alerts or creates deals

const express = require('express');
const { Pool } = require('pg');
const cron = require('node-cron');
const axios = require('axios');
require('dotenv').config();

const app = express();
app.use(express.static('public'));

const VERBOSE = process.env.VERBOSE_LOGS === '1';

// WhatsApp API version/base
const WA_VERSION = process.env.WHATSAPP_API_VERSION || 'v19.0';
const WA_BASE_URL = process.env.WHATSAPP_BASE_URL || `https://graph.facebook.com/${WA_VERSION}/`;

// Simple phone normalizer -> E.164-ish (best effort).
function normalizePhone(input) {
  const s = String(input || '').replace(/[^\d+]/g, '');
  if (s.startsWith('+')) return s;
  if (process.env.DEFAULT_COUNTRY_CODE) {
    return `+${process.env.DEFAULT_COUNTRY_CODE}${s.replace(/^0+/, '')}`;
  }
  return `+${s}`;
}

// Database connection with better error handling and connection pooling
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// Declare currentCronJob before using in signal handlers
let currentCronJob = null;

// Graceful shutdown handling
const gracefulShutdown = async (signal) => {
  console.log(`Received ${signal}. Shutting down gracefully...`);
  try {
    if (currentCronJob) {
      currentCronJob.destroy();
    }
  } catch (error) {
    console.error('Error stopping cron job:', error);
  }
  
  try {
    await pool.end();
    console.log('Database pool closed');
  } catch (error) {
    console.error('Error closing database pool:', error);
  }
  
  process.exit(0);
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

async function initializeDatabase() {
  const createTableAndIndexes = `
    CREATE TABLE IF NOT EXISTS rate_monitors (
      id SERIAL PRIMARY KEY,
      pd_id VARCHAR(255) NOT NULL,
      sell_currency VARCHAR(3) NOT NULL,
      buy_currency VARCHAR(3) NOT NULL,
      sell_amount DECIMAL(15,2),
      buy_amount DECIMAL(15,2),
      target_client_rate DECIMAL(10,6) NOT NULL CHECK (target_client_rate > 0),
      target_market_rate DECIMAL(10,6) NOT NULL CHECK (target_market_rate > 0),
      alert_or_order VARCHAR(10) NOT NULL CHECK (alert_or_order IN ('alert', 'order')),
      trigger_direction VARCHAR(10) DEFAULT 'above' CHECK (trigger_direction IN ('above', 'below')),
      status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'triggered', 'cancelled')),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      triggered_at TIMESTAMP,
      current_rate DECIMAL(10,6),
      initial_rate DECIMAL(10,6),
      last_checked TIMESTAMP,
      update_frequency VARCHAR(50),
      phone VARCHAR(32),
      retry_count INTEGER DEFAULT 0,
      last_error TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_currency_pair ON rate_monitors(sell_currency, buy_currency);
    CREATE INDEX IF NOT EXISTS idx_status ON rate_monitors(status);
    CREATE INDEX IF NOT EXISTS idx_last_checked ON rate_monitors(last_checked);
    
    CREATE UNIQUE INDEX IF NOT EXISTS uniq_active_monitor
    ON rate_monitors (pd_id, sell_currency, buy_currency, phone)
    WHERE status = 'active';
  `;

  try {
    await pool.query(createTableAndIndexes);
    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Database initialization error:', error);
    throw error;
  }
}

// Rate fetching service with caching and retry logic
class RateService {
  constructor() {
    this.baseURL = 'https://openexchangerates.org/api/latest.json';
    this.apiKey = process.env.OPENEXCHANGERATES_API_KEY;
    this.cache = new Map();
    this.cacheExpiry = 60000;
    this.retryAttempts = 3;
    this.retryDelay = 1000;
  }
  
  async getRate(fromCurrency, toCurrency, useCache = true) {
    const cacheKey = `${fromCurrency}/${toCurrency}`;
    
    if (useCache && this.cache.has(cacheKey)) {
      const cached = this.cache.get(cacheKey);
      if (Date.now() - cached.timestamp < this.cacheExpiry) {
        return cached.rate;
      }
    }
    
    for (let attempt = 1; attempt <= this.retryAttempts; attempt++) {
      try {
        const response = await axios.get(`${this.baseURL}?app_id=${this.apiKey}`, {
          timeout: 10000,
        });
        const rates = response.data.rates;
        
        let rate;
        if (fromCurrency === 'USD') {
          rate = rates[toCurrency] || null;
        } else if (toCurrency === 'USD') {
          rate = 1 / (rates[fromCurrency] || 1);
        } else {
          const fromRate = rates[fromCurrency];
          const toRate = rates[toCurrency];
          
          if (!fromRate || !toRate) {
            console.error(`Rate not found for ${fromCurrency} or ${toCurrency}`);
            return null;
          }
          
          rate = toRate / fromRate;
        }
        
        this.cache.set(cacheKey, {
          rate,
          timestamp: Date.now()
        });
        
        return rate;
      } catch (error) {
        console.error(`Attempt ${attempt} failed for ${fromCurrency}/${toCurrency}:`, error.message);
        
        if (error.response?.status === 401) {
          console.error('OpenExchangeRates API key invalid or missing');
          break;
        } else if (error.response?.status === 429) {
          console.error('OpenExchangeRates API rate limit exceeded');
          await this.sleep(5000 * attempt);
        } else if (attempt < this.retryAttempts) {
          await this.sleep(this.retryDelay * attempt);
        }
      }
    }
    
    return null;
  }
  
  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
  
  async getBatchRates(currencyPairs) {
    try {
      const response = await axios.get(`${this.baseURL}?app_id=${this.apiKey}`, {
        timeout: 10000,
      });
      const rates = response.data.rates;
      const results = {};
      
      for (const pair of currencyPairs) {
        const { from, to } = pair;
        const cacheKey = `${from}/${to}`;
        
        let rate;
        if (from === 'USD') {
          rate = rates[to] || null;
        } else if (to === 'USD') {
          rate = 1 / (rates[from] || 1);
        } else {
          const fromRate = rates[from];
          const toRate = rates[to];
          rate = (fromRate && toRate) ? toRate / fromRate : null;
        }
        
        results[cacheKey] = rate;
        
        if (rate !== null) {
          this.cache.set(cacheKey, {
            rate,
            timestamp: Date.now()
          });
        }
      }
      
      return results;
    } catch (error) {
      console.error('Error fetching batch rates:', error);
      return {};
    }
  }
  
  clearCache() {
    this.cache.clear();
  }
}

// WhatsApp service with better error handling and retry logic
class WhatsAppService {
  constructor() {
    this.apiKey = process.env.WHATSAPP_API_KEY;
    this.baseURL = WA_BASE_URL;
    this.retryAttempts = 3;
    this.retryDelay = 1000;
  }
  
  async sendMessage(phoneNumber, messageContent, retryCount = 0) {
    try {
      const payload = {
        messaging_product: "whatsapp",
        to: phoneNumber,
        ...messageContent
      };

      const response = await axios.post(
        `${this.baseURL}${process.env.WHATSAPP_PHONE_NUMBER_ID}/messages`,
        payload,
        {
          headers: {
            'Authorization': `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json'
          },
          timeout: 10000,
        }
      );
      
      return response.data;
    } catch (error) {
      console.error(`WhatsApp message error (attempt ${retryCount + 1}):`, error.message);
      
      if (retryCount < this.retryAttempts - 1) {
        await this.sleep(this.retryDelay * (retryCount + 1));
        return this.sendMessage(phoneNumber, messageContent, retryCount + 1);
      }
      
      throw error;
    }
  }
  
  async sendTemplateMessage(phoneNumber, templateName, parameters, headerImage = null) {
    const components = [];
    
    const headerImageUrl = headerImage || process.env.RATE_ALERT_HEADER_IMAGE;
    if (headerImageUrl) {
      components.push({
        type: "header",
        parameters: [{
          type: "image",
          image: {
            link: headerImageUrl
          }
        }]
      });
    }
    
    if (parameters && parameters.length > 0) {
      const body_parameters = parameters.map(param => 
        typeof param === 'object' ? param : { type: "text", text: param }
      );
      
      components.push({
        type: "body",
        parameters: body_parameters
      });
    }
    
    const messageContent = {
      type: "template",
      template: {
        name: templateName,
        language: { code: "en" }
      }
    };
    
    if (components.length > 0) {
      messageContent.template.components = components;
    }
    
    if (VERBOSE) {
      console.log('Sending template:', JSON.stringify(messageContent, null, 2));
    }
    
    return await this.sendMessage(phoneNumber, messageContent);
  }
  
  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Enhanced Pipeline service with retry logic
class PipelineService {
  constructor() {
    this.apiKey = process.env.CRM_API_KEY;
    this.baseURL = process.env.CRM_BASE_URL;
    this.retryAttempts = 3;
    this.retryDelay = 1000;
  }
  
  async createDeal(dealData, retryCount = 0) {
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
          },
          timeout: 10000,
        }
      );
      
      return response.data;
    } catch (error) {
      console.error(`CRM deal creation error (attempt ${retryCount + 1}):`, error.message);
      
      if (retryCount < this.retryAttempts - 1) {
        await this.sleep(this.retryDelay * (retryCount + 1));
        return this.createDeal(dealData, retryCount + 1);
      }
      
      throw error;
    }
  }
  
  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Enhanced PipeDrive service with caching
class PipeDriveService {
  constructor() {
    this.apiKey = process.env.PIPEDRIVE_API_KEY;
    this.baseURL = process.env.PIPEDRIVE_BASE_URL || 'https://api.pipedrive.com/v1';
    this.cache = new Map();
    this.cacheExpiry = 300000;
  }
  
  async getPersonById(personId) {
    const cacheKey = `person_${personId}`;
    
    if (this.cache.has(cacheKey)) {
      const cached = this.cache.get(cacheKey);
      if (Date.now() - cached.timestamp < this.cacheExpiry) {
        return cached.data;
      }
    }
    
    try {
      const response = await axios.get(
        `${this.baseURL}/persons/${personId}?api_token=${this.apiKey}`,
        { timeout: 10000 }
      );
      
      const data = response.data.data;
      this.cache.set(cacheKey, {
        data,
        timestamp: Date.now()
      });
      
      return data;
    } catch (error) {
      console.error(`Error fetching person ${personId}:`, error);
      return null;
    }
  }
  
  async getPersonMargin(personId) {
    try {
      const person = await this.getPersonById(personId);
      if (!person) return 0.005;
      
      const marginValue = person['d12fb97c7cf4908a8e57c8693aa187e64302413f'] || 
                         person['margin'] || 
                         0.5;
      
      return parseFloat(marginValue) / 100;
    } catch (error) {
      console.error(`Error fetching margin for person ${personId}:`, error);
      return 0.005;
    }
  }
  
  async searchPersonByPhone(phoneNumber) {
    try {
      const cleanPhone = phoneNumber.replace(/[\+\s\-\(\)]/g, '');
      
      const response = await axios.get(
        `${this.baseURL}/persons/search?term=${cleanPhone}&api_token=${this.apiKey}`,
        { timeout: 10000 }
      );
      
      if (response.data.data && response.data.data.items.length > 0) {
        return response.data.data.items[0].item;
      }
      
      return null;
    } catch (error) {
      console.error(`Error searching person by phone ${phoneNumber}:`, error);
      return null;
    }
  }
}

const rateService = new RateService();
const whatsappService = new WhatsAppService();
const pipelineService = new PipelineService();
const pipeDriveService = new PipeDriveService();

// Enhanced rate checking with batch processing and better error handling
async function checkRates() {
  console.log('Starting rate check...', new Date().toISOString());

  try {
    const { rows: monitors } = await pool.query(
      'SELECT * FROM rate_monitors WHERE status = $1 ORDER BY last_checked ASC NULLS FIRST',
      ['active']
    );

    if (monitors.length === 0) {
      console.log('No active monitors found');
      return;
    }

    const currencyPairs = new Map();
    monitors.forEach(monitor => {
      const pairKey = `${monitor.sell_currency}/${monitor.buy_currency}`;
      if (!currencyPairs.has(pairKey)) {
        currencyPairs.set(pairKey, []);
      }
      currencyPairs.get(pairKey).push(monitor);
    });

    const batchRates = await rateService.getBatchRates(
      Array.from(currencyPairs.keys()).map(pair => {
        const [from, to] = pair.split('/');
        return { from, to };
      })
    );

    const triggeredMonitors = [];

    for (const [pairKey, pairMonitors] of currencyPairs) {
      const currentRate = batchRates[pairKey];

      if (!currentRate) {
        console.log(`Could not fetch rate for ${pairKey}`);
        for (const monitor of pairMonitors) {
          await pool.query(
            'UPDATE rate_monitors SET retry_count = retry_count + 1, last_error = $1, last_checked = CURRENT_TIMESTAMP WHERE id = $2',
            [`Failed to fetch rate for ${pairKey}`, monitor.id]
          );
        }
        continue;
      }

      for (const monitor of pairMonitors) {
        await pool.query(
          'UPDATE rate_monitors SET current_rate = $1, last_checked = CURRENT_TIMESTAMP, retry_count = 0, last_error = NULL WHERE id = $2',
          [currentRate, monitor.id]
        );

        const targetMet =
          monitor.trigger_direction === 'above'
            ? currentRate >= Number(monitor.target_market_rate)
            : currentRate <= Number(monitor.target_market_rate);

        if (targetMet) {
          triggeredMonitors.push({ monitor, currentRate });
        }
      }
    }

    for (const { monitor, currentRate } of triggeredMonitors) {
      console.log(`🎯 Target met for monitor ${monitor.id} (mode=${monitor.alert_or_order})`);

      const claim = await pool.query(
        `UPDATE rate_monitors
         SET status = 'triggered', triggered_at = NOW()
         WHERE id = $1 AND status = 'active'
         RETURNING id`,
        [monitor.id]
      );

      if (claim.rowCount === 0) {
        console.log(`↪️ Already handled monitor ${monitor.id}, skipping notify`);
        continue;
      }

      try {
        if (monitor.alert_or_order === 'alert') {
          await notifyQuoteTriggered(monitor, currentRate);
        } else {
          await handleOrder(monitor, currentRate);
        }
      } catch (error) {
        console.error(`Error handling triggered monitor ${monitor.id}:`, error);
        await pool.query(
          'UPDATE rate_monitors SET status = $1, last_error = $2 WHERE id = $3',
          ['active', error.message, monitor.id]
        );
      }
    }

    console.log(`Rate check completed. Processed ${monitors.length} monitors, triggered ${triggeredMonitors.length}`);
  } catch (error) {
    console.error('Rate check error:', error);
  }
}

async function notifyQuoteTriggered(monitor, currentRate) {
  try {
    if (!process.env.QUOTE_BASE_URL) {
      throw new Error('QUOTE_BASE_URL not set');
    }
    if (!process.env.INTERNAL_SHARED_SECRET) {
      throw new Error('INTERNAL_SHARED_SECRET not set');
    }
    if (!monitor.phone) {
      throw new Error(`Monitor ${monitor.id} has no phone number`);
    }

    const url = `${process.env.QUOTE_BASE_URL}/api/send-alert-triggered`;
    const payload = {
      phone: monitor.phone,
      sellCurrency: monitor.sell_currency,
      buyCurrency: monitor.buy_currency,
      targetClientRate: Number(monitor.target_client_rate),
      currentClientRate: Number(currentRate)
    };

    if (VERBOSE) console.log('📤 POST to Quote', { url, payload });

    const response = await axios.post(url, payload, {
      headers: { "x-internal-secret": process.env.INTERNAL_SHARED_SECRET },
      timeout: 10000,
    });

    if (VERBOSE) console.log('✅ Quote responded', { status: response.status, data: response.data });
  } catch (error) {
    console.error('❌ notifyQuoteTriggered failed', {
      monitorId: monitor.id,
      status: error.response?.status,
      data: error.response?.data,
      message: error.message
    });
    throw error;
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
    throw error;
  }
}

// Enhanced monitoring with circuit breaker pattern
let checkRatesInProgress = false;
let consecutiveFailures = 0;
const maxConsecutiveFailures = 5;

async function safeCheckRates() {
  if (checkRatesInProgress) {
    console.log('Rate check already in progress, skipping...');
    return;
  }

  if (consecutiveFailures >= maxConsecutiveFailures) {
    console.log(`Skipping rate check due to ${consecutiveFailures} consecutive failures`);
    return;
  }

  checkRatesInProgress = true;

  try {
    await checkRates();
    consecutiveFailures = 0;
  } catch (error) {
    consecutiveFailures++;
    console.error(`Rate check failed (${consecutiveFailures}/${maxConsecutiveFailures}):`, error);
  } finally {
    checkRatesInProgress = false;
  }
}

function scheduleNextCheck() {
  if (currentCronJob) {
    currentCronJob.destroy();
  }
  
  pool.query(
    'SELECT * FROM rate_monitors WHERE status = $1 AND current_rate IS NOT NULL', 
    ['active']
  )
    .then(result => {
      let needsFrequentCheck = false;
      let hasActiveMonitors = result.rows.length > 0;
      
      for (const monitor of result.rows) {
        if (monitor.current_rate) {
          const proximityPercent = Math.abs(monitor.current_rate - monitor.target_market_rate) / monitor.target_market_rate;
          if (proximityPercent <= 0.002) {
            needsFrequentCheck = true;
            break;
          }
        }
      }
      
      if (!hasActiveMonitors) {
        console.log('No active monitors - scheduling hourly checks');
        currentCronJob = cron.schedule('0 * * * *', () => {
          console.log('Hourly maintenance check');
          safeCheckRates();
        });
      } else if (needsFrequentCheck) {
        console.log('Close to targets detected - switching to 1-minute checks');
        currentCronJob = cron.schedule('* * * * *', () => {
          console.log('High-frequency rate check triggered');
          safeCheckRates();
        });
      } else {
        console.log('Normal monitoring - using 15-minute checks');
        currentCronJob = cron.schedule('*/15 * * * *', () => {
          console.log('Standard rate check triggered');
          safeCheckRates();
        });
      }
    })
    .catch(err => {
      console.error('Error determining check frequency:', err);
      currentCronJob = cron.schedule('*/15 * * * *', () => {
        console.log('Fallback rate check triggered');
        safeCheckRates();
      });
    });
}

// Fix webhook/body parser order - mount raw parser BEFORE global json parser
app.use('/webhook/whatsapp', express.raw({ type: 'application/json' }));

// WhatsApp webhook handling with proper body parsing
app.post('/webhook/whatsapp', async (req, res) => {
  try {
    const body = JSON.parse(req.body.toString('utf8'));
    
    if (body.object === 'whatsapp_business_account') {
      body.entry?.forEach(entry => {
        entry.changes?.forEach(change => {
          if (change.field === 'messages' && change.value) {
            const message = change.value.messages?.[0];
            if (message && message.type !== 'system') {
              handleIncomingMessage(message, change.value).catch(error => {
                console.error('Error handling incoming message:', error);
              });
            }
          }
        });
      });
    }
    
    res.status(200).send('OK');
  } catch (error) {
    console.error('WhatsApp webhook error:', error);
    res.status(400).send('Error processing webhook');
  }
});

// WhatsApp webhook verification
app.get('/webhook/whatsapp', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  
  if (mode === 'subscribe' && token === process.env.WHATSAPP_VERIFY_TOKEN) {
    console.log('WhatsApp webhook verified');
    res.status(200).send(challenge);
  } else {
    res.status(403).send('Forbidden');
  }
});

// Add express.json() middleware AFTER webhook routes
app.use(express.json());

// Utility function to validate monitor input
function validateMonitorInput(req, res, next) {
  const {
    pdId,
    sellCurrency,
    buyCurrency,
    targetClientRate,
    alertOrOrder
  } = req.body;

  const errors = [];

  if (!pdId) errors.push('pdId is required');
  if (!sellCurrency || sellCurrency.length !== 3) errors.push('sellCurrency must be a 3-letter currency code');
  if (!buyCurrency || buyCurrency.length !== 3) errors.push('buyCurrency must be a 3-letter currency code');
  if (!targetClientRate || isNaN(targetClientRate) || targetClientRate <= 0) errors.push('targetClientRate must be a positive number');
  if (!alertOrOrder || !['alert', 'order'].includes(alertOrOrder)) errors.push('alertOrOrder must be "alert" or "order"');

  if (errors.length > 0) {
    return res.status(400).json({ error: 'Validation failed', details: errors });
  }

  next();
}

// API Routes
app.get('/api/rate/:from/:to', async (req, res) => {
  try {
    const { from, to } = req.params;
    
    if (!from || !to || from.length !== 3 || to.length !== 3) {
      return res.status(400).json({ error: 'Invalid currency codes' });
    }

    const rate = await rateService.getRate(from.toUpperCase(), to.toUpperCase());
    
    if (rate === null) {
      return res.status(404).json({ error: 'Rate not found' });
    }

    res.set('Cache-Control', 'public, max-age=60');
    res.json({ 
      rate, 
      pair: `${from}/${to}`,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Rate fetch error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/monitors', async (req, res) => {
  try {
    const { status, currency_pair, limit = 100, offset = 0 } = req.query;
    
    let query = 'SELECT * FROM rate_monitors';
    const params = [];
    const conditions = [];

    if (status) {
      conditions.push(`status = $${params.length + 1}`);
      params.push(status);
    }

    if (currency_pair) {
      const [sell, buy] = currency_pair.split('/');
      if (sell && buy) {
        conditions.push(`sell_currency = $${params.length + 1} AND buy_currency = $${params.length + 2}`);
        params.push(sell, buy);
      }
    }

    if (conditions.length > 0) {
      query += ` WHERE ${conditions.join(' AND ')}`;
    }

    query += ` ORDER BY created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
    params.push(limit, offset);

    const result = await pool.query(query, params);
    
    let countQuery = 'SELECT COUNT(*) FROM rate_monitors';
    if (conditions.length > 0) {
      countQuery += ` WHERE ${conditions.join(' AND ')}`;
    }
    const countResult = await pool.query(countQuery, params.slice(0, -2));

    res.json({
      monitors: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset)
    });
  } catch (error) {
    console.error('Get monitors error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/monitors', validateMonitorInput, async (req, res) => {
  try {
    const {
      pdId,
      sellCurrency,
      buyCurrency,
      sellAmount,
      buyAmount,
      targetClientRate,
      targetMarketRate,
      alertOrOrder,
      triggerDirection,
      updateFrequency,
      updateFrequencyLabel,
      phone
    } = req.body;

    if (VERBOSE) {
      console.log('MONITOR CREATE BODY:', JSON.stringify(req.body, null, 2));
    }

    const sellC = sellCurrency.toUpperCase();
    const buyC = buyCurrency.toUpperCase();

    const currentRate = await rateService.getRate(sellC, buyC);
    
    if (currentRate === null) {
      return res.status(400).json({ error: 'Unable to fetch current rate for currency pair' });
    }

    let finalMarketRate = targetMarketRate;
    if (!targetMarketRate && pdId) {
      try {
        const clientMargin = await pipeDriveService.getPersonMargin(pdId);
        finalMarketRate = targetClientRate * (1 - clientMargin);
        console.log(`Calculated market rate: ${finalMarketRate} (client rate: ${targetClientRate}, margin: ${clientMargin * 100}%)`);
      } catch (error) {
        console.error('Error calculating market rate from margin:', error);
        finalMarketRate = targetClientRate;
      }
    }

    if (!finalMarketRate) {
      return res.status(400).json({ 
        error: 'targetMarketRate is required when pdId is not provided or margin cannot be determined' 
      });
    }

    let direction = triggerDirection;
    if (!direction) {
      direction = finalMarketRate > currentRate ? 'above' : 'below';
    }

    const rawFreq = (updateFrequency || updateFrequencyLabel || '').toString().trim().toLowerCase();
    let dbFreq = null;
    if (rawFreq.startsWith('daily')) {
      dbFreq = 'Daily';
    } else if (rawFreq.startsWith('week')) {
      dbFreq = 'Weekly';
    } else if (rawFreq === 'on_target' || (rawFreq.includes('only') && rawFreq.includes('achiev'))) {
      dbFreq = 'Only when rate is achieved';
    }

    const normalizedPhone = normalizePhone(phone);

    const result = await pool.query(
      `INSERT INTO rate_monitors 
       (pd_id, sell_currency, buy_currency, sell_amount, buy_amount, 
        target_client_rate, target_market_rate, alert_or_order, trigger_direction, 
        initial_rate, current_rate, update_frequency, phone)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
       RETURNING *`,
      [
        pdId, 
        sellC, 
        buyC, 
        sellAmount, 
        buyAmount,
        targetClientRate, 
        finalMarketRate, 
        alertOrOrder, 
        direction,
        currentRate,
        currentRate,
        dbFreq,
        normalizedPhone
      ]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error('Create monitor error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/check-rates', async (req, res) => {
  try {
    consecutiveFailures = 0;
    await safeCheckRates();
    res.json({ 
      message: 'Rate check triggered successfully',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Manual rate check error:', error);
    res.status(500).json({ error: 'Rate check failed' });
  }
});

app.get('/health', async (req, res) => {
  const health = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    environment: process.env.NODE_ENV || 'development'
  };

  try {
    await pool.query('SELECT 1');
    health.database = 'connected';
  } catch (error) {
    health.database = 'disconnected';
    health.status = 'unhealthy';
  }

  const deepCheck = req.query.deep === '1';
  try {
    if (deepCheck) {
      const testRate = await rateService.getRate('USD', 'EUR');
      health.rateService = testRate ? 'working' : 'degraded';
    } else {
      health.rateService = rateService.cache.size > 0 ? 'cached' : 'unknown';
    }
  } catch (error) {
    health.rateService = 'error';
    health.status = 'degraded';
  }

  health.consecutiveFailures = consecutiveFailures;
  if (consecutiveFailures >= maxConsecutiveFailures) {
    health.status = 'degraded';
    health.circuitBreaker = 'open';
  }

  const statusCode = health.status === 'healthy' ? 200 : 503;
  res.status(statusCode).json(health);
});

app.post('/api/send-rate-alert-cta', async (req, res) => {
  try {
    if (req.headers['x-internal-secret'] !== process.env.INTERNAL_SHARED_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { phone } = req.body || {};
    if (!phone) {
      return res.status(400).json({ error: 'phone required' });
    }

    const normalizedPhone = normalizePhone(phone);
    if (!normalizedPhone || !/^\+?\d{10,15}$/.test(normalizedPhone.replace('+', ''))) {
      return res.status(400).json({ error: 'Invalid phone number format' });
    }

    const headerImage = process.env.RATE_ALERT_HEADER_IMAGE || 
      "https://raw.githubusercontent.com/Cornolly/summitfx-assets/main/Logo%20standard.png";

    const messageContent = {
      type: "template",
      template: {
        name: "rate_alert",
        language: { code: "en" },
        components: [
          {
            type: "header",
            parameters: [{ type: "image", image: { link: headerImage } }]
          },
          {
            type: "button",
            sub_type: "flow",
            index: "0",
            parameters: [
              {
                type: "payload",
                payload: JSON.stringify({
                  flow_message_version: "3",
                  flow_token: process.env.WA_FLOW_TOKEN,
                  flow_id: process.env.WA_FLOW_ID,
                  flow_cta: "Create rate alert",
                  flow_action: "data_exchange",
                  flow_action_payload: { screen: "Rate alert" }
                })
              }
            ]
          }
        ]
      }
    };

    const response = await whatsappService.sendMessage(normalizedPhone, messageContent);
    res.json({ 
      message_id: response?.messages?.[0]?.id || null, 
      status: 'sent',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('send-rate-alert-cta error:', error);
    res.status(500).json({ 
      error: 'failed_to_send', 
      details: error.response?.data || error.message 
    });
  }
});

app.post('/api/test-template/:phoneNumber', async (req, res) => {
  try {
    const phoneNumber = req.params.phoneNumber;
    
    const normalizedPhone = normalizePhone(phoneNumber);
    if (!normalizedPhone || !/^\+?\d{10,15}$/.test(normalizedPhone.replace('+', ''))) {
      return res.status(400).json({ error: 'Invalid phone number format' });
    }
    
    console.log('=== TESTING TEMPLATE SEND ===');
    console.log('Phone number:', normalizedPhone);
    
    const messageContent = {
      type: "template",
      template: {
        name: "rate_alert",
        language: { code: "en" },
        components: [
          {
            type: "header",
            parameters: [
              {
                type: "image",
                image: {
                  link: process.env.RATE_ALERT_HEADER_IMAGE ||
                    "https://raw.githubusercontent.com/Cornolly/summitfx-assets/main/Logo%20standard.png"
                }
              }
            ]
          },
          {
            type: "button",
            sub_type: "flow",
            index: "0",
            parameters: [
              {
                type: "payload",
                payload: JSON.stringify({
                  flow_message_version: "3",
                  flow_token: process.env.WA_FLOW_TOKEN,
                  flow_id: process.env.WA_FLOW_ID,
                  flow_cta: "Create rate alert",
                  flow_action: "data_exchange",
                  flow_action_payload: {
                    screen: "Rate alert"
                  }
                })
              }
            ]
          }
        ]
      }
    };

    if (VERBOSE) {
      console.log('Sending template:', JSON.stringify(messageContent, null, 2));
    }
    
    const response = await whatsappService.sendMessage(normalizedPhone, messageContent);
    
    console.log('Template sent successfully:', response);
    res.json({ 
      message: 'Template sent successfully', 
      whatsapp_response: response,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('=== TEMPLATE SEND ERROR ===');
    console.error('Error response data:', error.response?.data);
    
    res.status(500).json({ 
      error: 'Failed to send template',
      details: error.message,
      whatsapp_error: error.response?.data,
      timestamp: new Date().toISOString()
    });
  }
});

app.post('/api/test-whatsapp-config', (req, res) => {
  const config = {
    hasApiKey: !!process.env.WHATSAPP_API_KEY,
    hasPhoneNumberId: !!process.env.WHATSAPP_PHONE_NUMBER_ID,
    hasVerifyToken: !!process.env.WHATSAPP_VERIFY_TOKEN,
    hasFlowId: !!process.env.WA_FLOW_ID,
    hasFlowToken: !!process.env.WA_FLOW_TOKEN,
    phoneNumberId: process.env.WHATSAPP_PHONE_NUMBER_ID ? 'Set' : 'Missing',
    baseUrl: WA_BASE_URL,
    apiVersion: WA_VERSION,
    timestamp: new Date().toISOString()
  };
  
  res.json(config);
});

app.get('/api/monitors/stats', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT 
        status,
        COUNT(*) as count,
        AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at))/3600) as avg_age_hours
      FROM rate_monitors 
      GROUP BY status
    `);
    
    const currencyPairs = await pool.query(`
      SELECT 
        CONCAT(sell_currency, '/', buy_currency) as pair,
        COUNT(*) as count
      FROM rate_monitors 
      WHERE status = 'active'
      GROUP BY sell_currency, buy_currency
      ORDER BY count DESC
      LIMIT 10
    `);
    
    res.json({
      statusCounts: stats.rows,
      topCurrencyPairs: currencyPairs.rows,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Stats error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.patch('/api/monitors/:id/cancel', async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await pool.query(
      'UPDATE rate_monitors SET status = $1 WHERE id = $2 AND status = $3 RETURNING *',
      ['cancelled', id, 'active']
    );
    
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Monitor not found or already inactive' });
    }
    
    res.json({
      message: 'Monitor cancelled successfully',
      monitor: result.rows[0],
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Cancel monitor error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Enhanced message handling with better error handling and flow state management
const userFlowState = new Map();
const FLOW_STATE_TIMEOUT = 30 * 60 * 1000;

setInterval(() => {
  const now = Date.now();
  for (const [phoneNumber, state] of userFlowState.entries()) {
    if (now - state.lastActivity > FLOW_STATE_TIMEOUT) {
      userFlowState.delete(phoneNumber);
      console.log(`Cleaned up expired flow state for ${phoneNumber}`);
    }
  }
}, 5 * 60 * 1000);

async function handleIncomingMessage(message, messageData) {
  console.log('=== INCOMING WHATSAPP MESSAGE ===');
  console.log('Message ID:', message.id);
  console.log('Message Type:', message.type);
  console.log('From:', message.from);
  
  if (VERBOSE) {
    console.log('Full message object:', JSON.stringify(message, null, 2));
  }
  
  try {
    const phoneNumber = message.from;
    const messageType = message.type;
    
    if (userFlowState.has(phoneNumber)) {
      userFlowState.get(phoneNumber).lastActivity = Date.now();
    }
    
    switch (messageType) {
      case 'interactive':
        await handleInteractiveMessage(message, phoneNumber);
        break;
      case 'text':
        await handleTextMessage(message, phoneNumber);
        break;
      case 'button':
      case 'list_reply':
        await handleTemplateResponse(message, phoneNumber);
        break;
      default:
        console.log(`Unhandled message type: ${messageType}`);
    }
  } catch (error) {
    console.error('Error handling incoming message:', error);
    try {
      await whatsappService.sendMessage(message.from, {
        type: "text",
        text: { body: "Sorry, I encountered an error processing your message. Please try again later." }
      });
    } catch (sendError) {
      console.error('Error sending error message:', sendError);
    }
  } finally {
    console.log('=== END MESSAGE LOG ===');
  }
}

async function handleTemplateResponse(message, phoneNumber) {
  console.log('Handling template response for rate_alert');
  await startRateAlertFlow(phoneNumber);
}

async function handleInteractiveMessage(message, phoneNumber) {
  const interactive = message.interactive;
  
  if (interactive.type === 'button_reply') {
    const buttonId = interactive.button_reply.id;
    
    if (buttonId === 'create_rate_alert') {
      await startRateAlertFlow(phoneNumber);
    }
  } else if (interactive.type === 'list_reply') {
    const listReply = interactive.list_reply;
    await handleListSelection(phoneNumber, listReply);
  }
}

async function handleTextMessage(message, phoneNumber) {
  const text = message.text.body.toLowerCase().trim();
  
  const flowState = userFlowState.get(phoneNumber);
  
  if (flowState && flowState.waitingForRate) {
    const rateMatch = text.match(/^\d+\.?\d*$/);
    if (rateMatch) {
      await handleTargetRateInput(phoneNumber, parseFloat(text));
    } else {
      await whatsappService.sendMessage(phoneNumber, {
        type: "text",
        text: { body: "Please enter a valid number for the target rate (e.g., 1.25)" }
      });
    }
  } else {
    if (text.includes('help') || text.includes('start')) {
      await startRateAlertFlow(phoneNumber);
    } else {
      await whatsappService.sendMessage(phoneNumber, {
        type: "text",
        text: { body: "Hi! I can help you create rate alerts. Type 'help' to get started." }
      });
    }
  }
}

async function startRateAlertFlow(phoneNumber) {
  userFlowState.set(phoneNumber, {
    step: 'currency_selection',
    lastActivity: Date.now()
  });

  const currencyMessage = {
    type: "interactive",
    interactive: {
      type: "list",
      header: { type: "text", text: "Create Rate Alert" },
      body: { text: "Select the currency pair you want to monitor:" },
      footer: { text: "Choose sell currency first" },
      action: {
        button: "Select Currency",
        sections: [{
          title: "Major Currencies",
          rows: [
            { id: "USD", title: "USD", description: "US Dollar" },
            { id: "EUR", title: "EUR", description: "Euro" },
            { id: "GBP", title: "GBP", description: "British Pound" },
            { id: "JPY", title: "JPY", description: "Japanese Yen" },
            { id: "CAD", title: "CAD", description: "Canadian Dollar" },
            { id: "AUD", title: "AUD", description: "Australian Dollar" }
          ]
        }]
      }
    }
  };
  
  await whatsappService.sendMessage(phoneNumber, currencyMessage);
}

async function handleListSelection(phoneNumber, listReply) {
  const selection = listReply.id;
  const currentState = userFlowState.get(phoneNumber) || {};
  
  if (!currentState.sellCurrency) {
    currentState.sellCurrency = selection;
    currentState.step = 'buy_currency_selection';
    userFlowState.set(phoneNumber, currentState);
    
    await sendBuyCurrencySelection(phoneNumber, selection);
  } else if (!currentState.buyCurrency) {
    currentState.buyCurrency = selection;
    currentState.step = 'frequency_selection';
    userFlowState.set(phoneNumber, currentState);
    
    await sendFrequencySelection(phoneNumber);
  } else if (!currentState.updateFrequency) {
    currentState.updateFrequency = selection;
    currentState.step = 'target_rate_input';
    currentState.waitingForRate = true;
    userFlowState.set(phoneNumber, currentState);
    
    await requestTargetRate(phoneNumber, currentState);
  }
}

async function sendBuyCurrencySelection(phoneNumber, sellCurrency) {
  const currencies = [
    { id: "USD", title: "USD", description: "US Dollar" },
    { id: "EUR", title: "EUR", description: "Euro" },
    { id: "GBP", title: "GBP", description: "British Pound" },
    { id: "JPY", title: "JPY", description: "Japanese Yen" },
    { id: "CAD", title: "CAD", description: "Canadian Dollar" },
    { id: "AUD", title: "AUD", description: "Australian Dollar" }
  ].filter(curr => curr.id !== sellCurrency);
  
  const message = {
    type: "interactive",
    interactive: {
      type: "list",
      header: { type: "text", text: "Select Buy Currency" },
      body: { text: `You're selling ${sellCurrency}. What currency do you want to buy?` },
      action: {
        button: "Select Currency",
        sections: [{
          title: "Available Currencies",
          rows: currencies
        }]
      }
    }
  };
  
  await whatsappService.sendMessage(phoneNumber, message);
}

async function sendFrequencySelection(phoneNumber) {
  const message = {
    type: "interactive",
    interactive: {
      type: "list",
      header: { type: "text", text: "Update Frequency" },
      body: { text: "How often would you like updates about this rate?" },
      action: {
        button: "Select Frequency",
        sections: [{
          title: "Frequency Options",
          rows: [
            { id: "Daily", title: "Daily", description: "Get daily rate updates" },
            { id: "Weekly", title: "Weekly", description: "Get weekly rate updates" },
            { id: "Only when rate is achieved", title: "Target Only", description: "Only notify when target reached" }
          ]
        }]
      }
    }
  };
  
  await whatsappService.sendMessage(phoneNumber, message);
}

async function requestTargetRate(phoneNumber, flowState) {
  const currentRate = await rateService.getRate(flowState.sellCurrency, flowState.buyCurrency);
  const rateText = currentRate ? `\n\nCurrent rate: ${currentRate.toFixed(6)}` : '';
  
  const message = {
    type: "text",
    text: {
      body: `Perfect! You want to sell ${flowState.sellCurrency} for ${flowState.buyCurrency} with ${flowState.updateFrequency} updates.${rateText}\n\nWhat's your target rate? (e.g., 1.25)`
    }
  };
  
  await whatsappService.sendMessage(phoneNumber, message);
}

async function handleTargetRateInput(phoneNumber, targetRate) {
  const state = userFlowState.get(phoneNumber);
  if (!state?.sellCurrency || !state?.buyCurrency) {
    await whatsappService.sendMessage(phoneNumber, {
      type: "text",
      text: { body: "I couldn't find your rate alert setup. Please start over by typing 'help'." }
    });
    return;
  }

  const target = Number(targetRate);
  if (!Number.isFinite(target) || target <= 0) {
    await whatsappService.sendMessage(phoneNumber, {
      type: "text",
      text: { body: "Please enter a valid positive number for the target rate (e.g., 1.25)." }
    });
    return;
  }

  try {
    const client = await pipeDriveService.searchPersonByPhone(phoneNumber);
    if (!client) {
      await whatsappService.sendMessage(phoneNumber, {
        type: "text",
        text: { body: "Sorry, I couldn't find your client record. Please contact support to set up your account." }
      });
      return;
    }

    const sell = state.sellCurrency.toUpperCase();
    const buy  = state.buyCurrency.toUpperCase();

    const margin = await pipeDriveService.getPersonMargin(client.id) || 0;
    const marketTrigger = target * (1 - margin); // market target derived from client target

    const live = await rateService.getRate(sell, buy); // live MARKET rate
    if (live == null || !Number.isFinite(live)) {
      await whatsappService.sendMessage(phoneNumber, {
        type: "text",
        text: { body: "I couldn't fetch the current rate right now. Please try again shortly." }
      });
      return;
    }

    // ✅ FIX: compare market trigger vs live market rate
    const direction = marketTrigger > live ? 'above' : 'below';
    const normalizedPhone = normalizePhone(phoneNumber);

    try {
      const { rows: [monitor] } = await pool.query(
        `INSERT INTO rate_monitors
         (pd_id, sell_currency, buy_currency, target_client_rate, target_market_rate,
          alert_or_order, update_frequency, trigger_direction, initial_rate, current_rate, phone)
         VALUES ($1,$2,$3,$4,$5,'alert',$6,$7,$8,$8,$9)
         RETURNING *`,
        [ String(client.id), sell, buy, target, marketTrigger, state.updateFrequency || null, direction, live, normalizedPhone ]
      );

      await whatsappService.sendMessage(phoneNumber, {
        type: "text",
        text: { body:
          `✅ Rate alert created! (ID: ${monitor.id})\n\n` +
          `Pair: ${sell}/${buy}\n` +
          `Your target (client): ${target}\n` +
          `Market trigger: ${marketTrigger.toFixed(6)}\n` +
          `Update frequency: ${state.updateFrequency || 'Target Only'}\n` +
          `Current rate: ${live.toFixed(6)}\n\n` +
          `You'll be notified when the rate reaches your target.` }
      });

      // ✅ FIX: success-side cleanup/logging stays inside the try
      userFlowState.delete(phoneNumber);
      console.log('Rate monitor created via WhatsApp:', monitor);
      scheduleNextCheck();
      return; // prevent falling through
    } catch (err) {
      if (err.code === '23505') { // unique_violation
        await whatsappService.sendMessage(phoneNumber, {
          type: "text",
          text: { body: "You already have an active alert for that pair. Cancel it first or choose a different target." }
        });
        return;
      }
      throw err; // bubble up to outer catch
    }

  } catch (err) {
    console.error('Error creating rate monitor:', err);
    await whatsappService.sendMessage(phoneNumber, {
      type: "text",
      text: { body: "Sorry, there was an error creating your alert. Please try again or contact support." }
    });
  }
}

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  res.status(500).json({ 
    error: 'Internal server error',
    timestamp: new Date().toISOString()
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ 
    error: 'Not found',
    path: req.path,
    timestamp: new Date().toISOString()
  });
});

// Initialize and start server
async function start() {
  try {
    console.log('Starting Rate Monitoring Service...');
    
    const requiredEnvVars = [
      'DATABASE_URL',
      'OPENEXCHANGERATES_API_KEY',
      'WHATSAPP_API_KEY',
      'WHATSAPP_PHONE_NUMBER_ID'
    ];
    
    const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);
    if (missingVars.length > 0) {
      console.error('Missing required environment variables:', missingVars);
      process.exit(1);
    }
    
    const optionalEnvVars = [
      'QUOTE_BASE_URL',
      'INTERNAL_SHARED_SECRET', 
      'WA_FLOW_ID',
      'WA_FLOW_TOKEN',
      'RATE_ALERT_HEADER_IMAGE',
      'PIPEDRIVE_API_KEY'
    ];
    
    const missingOptionalVars = optionalEnvVars.filter(varName => !process.env[varName]);
    if (missingOptionalVars.length > 0) {
      console.warn('Missing optional environment variables (some features may not work):', missingOptionalVars);
    }
    
    await initializeDatabase();
    console.log('Database initialized successfully');
    
    scheduleNextCheck();
    console.log('Rate monitoring scheduled');
    
    if (process.env.CHECK_ON_STARTUP === '1') {
      console.log('Running initial rate check...');
      await safeCheckRates();
    }

    const port = process.env.PORT || 3000;
    app.listen(port, () => {
      console.log(`Rate monitoring service running on port ${port}`);
      console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
      console.log(`WhatsApp API Version: ${WA_VERSION}`);
      console.log(`Verbose logging: ${VERBOSE ? 'enabled' : 'disabled'}`);
      console.log(`Circuit breaker max failures: ${maxConsecutiveFailures}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

start();