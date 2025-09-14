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

// at top of server.js (once)
const VERBOSE = process.env.VERBOSE_LOGS === '1';

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

async function initializeDatabase() {
  const createTableAndIndexes = `
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
      trigger_direction VARCHAR(10) DEFAULT 'above' CHECK (trigger_direction IN ('above', 'below')),
      status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'triggered', 'cancelled')),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      triggered_at TIMESTAMP,
      current_rate DECIMAL(10,6),
      initial_rate DECIMAL(10,6),
      last_checked TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_currency_pair ON rate_monitors(sell_currency, buy_currency);
    CREATE INDEX IF NOT EXISTS idx_status ON rate_monitors(status);
  `;

  const alterTable = `
    ALTER TABLE rate_monitors
      ADD COLUMN IF NOT EXISTS update_frequency VARCHAR(50),
      ADD COLUMN IF NOT EXISTS phone VARCHAR(32);
  `;

  const addNextUpdateAt = `
  ALTER TABLE rate_monitors
    ADD COLUMN IF NOT EXISTS next_update_at TIMESTAMPTZ;

  CREATE INDEX IF NOT EXISTS idx_next_update_due
    ON rate_monitors(next_update_at)
    WHERE status = 'active'
      AND update_frequency IN ('Daily','Weekly');
`;

  // Enforce one active monitor per (pd_id, pair, phone)
  const createPartialUnique = `
    CREATE UNIQUE INDEX IF NOT EXISTS uniq_active_monitor
    ON rate_monitors (pd_id, sell_currency, buy_currency, phone)
    WHERE status = 'active';
  `;

  // 🔽 NEW: margin + generated current_client_rate
  // (If you don't want an upper limit, remove "AND margin <= 1" in the CHECK.)
  const addMarginAndClientRate = `
  ALTER TABLE rate_monitors
    ADD COLUMN IF NOT EXISTS margin NUMERIC(10,6)
    CHECK (margin >= 0);  -- no upper cap

  -- drop any plain current_client_rate so we can re-add as generated
  ALTER TABLE rate_monitors DROP COLUMN IF EXISTS current_client_rate;

  -- client = market / (1 + margin)
  ALTER TABLE rate_monitors
    ADD COLUMN current_client_rate NUMERIC(10,6)
    GENERATED ALWAYS AS (
      ROUND( current_rate / NULLIF(1 + COALESCE(margin, 0), 0), 6 )
    ) STORED;

  ALTER TABLE rate_monitors ALTER COLUMN margin SET DEFAULT 0.005;
  UPDATE rate_monitors SET margin = 0.005 WHERE margin IS NULL;
  `;


  try {
    await pool.query(createTableAndIndexes);
    await pool.query(alterTable);
    await pool.query(createPartialUnique);
    await pool.query(addMarginAndClientRate);
    await pool.query(addNextUpdateAt);
    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Database initialization error:', error);
  }
}

// Simple phone normalizer -> E.164-ish (best effort).
function normalizePhone(input) {
  const raw = String(input || '').trim();
  if (!raw) return null;

  // keep digits and '+' only
  const cleaned = raw.replace(/[^\d+]/g, '');

  // already looks like +<digits> → use as-is
  if (/^\+\d{6,}$/.test(cleaned)) return cleaned;

  // otherwise prepend default country code if provided
  const cc = (process.env.DEFAULT_COUNTRY_CODE || '').replace(/\D/g, '');
  const digits = cleaned.replace(/\D/g, '').replace(/^0+/, ''); // strip non-digits and leading zeros

  return cc ? `+${cc}${digits}` : `+${digits}`;
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
  
  async sendMessage(phoneNumber, messageData) {
    try {
      const response = await axios.post(
        `${this.baseURL}${process.env.WHATSAPP_PHONE_NUMBER_ID}/messages`,
        {
          messaging_product: "whatsapp",
          to: phoneNumber,
          ...messageData
        },
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
  
  async sendTemplateMessage(phoneNumber, templateName, parameters, headerImage = null) {
    try {
      const components = [];
      
      // Add header component if image is provided
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
      
      // Add body component if parameters are provided
      if (parameters && parameters.length > 0) {
        const body_parameters = parameters.map(param => 
          typeof param === 'object' ? param : { type: "text", text: param }
        );
        
        components.push({
          type: "body",
          parameters: body_parameters
        });
      }
      
      const payload = {
        messaging_product: "whatsapp",
        to: phoneNumber,
        type: "template",
        template: {
          name: templateName,
          language: { code: "en" }
        }
      };
      
      // Only add components if we have any
      if (components.length > 0) {
        payload.template.components = components;
      }
      
      if (VERBOSE) {
        console.log('Sending template payload:', JSON.stringify(payload, null, 2));
      }
      
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
      console.error('WhatsApp template message error:', error);
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

// PipeDrive integration service
class PipeDriveService {
  constructor() {
    this.apiKey = process.env.PIPEDRIVE_API_KEY;
    this.baseURL = process.env.PIPEDRIVE_BASE_URL || 'https://api.pipedrive.com/v1';
  }
  
  async getPersonById(personId) {
    try {
      const response = await axios.get(
        `${this.baseURL}/persons/${personId}?api_token=${this.apiKey}`
      );
      return response.data.data;
    } catch (error) {
      console.error(`Error fetching person ${personId}:`, error);
      return null;
    }
  }
  
  async getPersonMargin(personId) {
    try {
      const person = await this.getPersonById(personId);
      if (!person) return null;
      
      // Look for margin in custom fields using the actual API key
      const marginValue = person['d12fb97c7cf4908a8e57c8693aa187e64302413f'] || 
                         person['margin'] || 
                         0.5; // Default 0.5% margin if not found
      
      // Convert percentage to decimal (0.5 -> 0.005)
      return parseFloat(marginValue) / 100;
    } catch (error) {
      console.error(`Error fetching margin for person ${personId}:`, error);
      return 0.005; // Default 0.5% margin as decimal
    }
  }
  
  async searchPersonByPhone(phoneNumber) {
    try {
      // Clean phone number (remove + and spaces)
      const cleanPhone = phoneNumber.replace(/[\+\s\-\(\)]/g, '');
      
      const response = await axios.get(
        `${this.baseURL}/persons/search?term=${cleanPhone}&api_token=${this.apiKey}`
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

// Get current rate for a currency pair
app.get('/api/rate/:from/:to', async (req, res) => {
  try {
    const rate = await rateService.getRate(req.params.from, req.params.to);
    res.json({ rate });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Core monitoring function
async function checkRates() {
  console.log('Starting rate check...', new Date().toISOString());

  try {
    const { rows: monitors } = await pool.query(
      'SELECT * FROM rate_monitors WHERE status = $1',
      ['active']
    );

    const triggeredMonitors = [];

    for (const monitor of monitors) {
      const currentRate = await rateService.getRate(
        monitor.sell_currency,
        monitor.buy_currency
      );

      if (!currentRate) {
        console.log(`Could not fetch rate for ${monitor.sell_currency}/${monitor.buy_currency}`);
        continue;
      }

      // Update snapshot and read DB-computed client rate in same statement
      const { rows: [snap] } = await pool.query(
        `UPDATE rate_monitors
           SET current_rate = $1,
               last_checked = NOW()
         WHERE id = $2
         RETURNING current_rate, current_client_rate, margin`,
        [currentRate, monitor.id]
      );

      const dbCurrent = Number(snap.current_rate);
      const clientNow = Number(snap.current_client_rate);

      const targetMet =
        monitor.trigger_direction === 'above'
          ? dbCurrent >= Number(monitor.target_market_rate)
          : dbCurrent <= Number(monitor.target_market_rate);

      if (targetMet) {
        triggeredMonitors.push({
          monitor,
          currentRate: dbCurrent,
          currentClientRate: clientNow
        });
      }
    }

    // Process triggers after scanning all rows
    for (const item of triggeredMonitors) {
      const { monitor, currentRate, currentClientRate } = item;

      // Atomic claim to avoid dup sends
      const claim = await pool.query(
        `UPDATE rate_monitors
           SET status = 'triggered', triggered_at = NOW()
         WHERE id = $1 AND status = 'active'
         RETURNING id`,
        [monitor.id]
      );
      if (claim.rowCount === 0) continue;

      if (monitor.alert_or_order === 'alert') {
        await notifyQuoteTriggered(monitor, { currentRate, currentClientRate });
      } else {
        await handleOrder(monitor, currentRate);
      }
    }

    console.log('Rate check completed');
  } catch (error) {
    console.error('Rate check error:', error);
  }
}

// <<< add this near checkRates() >>>
async function sendDueUpdates() {
  // due = active + Daily/Weekly + next_update_at reached
  const { rows: due } = await pool.query(
    `SELECT * FROM rate_monitors
     WHERE status = 'active'
       AND update_frequency IN ('Daily','Weekly')
       AND next_update_at IS NOT NULL
       AND next_update_at <= NOW()`
  );

  for (const monitor of due) {
    // fresh market snapshot
    const currentRate = await rateService.getRate(monitor.sell_currency, monitor.buy_currency);
    if (!currentRate) continue;

    // update snapshot & get DB-computed client rate
    const { rows: [snap] } = await pool.query(
      `UPDATE rate_monitors
         SET current_rate = $1,
             last_checked = NOW()
       WHERE id = $2
       RETURNING current_client_rate`,
      [currentRate, monitor.id]
    );

    // ✅ THIS is the line you asked about
    await notifyQuoteUpdate(monitor, {
      currentClientRate: Number(snap.current_client_rate),
      period: monitor.update_frequency
    });

    // bump next_update_at by 1 day or 7 days to keep the same time-of-day
    const bump = monitor.update_frequency === 'Weekly' ? '7 days' : '1 day';
    await pool.query(
      `UPDATE rate_monitors
         SET next_update_at = (next_update_at + INTERVAL '${bump}')
       WHERE id = $1`,
      [monitor.id]
    );
  }
}


async function handleAlert(monitor, currentRate) {
  try {
    // Get client phone number (you'll need to modify this based on your client data structure)
    const clientData = await getClientData(monitor.pd_id);
    
    await whatsappService.sendTemplateMessage(
      clientData.phone,
      'rate_alert', // You'll need to create this template in WhatsApp Business
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

// --- Internal auth helper (PUT THIS HERE, once, before your routes) ---
function requireInternal(req, res, next) {
  if (req.headers['x-internal-secret'] !== process.env.INTERNAL_SHARED_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

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

app.post('/api/monitors', async (req, res) => {
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
      // 👇 accept what Quote sends
      updateFrequency,        // e.g. "daily" | "weekly" | "on_target"
      updateFrequencyLabel,   // e.g. "Daily" | "Weekly" | "Only when rate is achieved"
      phone
    } = req.body || {};

    // inside /api/monitors handler
    if (VERBOSE) {
      // avoid gigantic payloads
      const pretty = JSON.stringify(req.body, null, 2);
      console.log('MONITOR CREATE BODY:', pretty.length > 4000 ? pretty.slice(0, 4000) + ' …(truncated)' : pretty);
    }

    // Get current rate to store as initial/current reference
    const currentRate = await rateService.getRate(sellCurrency, buyCurrency);

    // Normalize frequency for DB (store human-readable)
    const rawFreq = (updateFrequency || updateFrequencyLabel || '').toString().trim().toLowerCase();
    let dbFreq = null; // keep null if nothing sent
    if (rawFreq.startsWith('daily')) {
      dbFreq = 'Daily';
    } else if (rawFreq.startsWith('week')) {
      dbFreq = 'Weekly';
    } else if (rawFreq === 'on_target' || (rawFreq.includes('only') && rawFreq.includes('achiev'))) {
      dbFreq = 'Only when rate is achieved';
    }

    // --- phone normalization & validation (ADD THIS BLOCK) ---
    const phoneNorm = normalizePhone(phone) || null;
    if (phone && (!phoneNorm || !/^\+\d{10,15}$/.test(phoneNorm))) {
      return res.status(400).json({ error: 'Invalid phone number format' });
    }

    // Next update time aligned to creation moment
    let nextUpdateAt = null;
    if (dbFreq === 'Daily') {
      nextUpdateAt = new Date(Date.now() + 24*60*60*1000).toISOString();
    } else if (dbFreq === 'Weekly') {
      nextUpdateAt = new Date(Date.now() + 7*24*60*60*1000).toISOString();
    }

    // margin: default 0.5%, override from Pipedrive if present and non-negative
    let margin = 0.005;
    try {
      const m = await pipeDriveService.getPersonMargin(pdId);
      if (Number.isFinite(m) && m >= 0) margin = m;
    } catch (_) {
      if (VERBOSE) console.log('getPersonMargin failed; using default', margin);
    }

    // Derive market trigger if caller didn't supply one (market = client * (1 + margin))
    const finalMarketRate = targetMarketRate ?? (Number(targetClientRate) * (1 + margin));

    // Re-derive direction if not provided, using the *final* market rate
    const finalDirection = triggerDirection || (finalMarketRate > currentRate ? 'above' : 'below');

    // INSERT — note finalMarketRate + phoneNorm + margin
    const result = await pool.query(
      `INSERT INTO rate_monitors 
        (pd_id, sell_currency, buy_currency, sell_amount, buy_amount, 
          target_client_rate, target_market_rate, alert_or_order, trigger_direction, 
          initial_rate, current_rate, update_frequency, phone, margin, next_update_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
      RETURNING *`,
      [
        pdId, sellCurrency, buyCurrency, sellAmount, buyAmount,
        targetClientRate, finalMarketRate, alertOrOrder, finalDirection,
        currentRate, currentRate, dbFreq, phoneNorm, margin, nextUpdateAt
      ]
    );


    res.status(201).json(result.rows[0]);
  } catch (error) {
    // Turn duplicate-active-monitor into a 409 so Quote can show a friendly message
    if (error?.code === '23505' && (error?.constraint === 'uniq_active_monitor' ||
                                     /uniq_active_monitor/i.test(error?.detail || ''))) {
      return res.status(409).json({
        error: 'duplicate_monitor',
        message: 'You already have an active alert for this currency pair. Cancel it first or choose a different target.'
      });
    }
  
    console.error('Create monitor error:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// 2) Stop updates → only notify when target is achieved
app.patch('/api/monitors/:id/stop-updates', requireInternal, async (req, res) => {
  const { id } = req.params;
  try {
    const { rows } = await pool.query(
      `UPDATE rate_monitors
         SET update_frequency = 'Only when rate is achieved',
          next_update_at   = NULL
       WHERE id = $1
       RETURNING *`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ error: 'not_found', message: 'Monitor not found' });
    }
    return res.json(rows[0]);
  } catch (err) {
    console.error('stop-updates error:', err);
    return res.status(500).json({ error: 'internal', message: 'Failed to stop updates' });
  }
});

// PATCH /api/monitors/:id/update-target
app.patch('/api/monitors/:id/update-target', requireInternal, async (req, res) => {
  const { id } = req.params;
  const newClient = Number(req.body?.targetClientRate);

  if (!Number.isFinite(newClient) || newClient <= 0) {
    return res.status(400).json({ error: 'invalid_target_client_rate' });
  }

  try {
    // Read margin/current/status
    const { rows: pre } = await pool.query(
      `SELECT margin, current_rate, status
         FROM rate_monitors
        WHERE id = $1`,
      [id]
    );
    if (pre.length === 0) return res.status(404).json({ error: 'not_found' });

    // Only allow updates on active rows
    if (pre[0].status !== 'active') {
      return res.status(409).json({
        error: 'not_active',
        message: 'Only active monitors can change the target.'
      });
    }

    const margin = Number(pre[0].margin ?? 0);
    const currentRate = Number(pre[0].current_rate ?? 0);

    // market = client * (1 + margin)
    const marketTarget = Math.round(newClient * (1 + margin) * 1e6) / 1e6;
    const direction = marketTarget > currentRate ? 'above' : 'below';

    const { rows: updated } = await pool.query(
      `UPDATE rate_monitors
          SET target_client_rate = $2,
              target_market_rate = $3,
              trigger_direction  = $4
        WHERE id = $1 AND status = 'active'
        RETURNING *`,
      [id, newClient, marketTarget, direction]
    );

    return res.json(updated[0]);
  } catch (err) {
    console.error('update-target error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// send the "alert_update" (Daily/Weekly) template via Quote
async function notifyQuoteUpdate(monitor, { currentClientRate, period }) {
  try {
    if (!process.env.QUOTE_BASE_URL || !process.env.INTERNAL_SHARED_SECRET) return;
    if (!monitor.phone) return;

    const url = `${process.env.QUOTE_BASE_URL}/api/send-alert-update`;
    const payload = {
      phone: monitor.phone,
      sellCurrency: monitor.sell_currency,
      buyCurrency: monitor.buy_currency,
      targetClientRate: Number(monitor.target_client_rate),
      currentClientRate: Number(currentClientRate),
      // Quote expects "period": "daily" | "weekly"
      period: (period || monitor.update_frequency || 'Daily')
                .toString().toLowerCase().startsWith('week') ? 'weekly' : 'daily',
      monitorId: monitor.id,
    };

    if (VERBOSE) console.log('📤 POST to Quote (alert_update)', { url, payload });
    await axios.post(url, payload, {
      headers: { 'x-internal-secret': process.env.INTERNAL_SHARED_SECRET },
      timeout: 10000
    });
  } catch (e) {
    console.error('❌ notifyQuoteUpdate failed', {
      status: e.response?.status,
      data: e.response?.data,
      message: e.message
    });
  }
}


// tell Quote to send the "alert_triggerd" template
async function notifyQuoteTriggered(monitor, { currentRate, currentClientRate }) {
  try {
    if (!process.env.QUOTE_BASE_URL) {
      console.error('❌ QUOTE_BASE_URL not set');
      return;
    }
    if (!process.env.INTERNAL_SHARED_SECRET) {
      console.error('❌ INTERNAL_SHARED_SECRET not set');
      return;
    }
    if (!monitor.phone) {
      console.error('❌ monitor has no phone; cannot notify Quote', { id: monitor.id });
      return;
    }

    const url = `${process.env.QUOTE_BASE_URL}/api/send-alert-triggered`;
    const payload = {
      phone: monitor.phone,
      sellCurrency: monitor.sell_currency,
      buyCurrency: monitor.buy_currency,
      targetClientRate: Number(monitor.target_client_rate),
      // Prefer DB-computed client rate at trigger time; fall back to target if missing
      currentClientRate: Number(currentClientRate ?? monitor.target_client_rate)
    };

    if (VERBOSE) console.log('📤 POST to Quote', { url, payload });

    const r = await axios.post(url, payload, {
      headers: { "x-internal-secret": process.env.INTERNAL_SHARED_SECRET },
      timeout: 10000
    });
    if (VERBOSE) console.log('✅ Quote responded', { status: r.status, data: r.data });
  } catch (e) {
    console.error('❌ notifyQuoteTriggered failed', {
      status: e.response?.status,
      data: e.response?.data,
      message: e.message
    });
  }
}

// CANCEL an active monitor
app.patch('/api/monitors/:id/cancel', requireInternal, async (req, res) => {
  const { id } = req.params;
  try {
    const { rowCount, rows } = await pool.query(
      `UPDATE rate_monitors
         SET status = 'cancelled',
          next_update_at   = NULL
       WHERE id = $1 AND status = 'active'
       RETURNING *`,
      [id]
    );
    if (rowCount === 0) {
      return res.status(409).json({ error: 'not_active_or_missing' });
    }
    return res.json(rows[0]);
  } catch (e) {
    console.error('cancel endpoint error', e);
    return res.status(500).json({ error: 'cancel_failed' });
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

// WhatsApp webhook handler for incoming messages
app.post('/webhook/whatsapp', express.raw({type: 'application/json'}), async (req, res) => {
  try {
    const body = JSON.parse(req.body.toString('utf8'));
    
    // Verify webhook (WhatsApp security)
    if (body.object === 'whatsapp_business_account') {
      body.entry?.forEach(entry => {
        entry.changes?.forEach(change => {
          if (change.field === 'messages') {
            const message = change.value.messages?.[0];
            if (message) {
              handleIncomingMessage(message, change.value);
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

// WhatsApp webhook verification (required by Meta)
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


// CTA endpoint used by Quote to send the Flow-enabled rate_alert template
app.post('/api/send-rate-alert-cta', async (req, res) => {
  try {
    if (req.headers['x-internal-secret'] !== process.env.INTERNAL_SHARED_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { phone } = req.body || {};
    if (!phone) return res.status(400).json({ error: 'phone required' });

    const headerImage =
      process.env.RATE_ALERT_HEADER_IMAGE ||
      "https://raw.githubusercontent.com/Cornolly/summitfx-assets/main/Logo%20standard.png";

    const payload = {
      messaging_product: "whatsapp",
      to: phone,
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
                  flow_id: process.env.WA_FLOW_ID,          // Flow ID from WA Flows
                  flow_cta: "Create rate alert",             // must match your template button text
                  flow_action: "data_exchange",
                  flow_action_payload: { screen: "Rate alert" }
                })
              }
            ]
          }
        ]
      }
    };

    const resp = await axios.post(
      `${process.env.WHATSAPP_BASE_URL || 'https://graph.facebook.com/v19.0/'}${process.env.WHATSAPP_PHONE_NUMBER_ID}/messages`,
      payload,
      { headers: { Authorization: `Bearer ${process.env.WHATSAPP_API_KEY}`, 'Content-Type': 'application/json' } }
    );

    return res.json({ message_id: resp.data?.messages?.[0]?.id || null, raw: resp.data });
  } catch (e) {
    console.error('send-rate-alert-cta error', e.response?.data || e.message);
    return res.status(500).json({ error: 'failed_to_send', details: e.response?.data || e.message });
  }
});


// Test endpoint to send rate_alert template with correct format
app.post('/api/test-template/:phoneNumber', async (req, res) => {
  try {
    const phoneNumber = req.params.phoneNumber;
    
    console.log('=== TESTING TEMPLATE SEND ===');
    console.log('Phone number:', phoneNumber);
    
    // Your template has a dynamic IMAGE parameter in the header
    // inside /api/test-template/:phoneNumber
    const payload = {
      messaging_product: "whatsapp",
      to: phoneNumber,                 // e.g. "447873884142"
      type: "template",
      template: {
        name: "rate_alert",
        language: { code: "en" },      // your template is English
        components: [
          // Media header (your template uses an Image header)
          {
            type: "header",
            parameters: [
              {
                type: "image",
                image: {
                  link:
                    process.env.RATE_ALERT_HEADER_IMAGE ||
                    "https://raw.githubusercontent.com/Cornolly/summitfx-assets/main/Logo%20standard.png"
                }
              }
            ]
          },
          // Flow button (required because the template CTA is "Complete Flow")
          {
            type: "button",
            sub_type: "flow",
            index: "0",
            parameters: [
              {
                type: "payload",
                payload: JSON.stringify({
                  flow_message_version: "3",        // required
                  flow_token: process.env.WA_FLOW_TOKEN, // any opaque string you set
                  flow_id: process.env.WA_FLOW_ID,         // <-- set this (from Flows, not template id)
                  flow_cta: "Create rate alert",           // <-- must exactly match button text in template
                  flow_action: "data_exchange",          // or "data_exchange" depending on your Flow
                  flow_action_payload: {
                    screen: "Rate alert"                   // <-- matches your pre-defined screen name
                  }
                })
              }
            ]
          }
        ]
      }
    };

    if (VERBOSE) {
      const pretty = JSON.stringify(payload, null, 2);
      console.log(
        'Sending template payload:',
        pretty.length > 2000 ? pretty.slice(0, 2000) + ' …(truncated)' : pretty
      );
    }
    
    const response = await axios.post(
      `${process.env.WHATSAPP_BASE_URL || 'https://graph.facebook.com/v17.0/'}${process.env.WHATSAPP_PHONE_NUMBER_ID}/messages`,
      payload,
      {
        headers: {
          'Authorization': `Bearer ${process.env.WHATSAPP_API_KEY}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    console.log('Template sent successfully:', response.data);
    res.json({ 
      message: 'Template sent successfully', 
      whatsapp_response: response.data 
    });
  } catch (error) {
    console.error('=== TEMPLATE SEND ERROR ===');
    console.error('Error response data:', JSON.stringify(error.response?.data, null, 2));
    
    res.status(500).json({ 
      error: 'Failed to send template',
      details: error.message,
      whatsapp_error: error.response?.data
    });
  }
});



// Also add a simple test endpoint to verify WhatsApp connection
app.post('/api/test-whatsapp-config', (req, res) => {
  const config = {
    hasApiKey: !!process.env.WHATSAPP_API_KEY,
    hasPhoneNumberId: !!process.env.WHATSAPP_PHONE_NUMBER_ID,
    hasVerifyToken: !!process.env.WHATSAPP_VERIFY_TOKEN,
    phoneNumberId: process.env.WHATSAPP_PHONE_NUMBER_ID ? 'Set' : 'Missing',
    baseUrl: process.env.WHATSAPP_BASE_URL || 'https://graph.facebook.com/v17.0/'
  };
  
  res.json(config);
});

// Enhanced webhook logging to capture template responses
async function handleIncomingMessage(message, messageData) {
  console.log('=== INCOMING WHATSAPP MESSAGE ===');
  console.log('Message ID:', message.id);
  console.log('Message Type:', message.type);
  if (VERBOSE) {
    console.log('Full message object:', JSON.stringify(message, null, 2));
    console.log('Full messageData object:', JSON.stringify(messageData, null, 2));
  }
  console.log('=== END MESSAGE LOG ===');

  
  try {
    const phoneNumber = message.from;
    const messageType = message.type;
    
    // Handle interactive message responses (from template buttons/lists)
    if (messageType === 'interactive') {
      await handleInteractiveMessage(message, phoneNumber);
    }
    // Handle text messages
    else if (messageType === 'text') {
      await handleTextMessage(message, phoneNumber);
    }
    // Handle template responses (when user responds to your rate_alert template)
    else if (messageType === 'button' || messageType === 'list_reply') {
      // Check if this is a response to the rate_alert template
      const context = messageData.context;
      if (context && context.from === process.env.WHATSAPP_PHONE_NUMBER_ID) {
        await handleTemplateResponse(message, phoneNumber);
      }
    }
  } catch (error) {
    console.error('Error handling incoming message:', error);
  }
}

async function handleTemplateResponse(message, phoneNumber) {
  console.log('Handling template response for rate_alert');
  
  // Check if message contains structured data from your rate_alert template
  // The exact structure depends on how your template is set up in Meta Business
  
  // If your template sends structured data, it might look like this:
  const messageText = message.text?.body;
  
  // Parse the template response data
  // You'll need to adjust this based on your actual template format
  if (messageText && messageText.includes('rate_alert')) {
    // Extract data from the template response
    // This is a placeholder - adjust based on your actual template structure
    await startRateAlertFlow(phoneNumber);
  }
}

async function handleInteractiveMessage(message, phoneNumber) {
  const interactive = message.interactive;
  
  // Handle template response for rate alert creation
  if (interactive.type === 'button_reply') {
    const buttonId = interactive.button_reply.id;
    
    if (buttonId === 'create_rate_alert') {
      // Start rate alert creation flow
      await startRateAlertFlow(phoneNumber);
    }
  }
  
  // Handle list responses (currency selection, frequency selection)
  else if (interactive.type === 'list_reply') {
    const listReply = interactive.list_reply;
    await handleListSelection(phoneNumber, listReply);
  }
}

async function handleTextMessage(message, phoneNumber) {
  const text = message.text.body.toLowerCase().trim();
  
  // Handle target rate input
  if (text.match(/^\d+\.?\d*$/)) {
    await handleTargetRateInput(phoneNumber, parseFloat(text));
  }
}

async function startRateAlertFlow(phoneNumber) {
  // Send currency selection message
  const currencyMessage = {
    messaging_product: "whatsapp",
    to: phoneNumber,
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

// Store user flow state (in production, use Redis or database)
const userFlowState = new Map();

async function handleListSelection(phoneNumber, listReply) {
  const selection = listReply.id;
  const currentState = userFlowState.get(phoneNumber) || {};
  
  // Handle sell currency selection
  if (!currentState.sellCurrency) {
    currentState.sellCurrency = selection;
    userFlowState.set(phoneNumber, currentState);
    
    // Send buy currency selection
    await sendBuyCurrencySelection(phoneNumber, selection);
  }
  // Handle buy currency selection
  else if (!currentState.buyCurrency) {
    currentState.buyCurrency = selection;
    userFlowState.set(phoneNumber, currentState);
    
    // Send frequency selection
    await sendFrequencySelection(phoneNumber);
  }
  // Handle frequency selection
  else if (!currentState.updateFrequency) {
    currentState.updateFrequency = selection;
    userFlowState.set(phoneNumber, currentState);
    
    // Request target rate
    await requestTargetRate(phoneNumber, currentState);
  }
}

async function sendBuyCurrencySelection(phoneNumber, sellCurrency) {
  // Filter out the sell currency from buy options
  const currencies = [
    { id: "USD", title: "USD", description: "US Dollar" },
    { id: "EUR", title: "EUR", description: "Euro" },
    { id: "GBP", title: "GBP", description: "British Pound" },
    { id: "JPY", title: "JPY", description: "Japanese Yen" },
    { id: "CAD", title: "CAD", description: "Canadian Dollar" },
    { id: "AUD", title: "AUD", description: "Australian Dollar" }
  ].filter(curr => curr.id !== sellCurrency);
  
  const message = {
    messaging_product: "whatsapp",
    to: phoneNumber,
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
    messaging_product: "whatsapp",
    to: phoneNumber,
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
  const message = {
    messaging_product: "whatsapp",
    to: phoneNumber,
    type: "text",
    text: {
      body: `Perfect! You want to sell ${flowState.sellCurrency} for ${flowState.buyCurrency} with ${flowState.updateFrequency} updates.\n\nWhat's your target rate? (e.g., 1.25)`
    }
  };
  
  await whatsappService.sendMessage(phoneNumber, message);
}

async function handleTargetRateInput(phoneNumber, targetRate) {
  const currentState = userFlowState.get(phoneNumber);
  
  if (!currentState || !currentState.sellCurrency || !currentState.buyCurrency) {
    // User sent a number but we don't have their flow state
    return;
  }
  
  try {
    // Find client in PipeDrive by phone number
    const client = await pipeDriveService.searchPersonByPhone(phoneNumber);
    if (!client) {
      await whatsappService.sendMessage(phoneNumber, {
        messaging_product: "whatsapp",
        to: phoneNumber,
        type: "text",
        text: { body: "Sorry, I couldn't find your client record. Please contact support." }
      });
      return;
    }
    
    // Get client margin from PipeDrive
    const clientMargin = await pipeDriveService.getPersonMargin(client.id);
    
    // Calculate market rate (client rate minus margin)
    const marketRate = targetRate * (1 - clientMargin);
    
    // Create the rate monitor
    const monitor = await pool.query(
      `INSERT INTO rate_monitors 
       (pd_id, sell_currency, buy_currency, target_client_rate, target_market_rate, 
        alert_or_order, update_frequency, trigger_direction, initial_rate, current_rate)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9) RETURNING *`,
      [
        client.id.toString(),
        currentState.sellCurrency,
        currentState.buyCurrency,
        targetRate,
        marketRate,
        'alert', // Default to alert
        currentState.updateFrequency,
        'above', // Will be determined by auto-detection logic
        await rateService.getRate(currentState.sellCurrency, currentState.buyCurrency)
      ]
    );
    
    // Send confirmation message
    await whatsappService.sendMessage(phoneNumber, {
      messaging_product: "whatsapp",
      to: phoneNumber,
      type: "text",
      text: {
        body: `✅ Rate alert created successfully!\n\nCurrency: ${currentState.sellCurrency}/${currentState.buyCurrency}\nYour target rate: ${targetRate}\nMarket trigger rate: ${marketRate.toFixed(6)}\nUpdate frequency: ${currentState.updateFrequency}\n\nYou'll be notified when the rate reaches your target.`
      }
    });
    
    // Clear flow state
    userFlowState.delete(phoneNumber);
    
    console.log('Rate monitor created via WhatsApp:', monitor.rows[0]);
    
  } catch (error) {
    console.error('Error creating rate monitor:', error);
    await whatsappService.sendMessage(phoneNumber, {
      messaging_product: "whatsapp",
      to: phoneNumber,
      type: "text", 
      text: { body: "Sorry, there was an error creating your alert. Please try again." }
    });
  }
}

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Dynamic scheduling based on proximity to targets
let currentCronJob = null;
let currentUpdateJob = null;

function scheduleNextCheck() {
  // Cancel existing job if any (only the rate-check job)
  if (currentCronJob) currentCronJob.stop();

  pool.query('SELECT * FROM rate_monitors WHERE status = $1', ['active'])
    .then(result => {
      let needsFrequentCheck = false;

      for (const monitor of result.rows) {
        if (monitor.current_rate) {
          const proximityPercent = Math.abs(monitor.current_rate - monitor.target_market_rate) / monitor.target_market_rate;
          if (proximityPercent <= 0.002) { // Within 0.2%
            needsFrequentCheck = true;
            break;
          }
        }
      }

      if (needsFrequentCheck) {
        console.log('Close to targets detected - switching to 1-minute checks');
        currentCronJob = cron.schedule('* * * * *', () => {
          console.log('High-frequency rate check triggered');
          checkRatesAndReschedule();
        });
      } else {
        console.log('Normal monitoring - using 15-minute checks');
        currentCronJob = cron.schedule('*/15 * * * *', () => {
          console.log('Standard rate check triggered');
          checkRatesAndReschedule();
        });
      }

      // 🔹 Make sure the DAILY/WEEKLY update pinger runs every minute
      if (!currentUpdateJob) {
        currentUpdateJob = cron.schedule('* * * * *', async () => {
          try {
            await sendDueUpdates();  // calls Quote with alert_update where next_update_at <= now()
          } catch (e) {
            console.error('sendDueUpdates error', e);
          }
        });
      }
    })
    .catch(err => {
      console.error('Error determining check frequency:', err);
      // Fallback to 15-minute checks
      currentCronJob = cron.schedule('*/15 * * * *', () => {
        console.log('Fallback rate check triggered');
        checkRatesAndReschedule();
      });

      // 🔹 Also ensure the updates job is running in the fallback path
      if (!currentUpdateJob) {
        currentUpdateJob = cron.schedule('* * * * *', async () => {
          try {
            await sendDueUpdates();
          } catch (e) {
            console.error('sendDueUpdates error', e);
          }
        });
      }
    });
}


async function checkRatesAndReschedule() {
  await checkRates();
  // Reschedule based on new conditions after rate check
  scheduleNextCheck();
}


// Initialize and start server
async function start() {
  await initializeDatabase();   // make sure tables/columns/indexes exist
  scheduleNextCheck();          // now it's safe to start the cron logic
  // (optional) run one immediate check:
  // await checkRates();

  const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(`Rate monitoring service running on port ${port}`);
  });
}

start().catch(console.error);