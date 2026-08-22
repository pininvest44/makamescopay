import express from 'express';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.MAKAME_API_KEY;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// SSE Clients for live progress logs
let clients = [];

app.get('/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  clients.push(res);
  req.on('close', () => {
    clients = clients.filter(client => client !== res);
  });
});

function sendLog(data) {
  clients.forEach(client => client.write(`data: ${JSON.stringify(data)}\n\n`));
}

// Format raw phone string to Safaricom standard (2547XXXXXXXX or 2541XXXXXXXX)
function sanitizePhone(phone) {
  let cleaned = phone.replace(/[^0-9]/g, '');
  if (cleaned.startsWith('0')) {
    cleaned = '254' + cleaned.slice(1);
  } else if (cleaned.startsWith('7') || cleaned.startsWith('1')) {
    cleaned = '254' + cleaned;
  }
  return cleaned;
}

// Queue Engine with 15 requests/minute constraint (4 sec delay per item)
let isProcessing = false;

app.post('/api/stkpush/bulk', async (req, res) => {
  if (isProcessing) {
    return res.status(429).json({ error: 'A bulk job is already running. Please wait.' });
  }

  const { numbers, amount, accountReference, transactionDesc } = req.body;

  if (!numbers || !amount || !accountReference) {
    return res.status(400).json({ error: 'Missing required fields.' });
  }

  const phoneList = numbers
    .split(/[\n,]+/)
    .map(n => sanitizePhone(n.trim()))
    .filter(n => /^254(7|1)\d{8}$/.test(n));

  if (phoneList.length === 0) {
    return res.status(400).json({ error: 'No valid Kenyan numbers provided.' });
  }

  res.json({ message: 'Job queued successfully.', count: phoneList.length });

  // Process queue asynchronously
  isProcessing = true;
  const RATE_LIMIT_DELAY = 4000; // 4000ms = 15 requests/minute

  for (let i = 0; i < phoneList.length; i++) {
    const phone = phoneList[i];
    
    try {
      const apiResponse = await fetch('https://makamescopay.com/api/payments/stkpush', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': API_KEY
        },
        body: JSON.stringify({
          phoneNumber: phone,
          amount: Number(amount),
          accountReference: accountReference,
          transactionDesc: transactionDesc || `Bulk Payment ${i + 1}`
        })
      });

      const responseData = await apiResponse.json();

      if (apiResponse.ok) {
        sendLog({
          status: 'SUCCESS',
          phone,
          index: i + 1,
          total: phoneList.length,
          data: responseData
        });
      } else {
        sendLog({
          status: 'FAILED',
          phone,
          index: i + 1,
          total: phoneList.length,
          error: responseData.message || responseData.error || 'Gateway Rejected Request'
        });
      }
    } catch (err) {
      sendLog({
        status: 'FAILED',
        phone,
        index: i + 1,
        total: phoneList.length,
        error: err.message
      });
    }

    // Delay next item to uphold rate limit (unless it is the last item)
    if (i < phoneList.length - 1) {
      await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY));
    }
  }

  sendLog({ status: 'COMPLETE', message: 'All STK Push requests processed.' });
  isProcessing = false;
});

app.listen(PORT, () => {
  console.log(`Server executing on port ${PORT}`);
});
