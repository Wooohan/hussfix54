import express, { Request, Response } from 'express';
import cors from 'cors';
import rateLimit from 'express-rate-limit';
import axios from 'axios';
import * as cheerio from 'cheerio';
import dotenv from 'dotenv';
import { scrapeCarrier, fetchSafetyData, fetchInsuranceData } from './scraper';
import { taskManager } from './taskManager';
import { cleanText, formatDateForFMCSA } from './utils';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3001;

const ALLOWED_ORIGINS = (process.env.CORS_ORIGINS || 'http://localhost:3000').split(',').map(o => o.trim());

app.use(cors({
  origin: (origin, callback) => {
    if (!origin || ALLOWED_ORIGINS.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
}));
app.use(express.json());

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 15,
  message: { error: 'Too many auth attempts. Try again in 15 minutes.' },
  standardHeaders: true,
  legacyHeaders: false,
});

app.post('/api/fmcsa-register', async (req: Request, res: Response) => {
  try {
    const { date } = req.body;
    
    const registerDate = date || formatDateForFMCSA(new Date());
    
    const registerUrl = 'https://li-public.fmcsa.dot.gov/LIVIEW/PKG_register.prc_reg_detail';
    
    const params = new URLSearchParams();
    params.append('pd_date', registerDate);
    params.append('pv_vpath', 'LIVIEW');

    console.log(`Scraping FMCSA Register for date: ${registerDate}`);

    const response = await axios.post(registerUrl, params.toString(), {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://li-public.fmcsa.dot.gov/LIVIEW/PKG_REGISTER.prc_reg_list',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://li-public.fmcsa.dot.gov'
      },
      timeout: 60000, // Increased timeout for large pages
    });

    if (!response.data.toUpperCase().includes('FMCSA REGISTER')) {
      return res.status(400).json({
        success: false,
        error: 'Invalid response from FMCSA. The page might not be available for this date.',
        entries: []
      });
    }

    const $ = cheerio.load(response.data);
    const rawText = $.text();
    
    const entries: Array<{ number: string; title: string; decided: string; category: string }> = [];
    
    const pattern = /((?:MC|FF|MX|MX-MC)-\d+)\s+([\s\S]*?)\s+(\d{2}\/\d{2}\/\d{4})/g;
    
    let match;
    const categoryKeywords: Record<string, string[]> = {
      'NAME CHANGE': ['NAME CHANGES'],
      'CERTIFICATE, PERMIT, LICENSE': ['CERTIFICATES, PERMITS & LICENSES'],
      'CERTIFICATE OF REGISTRATION': ['CERTIFICATES OF REGISTRATION'],
      'DISMISSAL': ['DISMISSALS'],
      'WITHDRAWAL': ['WITHDRAWAL OF APPLICATION'],
      'REVOCATION': ['REVOCATIONS'],
      'TRANSFERS': ['TRANSFERS'],
      'GRANT DECISION NOTICES': ['GRANT DECISION NOTICES']
    };

    while ((match = pattern.exec(rawText)) !== null) {
      const docket = match[1];
      const rawInfo = match[2];
      const decidedDate = match[3];

      const title = rawInfo.replace(/\s+/g, ' ').trim();
      
      if (title.length > 500) continue; 

      const beforeIndex = match.index;
      const contextText = rawText.substring(Math.max(0, beforeIndex - 1500), beforeIndex).toUpperCase();
      
      let category = 'MISCELLANEOUS';
      for (const [catName, keywords] of Object.entries(categoryKeywords)) {
        if (keywords.some(k => contextText.includes(k))) {
          category = catName;
        }
      }

      entries.push({
        number: docket,
        title,
        decided: decidedDate,
        category
      });
    }

    const uniqueEntries = entries.filter((entry, index, self) =>
      index === self.findIndex((e) => e.number === entry.number && e.title === entry.title)
    );

    console.log(`Extracted ${uniqueEntries.length} entries for ${registerDate}`);

    res.json({
      success: true,
      count: uniqueEntries.length,
      date: registerDate,
      lastUpdated: new Date().toISOString(),
      entries: uniqueEntries
    });

  } catch (error: any) {
    console.error('FMCSA Register scrape error:', error.message);
    res.status(500).json({ 
      success: false,
      error: 'Failed to scrape FMCSA register data', 
      details: error.message,
      entries: []
    });
  }
});


const ALLOWED_DOMAINS = [
  'safer.fmcsa.dot.gov',
  'ai.fmcsa.dot.gov',
  'searchcarriers.com',
  'li-public.fmcsa.dot.gov',
];

app.get('/api/proxy', async (req: Request, res: Response) => {
  const url = req.query.url as string;
  if (!url) return res.status(400).json({ error: 'Missing url parameter' });

  const isAllowed = ALLOWED_DOMAINS.some(domain => url.includes(domain));
  if (!isAllowed) return res.status(403).json({ error: 'Domain not allowed' });

  try {
    const headers: Record<string, string> = url.includes('searchcarriers.com')
      ? {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'application/json, text/plain, */*',
          'Accept-Language': 'en-US,en;q=0.5',
          'Referer': 'https://searchcarriers.com/',
          'Origin': 'https://searchcarriers.com',
        }
      : {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.5',
        };
    const resp = await axios.get(url, { headers, timeout: 30000, maxRedirects: 5 });
    const contentType = resp.headers['content-type'] || 'text/html; charset=utf-8';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.status(resp.status).send(resp.data);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/scrape/carrier/:mcNumber', async (req: Request, res: Response) => {
  try {
    const data = await scrapeCarrier(req.params.mcNumber);
    if (data) return res.json(data);
    res.status(404).json({ error: 'No data found' });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/scrape/safety/:dotNumber', async (req: Request, res: Response) => {
  try {
    const data = await fetchSafetyData(req.params.dotNumber);
    res.json(data);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/scrape/insurance/:dotNumber', async (req: Request, res: Response) => {
  try {
    const data = await fetchInsuranceData(req.params.dotNumber);
    res.json(data);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/tasks/scraper/start', async (req: Request, res: Response) => {
  try {
    const config = req.body.config || {};
    const taskId = await taskManager.startScraperTask(config);
    res.json({ task_id: taskId, status: 'started' });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/tasks/scraper/stop', async (req: Request, res: Response) => {
  const taskId = req.body.task_id;
  if (!taskId) return res.status(400).json({ error: 'task_id required' });
  taskManager.stopTask(taskId);
  res.json({ task_id: taskId, status: 'stopping' });
});

app.get('/api/tasks/scraper/status', async (req: Request, res: Response) => {
  const taskId = req.query.task_id as string;
  if (!taskId) return res.status(400).json({ error: 'task_id required' });
  const status = taskManager.getTaskStatus(taskId);
  if (!status) return res.status(404).json({ error: 'Task not found' });
  res.json(status);
});

app.get('/api/tasks/scraper/data', async (req: Request, res: Response) => {
  const taskId = req.query.task_id as string;
  if (!taskId) return res.status(400).json({ error: 'task_id required' });
  const data = taskManager.getTaskData(taskId);
  if (data === null) return res.status(404).json({ error: 'Task not found' });
  res.json(data);
});

app.post('/api/tasks/insurance/start', async (req: Request, res: Response) => {
  try {
    const config = req.body.config || {};
    const taskId = await taskManager.startInsuranceTask(config);
    res.json({ task_id: taskId, status: 'started' });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/tasks/insurance/stop', async (req: Request, res: Response) => {
  const taskId = req.body.task_id;
  if (!taskId) return res.status(400).json({ error: 'task_id required' });
  taskManager.stopTask(taskId);
  res.json({ task_id: taskId, status: 'stopping' });
});

app.get('/api/tasks/insurance/status', async (req: Request, res: Response) => {
  const taskId = req.query.task_id as string;
  if (!taskId) return res.status(400).json({ error: 'task_id required' });
  const status = taskManager.getTaskStatus(taskId);
  if (!status) return res.status(404).json({ error: 'Task not found' });
  res.json(status);
});

app.get('/api/tasks/active', async (req: Request, res: Response) => {
  const taskType = (req.query.task_type as string) || 'scraper';
  const taskId = taskManager.getActiveTaskId(taskType);
  if (!taskId) return res.json({ task_id: null });
  const status = taskManager.getTaskStatus(taskId);
  res.json({ task_id: taskId, task: status });
});

app.get('/api/tasks', async (_req: Request, res: Response) => {
  res.json(taskManager.listTasks());
});

app.post('/api/auth/login', authLimiter, async (req: Request, res: Response) => {
  res.json({ message: 'Use client-side auth' });
});

app.post('/api/auth/register', authLimiter, async (req: Request, res: Response) => {
  res.json({ message: 'Use client-side auth' });
});

app.get('/health', (req: Request, res: Response) => {
  res.json({ status: 'ok', message: 'FMCSA Scraper Backend is running' });
});

app.get('/healthz', (_req: Request, res: Response) => {
  res.json({ status: 'ok' });
});

app.listen(PORT, () => {
  console.log(`Backend server running on http://localhost:${PORT}`);
});
