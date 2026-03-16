import type { VercelRequest, VercelResponse } from '@vercel/node';

export default function handler(req: VercelRequest, res: VercelResponse) {
  const forwarded = req.headers['x-forwarded-for'];
  const ip = typeof forwarded === 'string'
    ? forwarded.split(',')[0].trim()
    : req.headers['x-real-ip'] as string || req.socket?.remoteAddress || '';

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.json({ ip });
}
