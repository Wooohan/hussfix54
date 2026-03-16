export function withTimeout<T>(promise: Promise<T>, ms: number, fallback: T): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([
    promise,
    new Promise<T>(resolve => { timer = setTimeout(() => resolve(fallback), ms); }),
  ]).finally(() => clearTimeout(timer));
}

export function cleanText(text: string | null | undefined): string {
  if (!text) return '';
  return text.replace(/\u00a0/g, ' ').replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
}

export function formatDateForFMCSA(date: Date): string {
  const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
  const day = String(date.getDate()).padStart(2, '0');
  const month = months[date.getMonth()];
  const year = String(date.getFullYear()).slice(-2);
  return `${day}-${month}-${year}`;
}

export function nowStr(): string {
  return new Date().toISOString().split('T')[1].split('.')[0];
}
