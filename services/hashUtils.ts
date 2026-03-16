/**
 * Hash a password using SHA-256 via the Web Crypto API.
 * Returns a hex-encoded hash string.
 */
export const hashPassword = async (password: string): Promise<string> => {
  const encoder = new TextEncoder();
  const data = encoder.encode(password);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
};

/**
 * Verify a password against a stored SHA-256 hash.
 */
export const verifyPassword = async (password: string, storedHash: string): Promise<boolean> => {
  const hash = await hashPassword(password);
  return hash === storedHash;
};
