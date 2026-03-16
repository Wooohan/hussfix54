import { supabase } from './supabaseClient';
import { User, BlockedIP } from '../types';
import { verifyPassword } from './hashUtils';

const dbRowToUser = (row: any): User => ({
  id: row.user_id,
  name: row.name,
  email: row.email,
  role: row.role as 'user' | 'admin',
  plan: row.plan as 'Free' | 'Starter' | 'Pro' | 'Enterprise',
  dailyLimit: row.daily_limit,
  recordsExtractedToday: row.records_extracted_today,
  lastActive: row.last_active || 'Never',
  ipAddress: row.ip_address || '',
  isOnline: row.is_online || false,
  isBlocked: row.is_blocked || false
});

const userToDbRow = (user: User) => ({
  user_id: user.id,
  name: user.name,
  email: user.email,
  role: user.role,
  plan: user.plan,
  daily_limit: user.dailyLimit,
  records_extracted_today: user.recordsExtractedToday,
  last_active: user.lastActive,
  ip_address: user.ipAddress,
  is_online: user.isOnline,
  is_blocked: user.isBlocked || false
});

export const fetchUsersFromSupabase = async (): Promise<User[]> => {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Error fetching users:', error);
      return [];
    }

    return (data || []).map(dbRowToUser);
  } catch (err) {
    console.error('Error in fetchUsersFromSupabase:', err);
    return [];
  }
};

export const fetchUserByEmail = async (email: string): Promise<User | null> => {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('email', email.toLowerCase())
      .single();

    if (error || !data) {
      return null;
    }

    return dbRowToUser(data);
  } catch (err) {
    console.error('Error in fetchUserByEmail:', err);
    return null;
  }
};

export const createUserInSupabase = async (user: User, passwordHash?: string): Promise<User | null> => {
  try {
    const dbRow: Record<string, unknown> = { ...userToDbRow(user) };
    if (passwordHash) {
      dbRow.password_hash = passwordHash;
    }
    
    const { data, error } = await supabase
      .from('users')
      .insert([dbRow])
      .select()
      .single();

    if (error) {
      console.error('Error creating user:', error);
      return null;
    }

    return dbRowToUser(data);
  } catch (err) {
    console.error('Error in createUserInSupabase:', err);
    return null;
  }
};

export const updateUserInSupabase = async (user: User): Promise<boolean> => {
  try {
    const { error } = await supabase
      .from('users')
      .update({
        name: user.name,
        role: user.role,
        plan: user.plan,
        daily_limit: user.dailyLimit,
        records_extracted_today: user.recordsExtractedToday,
        last_active: user.lastActive,
        ip_address: user.ipAddress,
        is_online: user.isOnline,
        is_blocked: user.isBlocked || false
      })
      .eq('user_id', user.id);

    if (error) {
      console.error('Error updating user:', error);
      return false;
    }

    return true;
  } catch (err) {
    console.error('Error in updateUserInSupabase:', err);
    return false;
  }
};

export const deleteUserFromSupabase = async (userId: string): Promise<boolean> => {
  try {
    const { error } = await supabase
      .from('users')
      .delete()
      .eq('user_id', userId);

    if (error) {
      console.error('Error deleting user:', error);
      return false;
    }

    return true;
  } catch (err) {
    console.error('Error in deleteUserFromSupabase:', err);
    return false;
  }
};

export const fetchBlockedIPsFromSupabase = async (): Promise<BlockedIP[]> => {
  try {
    const { data, error } = await supabase
      .from('blocked_ips')
      .select('*')
      .order('blocked_at', { ascending: false });

    if (error) {
      console.error('Error fetching blocked IPs:', error);
      return [];
    }

    return (data || []).map(row => ({
      ip: row.ip_address,
      blockedAt: row.blocked_at,
      reason: row.reason || 'No reason provided'
    }));
  } catch (err) {
    console.error('Error in fetchBlockedIPsFromSupabase:', err);
    return [];
  }
};

export const blockIPInSupabase = async (ip: string, reason: string): Promise<boolean> => {
  try {
    const { error } = await supabase
      .from('blocked_ips')
      .insert([{
        ip_address: ip,
        reason: reason || 'No reason provided'
      }]);

    if (error) {
      console.error('Error blocking IP:', error);
      return false;
    }

    return true;
  } catch (err) {
    console.error('Error in blockIPInSupabase:', err);
    return false;
  }
};

export const unblockIPInSupabase = async (ip: string): Promise<boolean> => {
  try {
    const { error } = await supabase
      .from('blocked_ips')
      .delete()
      .eq('ip_address', ip);

    if (error) {
      console.error('Error unblocking IP:', error);
      return false;
    }

    return true;
  } catch (err) {
    console.error('Error in unblockIPInSupabase:', err);
    return false;
  }
};

export const verifyUserPassword = async (email: string, password: string): Promise<boolean> => {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('password_hash')
      .eq('email', email.toLowerCase())
      .single();

    if (error || !data || !data.password_hash) {
      return false;
    }

    return verifyPassword(password, data.password_hash);
  } catch (err) {
    console.error('Error in verifyUserPassword:', err);
    return false;
  }
};

export const isIPBlocked = async (ip: string): Promise<boolean> => {
  try {
    const { data, error } = await supabase
      .from('blocked_ips')
      .select('ip_address')
      .eq('ip_address', ip)
      .single();

    if (error || !data) {
      return false;
    }

    return true;
  } catch (err) {
    return false;
  }
};
