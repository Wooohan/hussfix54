import { supabase } from './supabaseClient';

export interface FMCSARegisterEntry {
  id?: string;
  number: string;
  title: string;
  decided: string;
  category: string;
  date_fetched: string;
  extracted_date?: string;
  created_at?: string;
  updated_at?: string;
}


type QueryBuilder = ReturnType<ReturnType<typeof supabase.from>['select']>;

const fetchAllPages = async (buildQuery: () => QueryBuilder): Promise<Record<string, unknown>[]> => {
  let allData: Record<string, unknown>[] = [];
  let keepFetching = true;
  let from = 0;
  const step = 1000;

  while (keepFetching) {
    const { data, error } = await buildQuery().range(from, from + step - 1);

    if (error) {
      console.error('Batch fetch error:', error);
      throw error;
    }

    if (data && data.length > 0) {
      allData = [...allData, ...(data as Record<string, unknown>[])];
      if (data.length < step) {
        keepFetching = false;
      } else {
        from += step;
      }
    } else {
      keepFetching = false;
    }
  }
  return allData;
};


export const saveFMCSARegisterEntries = async (
  entries: FMCSARegisterEntry[],
  fetchDate: string,
  extractedDate?: string
): Promise<{ success: boolean; error?: string; count?: number }> => {
  try {
    if (!entries || entries.length === 0) {
      console.log('No entries to save');
      return { success: true, count: 0 };
    }

    const dateToUse = extractedDate || fetchDate;

    const records = entries.map(entry => ({
      number: entry.number,
      title: entry.title,
      decided: entry.decided || 'N/A',
      category: entry.category || 'MISCELLANEOUS',
      date_fetched: fetchDate,
      extracted_date: dateToUse,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }));

    console.log(`Saving ${records.length} entries for date: ${dateToUse}`);

    const { error } = await supabase
      .from('fmcsa_register')
      .upsert(records, { onConflict: 'number,extracted_date' });

    if (error) {
      console.error('Supabase save error:', error);
      return { success: false, error: `Database error: ${error.message}` };
    }

    return { success: true, count: records.length };
  } catch (err: any) {
    console.error('Exception saving FMCSA entries:', err);
    return { success: false, error: `Exception: ${err.message}` };
  }
};


export const fetchFMCSARegisterByExtractedDate = async (
  extractedDate: string,
  filters?: {
    category?: string;
    searchTerm?: string;
  }
): Promise<FMCSARegisterEntry[]> => {
  try {
    const buildQuery = () => {
      let q = supabase.from('fmcsa_register').select('*').eq('extracted_date', extractedDate);
      if (filters?.category && filters.category !== 'all') {
        q = q.eq('category', filters.category);
      }
      if (filters?.searchTerm) {
        const searchPattern = `%${filters.searchTerm}%`;
        q = q.or(`number.ilike.${searchPattern},title.ilike.${searchPattern}`);
      }
      return q.order('number', { ascending: true });
    };

    const data = await fetchAllPages(buildQuery);
    console.log(`Retrieved ${data.length} records for ${extractedDate}`);
    return data as unknown as FMCSARegisterEntry[];
  } catch (err) {
    console.error('Exception fetching from Supabase:', err);
    return [];
  }
};


export const fetchFMCSARegisterEntries = async (filters?: {
  category?: string;
  dateFrom?: string;
  dateTo?: string;
  searchTerm?: string;
  limit?: number;
}): Promise<FMCSARegisterEntry[]> => {
  try {
    if (filters?.limit) {
      let query = supabase.from('fmcsa_register').select('*');
      if (filters?.category && filters.category !== 'all') query = query.eq('category', filters.category);
      if (filters?.dateFrom) query = query.gte('extracted_date', filters.dateFrom);
      if (filters?.dateTo) query = query.lte('extracted_date', filters.dateTo);
      if (filters?.searchTerm) {
        const searchPattern = `%${filters.searchTerm}%`;
        query = query.or(`number.ilike.${searchPattern},title.ilike.${searchPattern}`);
      }
      query = query.order('extracted_date', { ascending: false }).order('number', { ascending: true });
      const { data } = await query.limit(filters.limit);
      return (data || []) as FMCSARegisterEntry[];
    }

    const buildQuery = () => {
      let q = supabase.from('fmcsa_register').select('*');
      if (filters?.category && filters.category !== 'all') q = q.eq('category', filters.category);
      if (filters?.dateFrom) q = q.gte('extracted_date', filters.dateFrom);
      if (filters?.dateTo) q = q.lte('extracted_date', filters.dateTo);
      if (filters?.searchTerm) {
        const searchPattern = `%${filters.searchTerm}%`;
        q = q.or(`number.ilike.${searchPattern},title.ilike.${searchPattern}`);
      }
      return q.order('extracted_date', { ascending: false }).order('number', { ascending: true });
    };

    const data = await fetchAllPages(buildQuery);
    return data as unknown as FMCSARegisterEntry[];
  } catch (err) {
    console.error('Exception fetching entries:', err);
    return [];
  }
};


export const getFMCSAEntriesByDate = async (date: string): Promise<FMCSARegisterEntry[]> => {
  try {
    const buildQuery = () => supabase.from('fmcsa_register').select('*').eq('extracted_date', date).order('number', { ascending: true });
    const data = await fetchAllPages(buildQuery);
    return data as unknown as FMCSARegisterEntry[];
  } catch (err) {
    console.error('Exception fetching by date:', err);
    return [];
  }
};

export const getFMCSACategories = async (): Promise<string[]> => {
  try {
    const { data, error } = await supabase.rpc('get_distinct_fmcsa_categories').select('category');
    if (error || !data) {
      const buildQuery = () => supabase.from('fmcsa_register').select('category').neq('category', null);
      const allData = await fetchAllPages(buildQuery);
      const categories = new Set<string>();
      allData.forEach((record) => {
        if (record.category) categories.add(record.category as string);
      });
      return Array.from(categories).sort();
    }
    return (data as { category: string }[]).map(r => r.category).sort();
  } catch (err) {
    console.error('Exception fetching categories:', err);
    return [];
  }
};


export const getFMCSAStatistics = async (
  dateFrom?: string,
  dateTo?: string
): Promise<{
  totalEntries: number;
  byCategory: Record<string, number>;
  dateRange: { from: string; to: string };
}> => {
  try {
    const buildQuery = () => {
      let q = supabase.from('fmcsa_register').select('*');
      if (dateFrom) q = q.gte('extracted_date', dateFrom);
      if (dateTo) q = q.lte('extracted_date', dateTo);
      return q;
    };

    const data = await fetchAllPages(buildQuery);

    const byCategory: Record<string, number> = {};
    data.forEach((entry) => {
      const cat = (entry.category as string) || 'UNCATEGORIZED';
      byCategory[cat] = (byCategory[cat] || 0) + 1;
    });

    return {
      totalEntries: data.length,
      byCategory,
      dateRange: { from: dateFrom || '', to: dateTo || '' },
    };
  } catch (err) {
    console.error('Exception fetching statistics:', err);
    return { totalEntries: 0, byCategory: {}, dateRange: { from: dateFrom || '', to: dateTo || '' } };
  }
};


export const getExtractedDates = async (): Promise<string[]> => {
  try {
    const { data, error } = await supabase.rpc('get_distinct_extracted_dates').select('extracted_date');
    if (error || !data) {
      const buildQuery = () => supabase.from('fmcsa_register').select('extracted_date').neq('extracted_date', null).order('extracted_date', { ascending: false });
      const allData = await fetchAllPages(buildQuery);
      const dates = new Set<string>();
      allData.forEach((record) => {
        if (record.extracted_date) dates.add(record.extracted_date as string);
      });
      return Array.from(dates).sort().reverse();
    }
    return (data as { extracted_date: string }[]).map(r => r.extracted_date).sort().reverse();
  } catch (err) {
    console.error('Exception fetching dates:', err);
    return [];
  }
};


export const deleteFMCSAEntriesBeforeDate = async (date: string): Promise<{ success: boolean; error?: string; deleted?: number }> => {
  try {
    // Delete doesn't return data by default in newer Supabase versions without .select()
    const { error, count } = await supabase
      .from('fmcsa_register')
      .delete({ count: 'exact' })
      .lt('extracted_date', date);

    if (error) throw error;

    return { success: true, deleted: count || 0 };
  } catch (err: any) {
    console.error('Exception deleting entries:', err);
    return { success: false, error: err.message };
  }
};


export const checkFMCSARegisterTable = async (): Promise<{
  exists: boolean;
  accessible: boolean;
  error?: string;
}> => {
  try {
    const { error } = await supabase
      .from('fmcsa_register')
      .select('*', { count: 'exact', head: true });

    if (error) {
      return {
        exists: !error.message.includes('does not exist'),
        accessible: false,
        error: error.message,
      };
    }

    return { exists: true, accessible: true };
  } catch (err: any) {
    return { exists: false, accessible: false, error: err.message };
  }
};
