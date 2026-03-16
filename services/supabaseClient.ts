import { createClient } from '@supabase/supabase-js';
import { CarrierData } from '../types';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
  console.error('Missing Supabase environment variables');
  console.error('Required: VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY');
  throw new Error('Missing Supabase environment variables');
}

export const supabase = createClient(supabaseUrl, supabaseAnonKey);

export interface CarrierRecord {
  id?: string;
  mc_number: string;
  dot_number: string;
  legal_name: string;
  dba_name?: string;
  entity_type: string;
  status: string;
  email?: string;
  phone?: string;
  power_units?: string;
  drivers?: string;
  non_cmv_units?: string;
  physical_address?: string;
  mailing_address?: string;
  date_scraped: string;
  mcs150_date?: string;
  mcs150_mileage?: string;
  operation_classification?: string[];
  carrier_operation?: string[];
  cargo_carried?: string[];
  out_of_service_date?: string;
  state_carrier_id?: string;
  duns_number?: string;
  safety_rating?: string;
  safety_rating_date?: string;
  basic_scores?: { category: string; measure: string }[];
  oos_rates?: { type: string; rate: string; oosPercent?: string; nationalAvg: string }[];
  insurance_policies?: { dot: string; carrier: string; policyNumber: string; effectiveDate: string; coverageAmount: string; type: string; class: string }[];
  inspections?: { reportNumber: string; location: string; date: string; oosViolations: number; driverViolations: number; vehicleViolations: number; hazmatViolations: number; violationList: { label: string; description: string; weight: string }[] }[];
  crashes?: { date: string; number: string; state: string; plateNumber: string; plateState: string; fatal: string; injuries: string; towaway?: string }[];
  created_at?: string;
  updated_at?: string;
}


export const saveCarrierToSupabase = async (
  carrier: Record<string, unknown>
): Promise<{ success: boolean; error?: string; data?: unknown }> => {
  try {
    // Validate required fields
    if (!carrier.mcNumber || !carrier.dotNumber || !carrier.legalName) {
      return {
        success: false,
        error: 'Missing required fields: mcNumber, dotNumber, or legalName',
      };
    }

    const record: CarrierRecord = {
      mc_number: carrier.mcNumber as string,
      dot_number: carrier.dotNumber as string,
      legal_name: carrier.legalName as string,
      dba_name: (carrier.dbaName as string) || undefined,
      entity_type: carrier.entityType as string,
      status: carrier.status as string,
      email: (carrier.email as string) || undefined,
      phone: (carrier.phone as string) || undefined,
      power_units: (carrier.powerUnits as string) || undefined,
      drivers: (carrier.drivers as string) || undefined,
      non_cmv_units: (carrier.nonCmvUnits as string) || undefined,
      physical_address: (carrier.physicalAddress as string) || undefined,
      mailing_address: (carrier.mailingAddress as string) || undefined,
      date_scraped: carrier.dateScraped as string,
      mcs150_date: (carrier.mcs150Date as string) || undefined,
      mcs150_mileage: (carrier.mcs150Mileage as string) || undefined,
      operation_classification: (carrier.operationClassification as string[]) || [],
      carrier_operation: (carrier.carrierOperation as string[]) || [],
      cargo_carried: (carrier.cargoCarried as string[]) || [],
      out_of_service_date: (carrier.outOfServiceDate as string) || undefined,
      state_carrier_id: (carrier.stateCarrierId as string) || undefined,
      duns_number: (carrier.dunsNumber as string) || undefined,
      safety_rating: (carrier.safetyRating as string) || undefined,
      safety_rating_date: (carrier.safetyRatingDate as string) || undefined,
      basic_scores: (carrier.basicScores as CarrierRecord['basic_scores']) || undefined,
      oos_rates: (carrier.oosRates as CarrierRecord['oos_rates']) || undefined,
      insurance_policies: (carrier.insurancePolicies as CarrierRecord['insurance_policies']) || undefined,
      inspections: (carrier.inspections as CarrierRecord['inspections']) || undefined,
      crashes: (carrier.crashes as CarrierRecord['crashes']) || undefined,
    };

    const { data, error } = await supabase
      .from('carriers')
      .upsert(record, { onConflict: 'mc_number' });

    if (error) {
      console.error('Supabase save error:', error);
      return {
        success: false,
        error: `Database error: ${error.message}`,
      };
    }

    console.log('Carrier saved:', carrier.mcNumber);
    return { success: true, data };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('Exception saving to Supabase:', err);
    return {
      success: false,
      error: `Exception: ${message}`,
    };
  }
};

const BATCH_SIZE = 500;

export const saveCarriersToSupabase = async (
  carriers: Record<string, unknown>[]
): Promise<{ success: boolean; error?: string; saved: number; failed: number }> => {
  let saved = 0;
  let failed = 0;

  const toRecord = (carrier: Record<string, unknown>): CarrierRecord => ({
    mc_number: carrier.mcNumber as string,
    dot_number: carrier.dotNumber as string,
    legal_name: carrier.legalName as string,
    dba_name: (carrier.dbaName as string) || undefined,
    entity_type: carrier.entityType as string,
    status: carrier.status as string,
    email: (carrier.email as string) || undefined,
    phone: (carrier.phone as string) || undefined,
    power_units: (carrier.powerUnits as string) || undefined,
    drivers: (carrier.drivers as string) || undefined,
    non_cmv_units: (carrier.nonCmvUnits as string) || undefined,
    physical_address: (carrier.physicalAddress as string) || undefined,
    mailing_address: (carrier.mailingAddress as string) || undefined,
    date_scraped: carrier.dateScraped as string,
    mcs150_date: (carrier.mcs150Date as string) || undefined,
    mcs150_mileage: (carrier.mcs150Mileage as string) || undefined,
    operation_classification: (carrier.operationClassification as string[]) || [],
    carrier_operation: (carrier.carrierOperation as string[]) || [],
    cargo_carried: (carrier.cargoCarried as string[]) || [],
    out_of_service_date: (carrier.outOfServiceDate as string) || undefined,
    state_carrier_id: (carrier.stateCarrierId as string) || undefined,
    duns_number: (carrier.dunsNumber as string) || undefined,
    safety_rating: (carrier.safetyRating as string) || undefined,
    safety_rating_date: (carrier.safetyRatingDate as string) || undefined,
    basic_scores: (carrier.basicScores as CarrierRecord['basic_scores']) || undefined,
    oos_rates: (carrier.oosRates as CarrierRecord['oos_rates']) || undefined,
    insurance_policies: (carrier.insurancePolicies as CarrierRecord['insurance_policies']) || undefined,
    inspections: (carrier.inspections as CarrierRecord['inspections']) || undefined,
    crashes: (carrier.crashes as CarrierRecord['crashes']) || undefined,
  });

  const valid = carriers.filter(c => c.mcNumber && c.dotNumber && c.legalName);
  const invalid = carriers.length - valid.length;
  failed += invalid;

  for (let i = 0; i < valid.length; i += BATCH_SIZE) {
    const batch = valid.slice(i, i + BATCH_SIZE).map(toRecord);
    try {
      const { error } = await supabase
        .from('carriers')
        .upsert(batch, { onConflict: 'mc_number' });
      if (error) {
        console.error('Batch upsert error:', error);
        failed += batch.length;
      } else {
        saved += batch.length;
      }
    } catch (err) {
      console.error('Batch upsert exception:', err);
      failed += batch.length;
    }
  }

  return {
    success: failed === 0,
    saved,
    failed,
    error: failed > 0 ? `${failed} carriers failed to save` : undefined,
  };
};

export interface CarrierFilters {
  // Motor Carrier
  mcNumber?: string;
  dotNumber?: string;
  legalName?: string;
  active?: string;           // 'true' | 'false' | ''
  state?: string;
  hasEmail?: string;         // 'true' | 'false' | ''
  hasBoc3?: string;          // 'true' | 'false' | ''
  hasCompanyRep?: string;    // 'true' | 'false' | ''
  yearsInBusinessMin?: number;
  yearsInBusinessMax?: number;
  // Carrier Operation
  classification?: string[];
  carrierOperation?: string[];
  hazmat?: string;           // 'true' | 'false' | ''
  powerUnitsMin?: number;
  powerUnitsMax?: number;
  driversMin?: number;
  driversMax?: number;
  cargo?: string[];
  // Insurance Policy
  insuranceRequired?: string[];
  bipdMin?: number;
  bipdMax?: number;
  bipdOnFile?: string;       // '1' | '0' | ''
  cargoOnFile?: string;      // '1' | '0' | ''
  bondOnFile?: string;       // '1' | '0' | ''
  // Safety
  oosMin?: number;
  oosMax?: number;
  crashesMin?: number;
  crashesMax?: number;
  injuriesMin?: number;
  injuriesMax?: number;
  fatalitiesMin?: number;
  fatalitiesMax?: number;
  towawayMin?: number;
  towawayMax?: number;
  inspectionsMin?: number;
  inspectionsMax?: number;
  // Pagination
  limit?: number;
}

export const fetchCarriersFromSupabase = async (filters: CarrierFilters = {}): Promise<CarrierData[]> => {
  try {
    let query = supabase
      .from('carriers')
      .select('*');

    const isFiltered = Object.keys(filters).some(k => {
      const key = k as keyof CarrierFilters;
      const val = filters[key];
      if (key === 'limit') return false;
      if (Array.isArray(val)) return val.length > 0;
      return val !== undefined && val !== '';
    });

    if (filters.mcNumber) {
      query = query.ilike('mc_number', `%${filters.mcNumber}%`);
    }
    if (filters.dotNumber) {
      query = query.ilike('dot_number', `%${filters.dotNumber}%`);
    }
    if (filters.legalName) {
      query = query.ilike('legal_name', `%${filters.legalName}%`);
    }
    if (filters.active === 'true') {
      query = query.ilike('status', '%AUTHORIZED%').not('status', 'ilike', '%NOT%');
    } else if (filters.active === 'false') {
      query = query.or('status.ilike.%NOT AUTHORIZED%,status.not.ilike.%AUTHORIZED%');
    }
    if (filters.state) {
      // Correct syntax for OR with ILIKE in PostgREST when using special characters like commas:
      // We must wrap the pattern in double quotes.
      const states = filters.state.split('|');
      const stateOrConditions = states.map(s => `physical_address.ilike."%, ${s}%"`).join(',');
      query = query.or(stateOrConditions);
    }
    if (filters.hasEmail === 'true') {
      query = query.not('email', 'is', null).neq('email', '');
    } else if (filters.hasEmail === 'false') {
      query = query.or('email.is.null,email.eq.');
    }
    if (filters.hasBoc3 === 'true') {
      query = query.contains('carrier_operation', ['BOC-3']);
    } else if (filters.hasBoc3 === 'false') {
      query = query.not('carrier_operation', 'cs', '{"BOC-3"}');
    }

    if (filters.classification && filters.classification.length > 0) {
      query = query.overlaps('operation_classification', filters.classification);
    }
    if (filters.carrierOperation && filters.carrierOperation.length > 0) {
      query = query.overlaps('carrier_operation', filters.carrierOperation);
    }
    if (filters.cargo && filters.cargo.length > 0) {
      query = query.overlaps('cargo_carried', filters.cargo);
    }
    if (filters.hazmat === 'true') {
      query = query.contains('cargo_carried', ['Hazardous Materials']);
    } else if (filters.hazmat === 'false') {
      query = query.not('cargo_carried', 'cs', '{"Hazardous Materials"}');
    }
    if (filters.powerUnitsMin !== undefined) {
      query = query.gte('power_units', filters.powerUnitsMin.toString());
    }
    if (filters.powerUnitsMax !== undefined) {
      query = query.lte('power_units', filters.powerUnitsMax.toString());
    }
    if (filters.driversMin !== undefined) {
      query = query.gte('drivers', filters.driversMin.toString());
    }
    if (filters.driversMax !== undefined) {
      query = query.lte('drivers', filters.driversMax.toString());
    }

    if (filters.insuranceRequired && filters.insuranceRequired.length > 0) {
      // Filter by insurance type in the insurance_policies JSONB array
      const insuranceOrConditions = filters.insuranceRequired.map(type => `insurance_policies.cs.[{"type": "${type}"}]`).join(',');
      query = query.or(insuranceOrConditions);
    }
    if (filters.bipdOnFile === '1') {
      query = query.contains('insurance_policies', [{ type: 'BI&PD' }]);
    }
    if (filters.cargoOnFile === '1') {
      query = query.contains('insurance_policies', [{ type: 'CARGO' }]);
    }
    if (filters.bondOnFile === '1') {
      query = query.contains('insurance_policies', [{ type: 'BOND' }]);
    }

    query = query.order('created_at', { ascending: false });

    const MAX_QUERY_LIMIT = 5000;
    if (!isFiltered) {
      query = query.limit(200);
    } else if (filters.limit) {
      query = query.limit(Math.min(filters.limit, MAX_QUERY_LIMIT));
    } else {
      query = query.limit(MAX_QUERY_LIMIT);
    }

    const { data, error } = await query;

    if (error) {
      console.error('Supabase fetch error:', error);
      return [];
    }

    let results = (data || []).map((record: CarrierRecord) => ({
      mcNumber: record.mc_number,
      dotNumber: record.dot_number,
      legalName: record.legal_name,
      dbaName: record.dba_name,
      entityType: record.entity_type,
      status: record.status,
      email: record.email,
      phone: record.phone,
      powerUnits: record.power_units,
      drivers: record.drivers,
      non_cmv_units: record.non_cmv_units,
      physicalAddress: record.physical_address,
      mailingAddress: record.mailing_address,
      dateScraped: record.date_scraped,
      mcs150Date: record.mcs150_date,
      mcs150Mileage: record.mcs150_mileage,
      operationClassification: record.operation_classification || [],
      carrierOperation: record.carrier_operation || [],
      cargoCarried: record.cargo_carried || [],
      outOfServiceDate: record.out_of_service_date,
      stateCarrierId: record.state_carrier_id,
      dunsNumber: record.duns_number,
      safetyRating: record.safety_rating,
      safetyRatingDate: record.safety_rating_date,
      basicScores: record.basic_scores,
      oosRates: record.oos_rates,
      insurancePolicies: record.insurance_policies,
      inspections: record.inspections,
      crashes: record.crashes,
    }));

    // Post-fetch filtering for Years in Business (since mcs150_date is a string in various formats)
    if (filters.yearsInBusinessMin !== undefined || filters.yearsInBusinessMax !== undefined) {
      results = results.filter(carrier => {
        if (!carrier.mcs150Date || carrier.mcs150Date === 'N/A') return false;
        try {
          const date = new Date(carrier.mcs150Date);
          if (isNaN(date.getTime())) return false;
          const diffMs = Date.now() - date.getTime();
          const ageDate = new Date(diffMs);
          const years = Math.abs(ageDate.getUTCFullYear() - 1970);
          
          if (filters.yearsInBusinessMin !== undefined && years < filters.yearsInBusinessMin) return false;
          if (filters.yearsInBusinessMax !== undefined && years > filters.yearsInBusinessMax) return false;
          return true;
        } catch (e) {
          return false;
        }
      });
    }

    // Post-fetch filtering for Safety Metrics (OOS Violations, Crashes, Injuries, Inspections)
    // These are stored in JSONB columns and need to be filtered client-side
    if (
      filters.oosMin !== undefined || filters.oosMax !== undefined ||
      filters.crashesMin !== undefined || filters.crashesMax !== undefined ||
      filters.injuriesMin !== undefined || filters.injuriesMax !== undefined ||
      filters.fatalitiesMin !== undefined || filters.fatalitiesMax !== undefined ||
      filters.towawayMin !== undefined || filters.towawayMax !== undefined ||
      filters.inspectionsMin !== undefined || filters.inspectionsMax !== undefined
    ) {
      results = results.filter(carrier => {
        // Count OOS Violations from inspections
        if (filters.oosMin !== undefined || filters.oosMax !== undefined) {
          let oosCount = 0;
          if (carrier.inspections && Array.isArray(carrier.inspections)) {
            oosCount = carrier.inspections.reduce((sum, inspection) => sum + (inspection.oosViolations || 0), 0);
          }
          if (filters.oosMin !== undefined && oosCount < filters.oosMin) return false;
          if (filters.oosMax !== undefined && oosCount > filters.oosMax) return false;
        }

        // Count total crashes
        if (filters.crashesMin !== undefined || filters.crashesMax !== undefined) {
          const crashCount = (carrier.crashes && Array.isArray(carrier.crashes)) ? carrier.crashes.length : 0;
          if (filters.crashesMin !== undefined && crashCount < filters.crashesMin) return false;
          if (filters.crashesMax !== undefined && crashCount > filters.crashesMax) return false;
        }

        // Count total injuries from crashes
        if (filters.injuriesMin !== undefined || filters.injuriesMax !== undefined) {
          let injuryCount = 0;
          if (carrier.crashes && Array.isArray(carrier.crashes)) {
            injuryCount = carrier.crashes.reduce((sum, crash) => {
              const injuries = parseInt(crash.injuries || '0');
              return sum + (isNaN(injuries) ? 0 : injuries);
            }, 0);
          }
          if (filters.injuriesMin !== undefined && injuryCount < filters.injuriesMin) return false;
          if (filters.injuriesMax !== undefined && injuryCount > filters.injuriesMax) return false;
        }

        // Count total fatalities from crashes
        if (filters.fatalitiesMin !== undefined || filters.fatalitiesMax !== undefined) {
          let fatalityCount = 0;
          if (carrier.crashes && Array.isArray(carrier.crashes)) {
            fatalityCount = carrier.crashes.reduce((sum, crash) => {
              const fatals = parseInt(crash.fatal || '0');
              return sum + (isNaN(fatals) ? 0 : fatals);
            }, 0);
          }
          if (filters.fatalitiesMin !== undefined && fatalityCount < filters.fatalitiesMin) return false;
          if (filters.fatalitiesMax !== undefined && fatalityCount > filters.fatalitiesMax) return false;
        }

        if (filters.towawayMin !== undefined || filters.towawayMax !== undefined) {
          let towawayCount = 0;
          if (carrier.crashes && Array.isArray(carrier.crashes)) {
            towawayCount = carrier.crashes.reduce((sum: number, crash: { towaway?: string }) => {
              return sum + (crash.towaway === 'Yes' || crash.towaway === 'Y' ? 1 : 0);
            }, 0);
          }
          if (filters.towawayMin !== undefined && towawayCount < filters.towawayMin) return false;
          if (filters.towawayMax !== undefined && towawayCount > filters.towawayMax) return false;
        }

        // Count total inspections
        if (filters.inspectionsMin !== undefined || filters.inspectionsMax !== undefined) {
          const inspectionCount = (carrier.inspections && Array.isArray(carrier.inspections)) ? carrier.inspections.length : 0;
          if (filters.inspectionsMin !== undefined && inspectionCount < filters.inspectionsMin) return false;
          if (filters.inspectionsMax !== undefined && inspectionCount > filters.inspectionsMax) return false;
        }

        return true;
      });
    }

    return results;
  } catch (err) {
    console.error('Exception fetching from Supabase:', err);
    return [];
  }
};

/**
 * Delete carrier by MC number
 */
export const deleteCarrier = async (
  mcNumber: string
): Promise<{ success: boolean; error?: string }> => {
  try {
    const { error } = await supabase
      .from('carriers')
      .delete()
      .eq('mc_number', mcNumber);

    if (error) {
      console.error('Supabase delete error:', error);
      return { success: false, error: error.message };
    }

    console.log('Carrier deleted:', mcNumber);
    return { success: true };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('Exception deleting carrier:', err);
    return { success: false, error: message };
  }
};

/**
 * Get carrier count
 */
export const getCarrierCount = async (): Promise<number> => {
  try {
    const { count, error } = await supabase
      .from('carriers')
      .select('*', { count: 'exact', head: true });

    if (error) {
      console.error('Error getting carrier count:', error);
      return 0;
    }

    return count || 0;
  } catch (err) {
    console.error('Exception getting carrier count:', err);
    return 0;
  }
};

export const updateCarrierInsurance = async (dotNumber: string, insuranceData: { policies: CarrierRecord['insurance_policies'] }): Promise<{ success: boolean; error?: string }> => {
  try {
    const { error } = await supabase
      .from('carriers')
      .update({
        insurance_policies: insuranceData.policies,
        updated_at: new Date().toISOString(),
      })
      .eq('dot_number', dotNumber);

    if (error) {
      console.error('Supabase update error:', error);
      return { success: false, error: error.message };
    }

    console.log('Insurance data updated for DOT:', dotNumber);
    return { success: true };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('Exception updating Supabase:', err);
    return { success: false, error: message };
  }
};

export const updateCarrierSafety = async (dotNumber: string, safetyData: { rating?: string; ratingDate?: string; basicScores?: CarrierRecord['basic_scores']; oosRates?: CarrierRecord['oos_rates'] }): Promise<{ success: boolean; error?: string }> => {
  try {
    const { error } = await supabase
      .from('carriers')
      .update({
        safety_rating: safetyData.rating,
        safety_rating_date: safetyData.ratingDate,
        basic_scores: safetyData.basicScores,
        oos_rates: safetyData.oosRates,
        updated_at: new Date().toISOString(),
      })
      .eq('dot_number', dotNumber);

    if (error) {
      console.error('Supabase safety update error:', error);
      return { success: false, error: error.message };
    }

    console.log('Safety data updated for DOT:', dotNumber);
    return { success: true };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('Exception updating safety data:', err);
    return { success: false, error: message };
  }
};


/**
 * Fetches carriers within a specific MC Number range
 */
interface CarrierMCRangeResult {
  mcNumber: string;
  dotNumber: string;
  legalName: string;
  insurancePolicies: CarrierRecord['insurance_policies'];
}

export const getCarriersByMCRange = async (start: string, end: string): Promise<CarrierMCRangeResult[]> => {
  try {
    const { data, error } = await supabase
      .from('carriers')
      .select('*')
      .gte('mc_number', start)
      .lte('mc_number', end)
      .order('mc_number', { ascending: true });

    if (error) throw error;

    return (data || []).map(record => ({
      mcNumber: record.mc_number,
      dotNumber: record.dot_number,
      legalName: record.legal_name,
      insurancePolicies: record.insurance_policies || []
    }));
  } catch (err) {
    console.error('Error fetching MC range:', err);
    return [];
  }
};
