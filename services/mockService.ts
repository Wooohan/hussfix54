import { CarrierData, InsurancePolicy, BasicScore, OosRate } from '../types';
const BASE_URL = '';
interface InsuranceRawData {
  name_company?: string;
  insurance_company?: string;
  insurance_company_name?: string;
  company_name?: string;
  policy_no?: string;
  policy_number?: string;
  pol_num?: string;
  effective_date?: string;
  max_cov_amount?: string;
  coverage_to?: string;
  coverage_amount?: string;
  ins_type_code?: string;
  ins_class_code?: string;
  data?: InsuranceRawData[];
}
export const fetchInsuranceData = async (dot: string): Promise<{
  policies: InsurancePolicy[];
  raw: InsuranceRawData | InsuranceRawData[] | null;
}> => {
  if (!dot) return { policies: [], raw: null };
  const targetUrl = `https://searchcarriers.com/company/${dot}/insurances`;
  const proxyUrl = `${BASE_URL}/api/proxy?url=${encodeURIComponent(targetUrl)}`;
  let result: InsuranceRawData | InsuranceRawData[] | null = null;
  try {
    const res = await fetch(proxyUrl, {
      signal: AbortSignal.timeout(10000)
    });
    if (res.ok) {
      result = await res.json();
    }
  } catch (e) {
    console.error('Insurance fetch error:', e);
    return { policies: [], raw: null };
  }
  const resultObj = result as InsuranceRawData & { data?: InsuranceRawData[] };
  const rawData: InsuranceRawData[] = resultObj?.data || (Array.isArray(result) ? result as InsuranceRawData[] : []);
  const policies: InsurancePolicy[] = [];
  if (Array.isArray(rawData)) {
    rawData.forEach((p: InsuranceRawData) => {
      const carrier = (
        p.name_company || p.insurance_company || p.insurance_company_name || p.company_name || 'NOT SPECIFIED'
      ).toString().toUpperCase();
      const policyNumber = (p.policy_no || p.policy_number || p.pol_num || 'N/A').toString().toUpperCase();
      const effectiveDate = p.effective_date ? p.effective_date.split(' ')[0] : 'N/A';
      let coverage = p.max_cov_amount || p.coverage_to || p.coverage_amount || 'N/A';
      if (coverage !== 'N/A' && !isNaN(Number(coverage))) {
        const num = Number(coverage);
        coverage = num < 10000 && num > 0
          ? `$${(num * 1000).toLocaleString()}`
          : `$${num.toLocaleString()}`;
      }
      let type = (p.ins_type_code || 'N/A').toString();
      if (type === '1') type = 'BI&PD';
      else if (type === '2') type = 'CARGO';
      else if (type === '3') type = 'BOND';
      let iClass = (p.ins_class_code || 'N/A').toString().toUpperCase();
      if (iClass === 'P') iClass = 'PRIMARY';
      else if (iClass === 'E') iClass = 'EXCESS';
      policies.push({ dot, carrier, policyNumber, effectiveDate, coverageAmount: coverage, type, class: iClass });
    });
  }
  return { policies, raw: result };
};
export const downloadCSV = (data: CarrierData[]) => {
  const headers = [
    'Date', 'MC', 'Email', 'Entity Type', 'Operating Authority Status', 'Out of Service Date',
    'Legal_Name', 'DBA Name', 'Physical Address', 'Phone', 'Mailing Address', 'USDOT Number',
    'State Carrier ID Number', 'Power Units', 'Drivers', 'DUNS Number',
    'MCS-150 Form Date', 'MCS-150 Mileage (Year)', 'Operation Classification',
    'Carrier Operation', 'Cargo Carried', 'Safety Rating', 'Rating Date',
    'BASIC Scores', 'OOS Rates', 'Inspections'
  ];
  const esc = (val: string | number | undefined) => {
    if (!val) return '""';
    return `"${String(val).replace(/"/g, '""')}"`;
  };
  const csvRows = data.map(row => [
    esc(row.dateScraped), row.mcNumber, esc(row.email),
    esc(row.entityType), esc(row.status), esc(row.outOfServiceDate),
    esc(row.legalName), esc(row.dbaName), esc(row.physicalAddress),
    esc(row.phone), esc(row.mailingAddress), esc(row.dotNumber),
    esc(row.stateCarrierId), esc(row.powerUnits), esc(row.drivers),
    esc(row.dunsNumber), esc(row.mcs150Date), esc(row.mcs150Mileage),
    esc(row.operationClassification.join(', ')),
    esc(row.carrierOperation.join(', ')),
    esc(row.cargoCarried.join(', ')),
    esc(row.safetyRating), esc(row.safetyRatingDate),
    esc(row.basicScores?.map((s: BasicScore) => `${s.category}: ${s.measure}`).join(' | ')),
    esc(row.oosRates?.map((r: OosRate) => `${r.type}: ${r.rate} (Avg: ${r.nationalAvg})`).join(' | ')),
    esc(row.inspections?.map((i: { reportNumber: string; oosViolations: number; driverViolations: number; vehicleViolations: number; hazmatViolations: number }) => `Report ${i.reportNumber}: ${i.oosViolations} OOS, ${i.driverViolations} Driver, ${i.vehicleViolations} Vehicle, ${i.hazmatViolations} Hazmat`).join(' | '))
  ]);
  const csv = [headers.join(','), ...csvRows.map(r => r.join(','))].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = `fmcsa_export_${new Date().toISOString().slice(0, 10)}.csv`;
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};
