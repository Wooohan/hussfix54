import React from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { Users, Database, Activity, TrendingUp } from 'lucide-react';
import { CarrierData } from '../types';

interface DashboardProps {
  carriers: CarrierData[];
  isLoading: boolean;
}

export const Dashboard: React.FC<DashboardProps> = ({ carriers, isLoading }) => {
  const totalScraped = carriers.length;
  const activeCarriers = carriers.filter(c =>
    c.status?.toUpperCase().includes('AUTHORIZED') && !c.status?.toUpperCase().includes('NOT AUTHORIZED')
  ).length;
  const brokers = carriers.filter(c => c.entityType?.toUpperCase().includes('BROKER')).length;
  const withEmail = carriers.filter(c => c.email && c.email.length > 0).length;
  const emailRate = totalScraped > 0 ? ((withEmail / totalScraped) * 100).toFixed(1) : '0';

  const notAuthorized = carriers.filter(c =>
    c.status?.toUpperCase().includes('NOT AUTHORIZED')
  ).length;
  const other = totalScraped - activeCarriers - notAuthorized;

  const entityData = [
    { name: 'Authorized', value: activeCarriers, color: '#4ade80' },
    { name: 'Not Auth', value: notAuthorized, color: '#f87171' },
    { name: 'Other', value: other, color: '#facc15' },
  ];

  return (
    <div className="p-8 space-y-8 animate-fade-in">
      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Dashboard Overview</h1>
          <p className="text-slate-400">
            {isLoading ? 'Loading carrier data...' : `${totalScraped.toLocaleString()} carriers loaded from database`}
          </p>
        </div>
        <div className="flex items-center gap-2 text-sm text-slate-400 bg-slate-800/50 px-3 py-1 rounded-full border border-slate-700">
          <span className={`w-2 h-2 rounded-full ${isLoading ? 'bg-yellow-500' : 'bg-green-500'} animate-pulse`}></span>
          {isLoading ? 'Loading...' : 'System Operational'}
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {[
          { label: 'Total in DB', value: totalScraped.toLocaleString(), icon: Database, color: 'text-blue-400' },
          { label: 'Active Carriers', value: activeCarriers.toLocaleString(), icon: Users, color: 'text-green-400' },
          { label: 'Brokers', value: brokers.toLocaleString(), icon: Activity, color: 'text-purple-400' },
          { label: 'Email Rate', value: `${emailRate}%`, icon: TrendingUp, color: 'text-indigo-400' },
        ].map((stat, idx) => (
          <div key={idx} className="bg-slate-800/50 border border-slate-700 p-6 rounded-2xl hover:bg-slate-800 transition-all group">
            <div className="flex justify-between items-start mb-4">
              <div className={`p-3 rounded-xl bg-slate-900/50 ${stat.color} group-hover:scale-110 transition-transform`}>
                <stat.icon size={24} />
              </div>
            </div>
            <h3 className="text-slate-400 text-sm font-medium">{stat.label}</h3>
            <p className="text-2xl font-bold text-white mt-1">{stat.value}</p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-slate-800/50 border border-slate-700 p-6 rounded-2xl">
          <h3 className="text-lg font-bold text-white mb-6">Authority Status Breakdown</h3>
          {totalScraped === 0 ? (
            <div className="h-[300px] flex items-center justify-center text-slate-500">
              No carrier data available. Run the scraper to populate.
            </div>
          ) : (
            <div className="h-[300px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={entityData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                  <XAxis dataKey="name" stroke="#94a3b8" />
                  <YAxis stroke="#94a3b8" />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155', color: '#f1f5f9' }}
                    itemStyle={{ color: '#f1f5f9' }}
                  />
                  <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                    {entityData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>

        <div className="bg-slate-800/50 border border-slate-700 p-6 rounded-2xl">
          <h3 className="text-lg font-bold text-white mb-6">Quick Stats</h3>
          <div className="space-y-4">
            <div className="flex justify-between items-center p-3 bg-slate-900/50 rounded-xl">
              <span className="text-slate-400 text-sm">With Safety Rating</span>
              <span className="text-white font-bold">{carriers.filter(c => c.safetyRating).length}</span>
            </div>
            <div className="flex justify-between items-center p-3 bg-slate-900/50 rounded-xl">
              <span className="text-slate-400 text-sm">With Insurance</span>
              <span className="text-white font-bold">{carriers.filter(c => c.insurancePolicies && c.insurancePolicies.length > 0).length}</span>
            </div>
            <div className="flex justify-between items-center p-3 bg-slate-900/50 rounded-xl">
              <span className="text-slate-400 text-sm">With Inspections</span>
              <span className="text-white font-bold">{carriers.filter(c => c.inspections && c.inspections.length > 0).length}</span>
            </div>
            <div className="flex justify-between items-center p-3 bg-slate-900/50 rounded-xl">
              <span className="text-slate-400 text-sm">With Crashes</span>
              <span className="text-white font-bold">{carriers.filter(c => c.crashes && c.crashes.length > 0).length}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
