import { useFieldConfigs, useSourcePriorities } from '../../hooks/useMdm';
import type { FieldConfig, SourcePriorityConfig } from '../../types/mdm.types';
import { Settings, AlertCircle } from 'lucide-react';
import { cn } from '../../lib/utils';

function Badge({ children, color = 'gray' }: { children: React.ReactNode; color?: 'green' | 'blue' | 'gray' | 'amber' }) {
  const colors = {
    green: 'bg-green-100 text-green-700',
    blue:  'bg-blue-100 text-blue-700',
    amber: 'bg-amber-100 text-amber-700',
    gray:  'bg-gray-100 text-gray-500',
  };
  return (
    <span className={cn('text-xs font-medium px-2 py-0.5 rounded-full', colors[color])}>
      {children}
    </span>
  );
}

function SectionCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden mb-6">
      <div className="px-5 py-3 border-b border-gray-50 bg-gray-50/60">
        <h2 className="text-sm font-semibold text-gray-700">{title}</h2>
      </div>
      {children}
    </div>
  );
}

function FieldConfigTable({ configs }: { configs: FieldConfig[] }) {
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-gray-50">
          <th className="text-left px-4 py-2.5 text-xs text-gray-400 font-medium uppercase tracking-wide">Pole</th>
          <th className="text-left px-4 py-2.5 text-xs text-gray-400 font-medium uppercase tracking-wide">Waga matchu</th>
          <th className="text-left px-4 py-2.5 text-xs text-gray-400 font-medium uppercase tracking-wide">Blocking key</th>
          <th className="text-left px-4 py-2.5 text-xs text-gray-400 font-medium uppercase tracking-wide">Standaryzator</th>
          <th className="text-left px-4 py-2.5 text-xs text-gray-400 font-medium uppercase tracking-wide">Aktywny</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-gray-50">
        {configs.map(f => (
          <tr key={f.fieldName} className={cn(!f.isActive && 'opacity-50')}>
            <td className="px-4 py-2.5 font-mono text-xs text-gray-700">{f.fieldName}</td>
            <td className="px-4 py-2.5">
              {f.matchWeight > 0 ? (
                <div className="flex items-center gap-2">
                  <div className="w-16 h-1.5 bg-gray-100 rounded-full overflow-hidden">
                    <div className="h-full bg-blue-400 rounded-full" style={{ width: `${f.matchWeight * 100}%` }} />
                  </div>
                  <span className="text-xs tabular-nums text-gray-500">{Math.round(f.matchWeight * 100)}%</span>
                </div>
              ) : <span className="text-xs text-gray-300">—</span>}
            </td>
            <td className="px-4 py-2.5">
              {f.isBlockingKey ? <Badge color="blue">tak</Badge> : <span className="text-xs text-gray-300">nie</span>}
            </td>
            <td className="px-4 py-2.5 font-mono text-xs text-gray-500">{f.standardizer ?? '—'}</td>
            <td className="px-4 py-2.5">
              {f.isActive ? <Badge color="green">tak</Badge> : <Badge color="gray">nie</Badge>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function SourcePriorityTable({ configs }: { configs: SourcePriorityConfig[] }) {
  const defaults = configs.filter(c => c.fieldName === '__default__').sort((a, b) => a.priority - b.priority);
  const overrides = configs.filter(c => c.fieldName !== '__default__').sort((a, b) =>
    a.fieldName.localeCompare(b.fieldName) || a.priority - b.priority
  );

  const SOURCE_COLOR: Record<string, 'green' | 'blue' | 'amber' | 'gray'> = {
    lightspeed: 'green', yext: 'blue', mcwin: 'amber', gopos: 'gray',
  };

  return (
    <div>
      <div className="px-4 pt-3 pb-1">
        <p className="text-xs text-gray-400 font-medium uppercase tracking-wide mb-2">Domyślne priorytety</p>
        <div className="flex gap-2 flex-wrap">
          {defaults.map(d => (
            <div key={d.sourceSystem} className="flex items-center gap-1.5 bg-gray-50 border border-gray-100 rounded-lg px-3 py-1.5">
              <span className="text-xs font-bold text-gray-400">#{d.priority}</span>
              <Badge color={SOURCE_COLOR[d.sourceSystem] ?? 'gray'}>{d.sourceSystem}</Badge>
            </div>
          ))}
        </div>
      </div>
      {overrides.length > 0 && (
        <div className="px-4 pt-3 pb-3">
          <p className="text-xs text-gray-400 font-medium uppercase tracking-wide mb-2">Nadpisania per pole</p>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-50">
                <th className="text-left py-1.5 text-xs text-gray-400 font-medium">Pole</th>
                <th className="text-left py-1.5 text-xs text-gray-400 font-medium">Źródło</th>
                <th className="text-left py-1.5 text-xs text-gray-400 font-medium">Priorytet</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {overrides.map(o => (
                <tr key={`${o.fieldName}-${o.sourceSystem}`}>
                  <td className="py-1.5 font-mono text-xs text-gray-600">{o.fieldName}</td>
                  <td className="py-1.5"><Badge color={SOURCE_COLOR[o.sourceSystem] ?? 'gray'}>{o.sourceSystem}</Badge></td>
                  <td className="py-1.5 text-xs text-gray-500">#{o.priority}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export function ConfigViewer() {
  const { data: fieldConfigs, isLoading: loadingFields, isError: errFields } = useFieldConfigs();
  const { data: sourcePriorities, isLoading: loadingPrio, isError: errPrio } = useSourcePriorities();

  return (
    <div className="p-6 max-w-4xl">
      <div className="flex items-center gap-2 mb-6">
        <Settings size={18} className="text-gray-400" />
        <div>
          <h1 className="text-xl font-bold text-gray-900">Konfiguracja MDM</h1>
          <p className="text-sm text-gray-400 mt-0.5">Entity: <span className="font-mono text-xs">business_location</span></p>
        </div>
      </div>

      <SectionCard title="Konfiguracja pól (field_config)">
        {loadingFields ? (
          <div className="p-6 flex justify-center"><div className="animate-spin h-6 w-6 border-2 border-blue-400 border-t-transparent rounded-full" /></div>
        ) : errFields ? (
          <div className="p-4 flex items-center gap-2 text-red-500 text-sm"><AlertCircle size={14} /> Błąd ładowania</div>
        ) : (
          <FieldConfigTable configs={fieldConfigs ?? []} />
        )}
      </SectionCard>

      <SectionCard title="Priorytety źródeł (source_priority)">
        {loadingPrio ? (
          <div className="p-6 flex justify-center"><div className="animate-spin h-6 w-6 border-2 border-blue-400 border-t-transparent rounded-full" /></div>
        ) : errPrio ? (
          <div className="p-4 flex items-center gap-2 text-red-500 text-sm"><AlertCircle size={14} /> Błąd ładowania</div>
        ) : (
          <SourcePriorityTable configs={sourcePriorities ?? []} />
        )}
      </SectionCard>

      <p className="text-xs text-gray-300 text-center mt-2">
        Widok tylko do odczytu — aby zmienić konfigurację, zaktualizuj tabele <span className="font-mono">mdm_config.*</span> w Fabric Lakehouse
      </p>
    </div>
  );
}
