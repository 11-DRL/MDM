import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { getGoldenRecords, getEntitySchema } from '../../api/v2Api';
import { useEntity } from '../../hooks/useEntity';
import { MapPin, Building2, ChevronRight, ChevronLeft, AlertCircle } from 'lucide-react';
import { cn } from '../../lib/utils';
import type { GenericGoldenRecord, FieldSchema } from '../../types/v2.types';

const FLAG: Record<string, string> = {
  DE: '🇩🇪', AT: '🇦🇹', CH: '🇨🇭', IT: '🇮🇹', NL: '🇳🇱', PL: '🇵🇱', FR: '🇫🇷', GB: '🇬🇧', CZ: '🇨🇿',
};

const ICON_MAP: Record<string, React.ReactNode> = {
  MapPin: <MapPin size={12} className="text-blue-600" />,
  Building2: <Building2 size={12} className="text-indigo-600" />,
};

/** Pick the best "display name" from a record's attributes */
function displayName(rec: GenericGoldenRecord): string {
  const a = rec.attributes;
  return String(a.name ?? a.legal_entity_code ?? a.entity_code ?? Object.values(a)[0] ?? '—');
}

/** Pick up to N visible columns from schema (excluding the name column which is always shown) */
function pickColumns(fields: FieldSchema[], max = 4): FieldSchema[] {
  return fields
    .filter(f => f.isGoldenField && f.fieldName !== 'name')
    .sort((a, b) => a.displayOrder - b.displayOrder)
    .slice(0, max);
}

function CellValue({ value, widget }: { value: unknown; widget: string }) {
  if (value == null) return <span className="text-gray-300">—</span>;
  if (widget === 'boolean') return <span>{value ? '✓' : '✗'}</span>;
  const str = String(value);
  if (widget === 'select' && FLAG[str]) return <>{FLAG[str]} {str}</>;
  return <>{str}</>;
}

const PAGE_SIZE = 25;

export function GoldenList() {
  const navigate = useNavigate();
  const { entityId, selectedEntity } = useEntity();
  const [page, setPage] = useState(1);

  const { data, isLoading, isError } = useQuery({
    queryKey: ['v2', 'golden', entityId, page, PAGE_SIZE],
    queryFn: () => getGoldenRecords(entityId, page, PAGE_SIZE),
    placeholderData: (prev) => prev,
  });

  const { data: schema } = useQuery({
    queryKey: ['v2', 'schema', entityId],
    queryFn: () => getEntitySchema(entityId),
    staleTime: 5 * 60_000,
  });

  const columns = schema ? pickColumns(schema.fields) : [];
  const icon = ICON_MAP[selectedEntity?.icon ?? 'MapPin'] ?? ICON_MAP.MapPin;
  const entityLabel = selectedEntity?.displayLabelPl ?? 'Rekordy';

  if (isLoading) {
    return (
      <div className="p-8 flex justify-center">
        <div className="animate-spin h-8 w-8 border-2 border-blue-500 border-t-transparent rounded-full" />
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="p-8 flex items-center gap-2 text-red-500">
        <AlertCircle size={18} /> Nie udało się załadować rekordów.
      </div>
    );
  }

  const totalPages = Math.ceil(data.total / PAGE_SIZE);

  return (
    <div className="p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Golden Records</h1>
          <p className="text-sm text-gray-400 mt-0.5">
            {data.total} {entityLabel.toLowerCase()}
          </p>
        </div>
      </div>

      {data.items.length === 0 ? (
        <div className="text-center py-16 text-gray-400">
          <p className="text-lg">Brak rekordów</p>
          <p className="text-sm mt-1">Dodaj pierwszy rekord przez formularz "Nowy rekord"</p>
        </div>
      ) : (
        <>
          {/* Table */}
          <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100 bg-gray-50/60">
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Nazwa</th>
                  {columns.map(col => (
                    <th key={col.fieldName} className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">
                      {col.displayNamePl}
                    </th>
                  ))}
                  <th className="px-4 py-3" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {data.items.map((rec) => (
                  <tr
                    key={rec.hk}
                    onClick={() => navigate(`/golden/${rec.hk}`)}
                    className="hover:bg-blue-50/40 cursor-pointer transition-colors"
                  >
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <div className={cn(
                          "w-6 h-6 rounded-md flex items-center justify-center shrink-0",
                          entityId === 'legal_entity' ? 'bg-indigo-100' : 'bg-blue-100'
                        )}>
                          {icon}
                        </div>
                        <span className="font-medium text-gray-900 truncate max-w-[220px]">
                          {displayName(rec)}
                        </span>
                      </div>
                    </td>
                    {columns.map(col => (
                      <td key={col.fieldName} className="px-4 py-3 text-gray-600">
                        <CellValue value={rec.attributes[col.fieldName]} widget={col.uiWidget} />
                      </td>
                    ))}
                    <td className="px-4 py-3 text-gray-300">
                      <ChevronRight size={16} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-4 text-sm text-gray-500">
              <span>Strona {page} z {totalPages}</span>
              <div className="flex gap-2">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="flex items-center gap-1 px-3 py-1.5 rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  <ChevronLeft size={14} /> Poprzednia
                </button>
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                  className="flex items-center gap-1 px-3 py-1.5 rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  Następna <ChevronRight size={14} />
                </button>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
