import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useGoldenLocations } from '../../hooks/useMdm';
import { MapPin, ChevronRight, ChevronLeft, AlertCircle } from 'lucide-react';
import { cn } from '../../lib/utils';

const FLAG: Record<string, string> = {
  DE: '🇩🇪', AT: '🇦🇹', CH: '🇨🇭', IT: '🇮🇹', NL: '🇳🇱', PL: '🇵🇱', FR: '🇫🇷', GB: '🇬🇧',
};

function CompletenessBar({ score }: { score?: number }) {
  if (score == null) return <span className="text-gray-300 text-xs">—</span>;
  const pct = Math.round(score * 100);
  const color = pct >= 90 ? 'bg-green-500' : pct >= 70 ? 'bg-amber-400' : 'bg-red-400';
  return (
    <div className="flex items-center gap-2">
      <div className="w-20 h-1.5 bg-gray-100 rounded-full overflow-hidden">
        <div className={cn('h-full rounded-full', color)} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-gray-500 tabular-nums">{pct}%</span>
    </div>
  );
}

const PAGE_SIZE = 25;

export function GoldenList() {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);
  const { data, isLoading, isError } = useGoldenLocations(page, PAGE_SIZE);

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
          <p className="text-sm text-gray-400 mt-0.5">{data.total} aktywnych lokalizacji</p>
        </div>
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 bg-gray-50/60">
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Nazwa</th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Kraj</th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Miasto</th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Kompletność</th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Źródła</th>
              <th className="px-4 py-3" />
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {data.items.map((loc) => (
              <tr
                key={loc.locationHk}
                onClick={() => navigate(`/golden/${loc.locationHk}`)}
                className="hover:bg-blue-50/40 cursor-pointer transition-colors"
              >
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <div className="w-6 h-6 rounded-md bg-blue-100 flex items-center justify-center shrink-0">
                      <MapPin size={12} className="text-blue-600" />
                    </div>
                    <span className="font-medium text-gray-900 truncate max-w-[220px]">{loc.name}</span>
                  </div>
                </td>
                <td className="px-4 py-3 text-gray-600">
                  {FLAG[loc.country ?? ''] ?? ''} {loc.country}
                </td>
                <td className="px-4 py-3 text-gray-600">{loc.city}</td>
                <td className="px-4 py-3">
                  <CompletenessBar score={loc.completenessScore} />
                </td>
                <td className="px-4 py-3">
                  <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded-full tabular-nums">
                    {loc.sourcesCount ?? 1} src
                  </span>
                </td>
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
    </div>
  );
}
