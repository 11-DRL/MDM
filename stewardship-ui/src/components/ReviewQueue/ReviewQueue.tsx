import React from 'react';
import { useMatchCandidates, useQueueStats, usePairReview } from '../../hooks/useMdm';
import type { MatchCandidate } from '../../types/mdm.types';
import { CheckCircle, XCircle, Clock, TrendingUp, MapPin } from 'lucide-react';
import { cn } from '../../lib/utils';
import { useNavigate } from 'react-router-dom';

// ---------- Stats bar ----------

function StatsBar() {
  const { data: stats, isLoading } = useQueueStats();

  if (isLoading) return <div className="h-24 animate-pulse bg-gray-100 rounded-lg" />;

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
      <StatCard label="Pending Review" value={stats?.pendingCount ?? 0}
        icon={<Clock className="text-amber-500" />} accent="amber" />
      <StatCard label="Auto Accepted" value={stats?.autoAcceptedCount ?? 0}
        icon={<CheckCircle className="text-green-400" />} accent="green" />
      <StatCard label="Golden Records" value={stats?.totalGoldenRecords ?? 0}
        icon={<MapPin className="text-blue-500" />} accent="blue" />
      <StatCard label="Avg Completeness" value={`${((stats?.avgCompletenessScore ?? 0) * 100).toFixed(0)}%`}
        icon={<TrendingUp className="text-purple-500" />} accent="purple" />
    </div>
  );
}

function StatCard({ label, value, icon, accent }: {
  label: string; value: number | string; icon: React.ReactNode; accent: string;
}) {
  return (
    <div className={cn("bg-white rounded-xl border border-gray-100 shadow-sm p-4 flex items-center gap-3")}>
      <div className={cn("p-2 rounded-lg", `bg-${accent}-50`)}>{icon}</div>
      <div>
        <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
        <p className="text-2xl font-bold text-gray-900">{value}</p>
      </div>
    </div>
  );
}

// ---------- Queue Row ----------

function ScoreBadge({ score }: { score: number }) {
  const pct = Math.round(score * 100);
  const color = score >= 0.97 ? 'bg-green-100 text-green-700'
              : score >= 0.90 ? 'bg-blue-100 text-blue-700'
              : 'bg-amber-100 text-amber-700';
  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold", color)}>
      {pct}%
    </span>
  );
}

function QueueRow({ candidate, onSelect }: {
  candidate: MatchCandidate;
  onSelect: (c: MatchCandidate) => void;
}) {
  const { mutate: reviewPair, isPending } = usePairReview();

  function handleQuickAction(action: 'accept' | 'reject', e: React.MouseEvent) {
    e.stopPropagation();
    reviewPair({
      pairId: candidate.pairId,
      action,
      canonicalHk: action === 'accept' ? candidate.hkLeft : undefined,
    });
  }

  return (
    <tr
      className="border-b border-gray-50 hover:bg-gray-50 cursor-pointer transition-colors"
      onClick={() => onSelect(candidate)}
    >
      <td className="py-3 px-4">
        <div className="font-medium text-gray-900 text-sm">
          {(candidate as any).leftName ?? candidate.hkLeft.slice(0, 8) + '...'}
        </div>
        <div className="text-xs text-gray-400">
          {(candidate as any).leftCity}, {(candidate as any).leftCountry}
        </div>
      </td>
      <td className="py-3 px-4">
        <div className="font-medium text-gray-900 text-sm">
          {(candidate as any).rightName ?? candidate.hkRight.slice(0, 8) + '...'}
        </div>
        <div className="text-xs text-gray-400">
          {(candidate as any).rightCity}, {(candidate as any).rightCountry}
        </div>
      </td>
      <td className="py-3 px-4 text-center">
        <ScoreBadge score={candidate.matchScore} />
      </td>
      <td className="py-3 px-4 text-center">
        <span className="text-xs text-gray-500">{candidate.matchType.replace(/_/g, ' ')}</span>
      </td>
      <td className="py-3 px-4 text-center">
        <div className="flex items-center justify-center gap-2">
          <button
            onClick={(e) => handleQuickAction('accept', e)}
            disabled={isPending}
            className="p-1.5 rounded-lg bg-green-50 hover:bg-green-100 text-green-600 transition-colors disabled:opacity-50"
            title="Accept — ta sama lokalizacja"
          >
            <CheckCircle size={16} />
          </button>
          <button
            onClick={(e) => handleQuickAction('reject', e)}
            disabled={isPending}
            className="p-1.5 rounded-lg bg-red-50 hover:bg-red-100 text-red-600 transition-colors disabled:opacity-50"
            title="Reject — różne lokalizacje"
          >
            <XCircle size={16} />
          </button>
        </div>
      </td>
    </tr>
  );
}

// ---------- Main Queue Component ----------

export function ReviewQueue() {
  const navigate = useNavigate();
  const [page, setPage] = React.useState(1);
  const { data, isLoading, isError } = useMatchCandidates(page, 25);

  function handleSelectPair(candidate: MatchCandidate) {
    navigate(`/pairs/${candidate.pairId}`);
  }

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Review Queue</h1>
        <p className="text-gray-500 text-sm mt-1">Business Location — match pairs do zatwierdzenia</p>
      </div>

      <StatsBar />

      {isLoading && (
        <div className="space-y-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="h-16 animate-pulse bg-gray-100 rounded-lg" />
          ))}
        </div>
      )}

      {isError && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
          Błąd ładowania kolejki. Sprawdź połączenie z Fabric SQL Endpoint.
        </div>
      )}

      {data && (
        <>
          <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b border-gray-100">
                <tr>
                  <th className="py-3 px-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Lewa (wyższy priorytet)</th>
                  <th className="py-3 px-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Prawa</th>
                  <th className="py-3 px-4 text-center text-xs font-semibold text-gray-500 uppercase tracking-wider">Score</th>
                  <th className="py-3 px-4 text-center text-xs font-semibold text-gray-500 uppercase tracking-wider">Typ</th>
                  <th className="py-3 px-4 text-center text-xs font-semibold text-gray-500 uppercase tracking-wider">Akcja</th>
                </tr>
              </thead>
              <tbody>
                {data.items.map((c) => (
                  <QueueRow key={c.pairId} candidate={c} onSelect={handleSelectPair} />
                ))}
                {data.items.length === 0 && (
                  <tr>
                    <td colSpan={5} className="py-12 text-center text-gray-400">
                      <CheckCircle size={32} className="mx-auto mb-2 text-green-300" />
                      Kolejka pusta — wszystkie pary przejrzane!
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {data.total > 25 && (
            <div className="flex justify-center gap-2 mt-4">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}
                className="px-3 py-1.5 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-40">
                ← Wstecz
              </button>
              <span className="px-3 py-1.5 text-sm text-gray-500">
                {page} / {Math.ceil(data.total / 25)}
              </span>
              <button onClick={() => setPage(p => p + 1)} disabled={page >= Math.ceil(data.total / 25)}
                className="px-3 py-1.5 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-40">
                Dalej →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
