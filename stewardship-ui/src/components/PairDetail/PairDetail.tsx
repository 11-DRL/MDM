import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { usePairReview } from '../../hooks/useMdm';
import { getMatchCandidates } from '../../api/mdmApi';
import type { MatchCandidate, SatelliteLocation } from '../../types/mdm.types';
import { CheckCircle, XCircle, ArrowLeft, Star, Phone, Globe, MapPin } from 'lucide-react';
import { cn } from '../../lib/utils';

// Mapuje match_score na opisową etykietę
function scoreLabel(score: number): { label: string; color: string } {
  if (score >= 0.97) return { label: 'Bardzo wysoki', color: 'text-green-600' };
  if (score >= 0.90) return { label: 'Wysoki',        color: 'text-blue-600' };
  if (score >= 0.85) return { label: 'Umiarkowany',   color: 'text-amber-600' };
  return                     { label: 'Niski',         color: 'text-red-600' };
}

// Panel atrybutów jednej strony pary
function AttributePanel({ title, source, attrs, isLeft }: {
  title: string;
  source?: string;
  attrs?: SatelliteLocation;
  isLeft: boolean;
}) {
  const borderColor = isLeft ? 'border-blue-200 bg-blue-50/30' : 'border-orange-200 bg-orange-50/30';

  return (
    <div className={cn("rounded-xl border p-5 flex-1", borderColor)}>
      <div className="mb-4">
        <div className="flex items-center gap-2">
          <span className={cn("text-xs font-bold uppercase tracking-wider px-2 py-0.5 rounded-full",
            isLeft ? "bg-blue-100 text-blue-700" : "bg-orange-100 text-orange-700")}>
            {isLeft ? 'Lewy (wyższy priorytet)' : 'Prawy'}
          </span>
          {source && (
            <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full uppercase">{source}</span>
          )}
        </div>
        <h3 className="text-lg font-bold text-gray-900 mt-2">{attrs?.name ?? title}</h3>
      </div>

      <dl className="space-y-2 text-sm">
        {[
          { key: 'Kraj',    val: attrs?.country,      icon: <MapPin size={12} /> },
          { key: 'Miasto',  val: attrs?.city,          icon: null },
          { key: 'Zip',     val: attrs?.zipCode,       icon: null },
          { key: 'Adres',   val: attrs?.address,       icon: null },
          { key: 'Telefon', val: attrs?.phone,         icon: <Phone size={12} /> },
          { key: 'Strona',  val: attrs?.websiteUrl,    icon: <Globe size={12} /> },
          { key: 'Rating',  val: attrs?.avgRating ? `${attrs.avgRating} ⭐ (${attrs.reviewCount} reviews)` : null, icon: <Star size={12} /> },
          { key: 'CostCenter', val: attrs?.costCenter, icon: null },
          { key: 'Region',  val: attrs?.region,        icon: null },
        ].map(({ key, val, icon }) => val ? (
          <div key={key} className="flex gap-2">
            <dt className="text-gray-400 w-24 shrink-0 flex items-center gap-1">{icon}{key}</dt>
            <dd className="text-gray-900 font-medium break-all">{val}</dd>
          </div>
        ) : null)}
      </dl>
    </div>
  );
}

// Pasek komponentów score
function ScoreBreakdown({ candidate }: { candidate: MatchCandidate }) {
  const { label, color } = scoreLabel(candidate.matchScore);
  return (
    <div className="bg-white rounded-xl border border-gray-100 p-5 shadow-sm">
      <h4 className="text-sm font-semibold text-gray-700 mb-3">Score breakdown</h4>
      <div className="flex items-center gap-4 mb-3">
        <div className="text-4xl font-bold text-gray-900">
          {Math.round(candidate.matchScore * 100)}%
        </div>
        <div>
          <p className={cn("text-sm font-semibold", color)}>{label}</p>
          <p className="text-xs text-gray-400">{candidate.matchType.replace(/_/g, ' ')}</p>
        </div>
      </div>
      <div className="space-y-2 text-xs">
        {candidate.nameScore != null && (
          <ScoreBar label="Nazwa (50%)" value={candidate.nameScore} weight={0.5} />
        )}
        {candidate.zipMatch != null && (
          <ScoreBar label="Zip code (30%)" value={candidate.zipMatch ? 1 : 0} weight={0.3} />
        )}
        {candidate.geoScore != null && (
          <ScoreBar label="Geo (20%)" value={candidate.geoScore ?? 0} weight={0.2} />
        )}
      </div>
    </div>
  );
}

function ScoreBar({ label, value, weight }: { label: string; value: number; weight: number }) {
  const pct = Math.round(value * 100);
  const contribution = value * weight;
  return (
    <div>
      <div className="flex justify-between mb-0.5">
        <span className="text-gray-500">{label}</span>
        <span className="font-semibold text-gray-700">{pct}% → +{Math.round(contribution * 100)}pts</span>
      </div>
      <div className="h-1.5 bg-gray-100 rounded-full">
        <div className="h-1.5 bg-blue-400 rounded-full" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

// ---------- Main Component ----------

export function PairDetail() {
  const { pairId } = useParams<{ pairId: string }>();
  const navigate = useNavigate();
  const { mutate: reviewPair, isPending } = usePairReview();

  // Pobierz parę (w produkcji: endpoint per pairId; tu filtrujemy z listy)
  const { data: queueData, isLoading } = useQuery({
    queryKey: ['pair-detail', pairId],
    queryFn: () => getMatchCandidates(1, 1000, 'all'),
  });

  const candidate = queueData?.items.find(c => c.pairId === pairId);

  function handleAction(action: 'accept' | 'reject') {
    if (!candidate) return;
    reviewPair({
      pairId: candidate.pairId,
      action,
      canonicalHk: action === 'accept' ? candidate.hkLeft : undefined,
    }, {
      onSuccess: () => navigate('/queue'),
    });
  }

  if (isLoading) {
    return <div className="p-8 flex justify-center"><div className="animate-spin h-8 w-8 border-2 border-blue-500 border-t-transparent rounded-full" /></div>;
  }

  if (!candidate) {
    return <div className="p-8 text-center text-gray-400">Para nie znaleziona.</div>;
  }

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex items-center gap-3 mb-6">
        <button onClick={() => navigate('/queue')}
          className="p-2 rounded-lg hover:bg-gray-100 text-gray-500 transition-colors">
          <ArrowLeft size={18} />
        </button>
        <div>
          <h1 className="text-xl font-bold text-gray-900">Pair Review</h1>
          <p className="text-xs text-gray-400 font-mono">{candidate.pairId}</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Panels */}
        <div className="lg:col-span-2 flex gap-4">
          <AttributePanel
            title={candidate.hkLeft.slice(0, 8)}
            source={(candidate as any).leftSource}
            attrs={candidate.leftAttributes}
            isLeft
          />
          <AttributePanel
            title={candidate.hkRight.slice(0, 8)}
            source={(candidate as any).rightSource}
            attrs={candidate.rightAttributes}
            isLeft={false}
          />
        </div>

        {/* Score + Actions */}
        <div className="space-y-4">
          <ScoreBreakdown candidate={candidate} />

          <div className="bg-white rounded-xl border border-gray-100 p-5 shadow-sm space-y-3">
            <h4 className="text-sm font-semibold text-gray-700">Decyzja</h4>
            <button
              onClick={() => handleAction('accept')}
              disabled={isPending}
              className="w-full flex items-center justify-center gap-2 bg-green-600 hover:bg-green-700 text-white font-semibold py-2.5 px-4 rounded-lg transition-colors disabled:opacity-50"
            >
              <CheckCircle size={18} />
              Ta sama lokalizacja (Accept)
            </button>
            <button
              onClick={() => handleAction('reject')}
              disabled={isPending}
              className="w-full flex items-center justify-center gap-2 bg-white hover:bg-red-50 text-red-600 font-semibold py-2.5 px-4 rounded-lg border border-red-200 transition-colors disabled:opacity-50"
            >
              <XCircle size={18} />
              Różne lokalizacje (Reject)
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
