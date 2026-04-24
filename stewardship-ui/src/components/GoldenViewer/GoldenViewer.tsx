import { useParams, useNavigate } from 'react-router-dom';
import { useGoldenLocation, useStewardshipLog, useFieldOverride } from '../../hooks/useMdm';
import { ArrowLeft, Edit2, CheckCircle, MapPin, Star, Phone, Globe } from 'lucide-react';
import React, { useState } from 'react';
import type { MatchSource } from '../../types/mdm.types';

const SOURCE_COLORS: Record<MatchSource, string> = {
  lightspeed: 'bg-blue-100 text-blue-700',
  yext:       'bg-purple-100 text-purple-700',
  mcwin:      'bg-green-100 text-green-700',
  gopos:      'bg-orange-100 text-orange-700',
  manual:     'bg-gray-100 text-gray-600',
};

function SourceBadge({ source }: { source?: MatchSource | null }) {
  if (!source) return null;
  return (
    <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${SOURCE_COLORS[source] ?? 'bg-gray-100 text-gray-600'}`}>
      {source}
    </span>
  );
}

function EditableField({ fieldName, label, value, source, locationHk }: {
  fieldName: string; label: string; value?: string | number | null;
  source?: MatchSource | null; locationHk: string;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(String(value ?? ''));
  const [reason, setReason] = useState('');
  const { mutate: overrideField, isPending } = useFieldOverride();

  function handleSave() {
    // expectedOldValue = wartość którą user widział przed edycją.
    // Backend porówna z aktualnym stanem w DB → 412 jeśli ktoś inny zmienił.
    const expectedOldValue = value === null || value === undefined ? '' : String(value);
    overrideField(
      { locationHk, fieldName, newValue: draft, reason, expectedOldValue },
      { onSuccess: () => setEditing(false) },
    );
  }

  return (
    <div className="flex items-start gap-3 py-3 border-b border-gray-50 last:border-0">
      <div className="w-32 shrink-0 text-xs text-gray-400 pt-0.5 uppercase tracking-wide">{label}</div>
      <div className="flex-1">
        {editing ? (
          <div className="space-y-2">
            <input
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              className="w-full px-3 py-1.5 text-sm border border-blue-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-200"
            />
            <input
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="Powód zmiany..."
              className="w-full px-3 py-1.5 text-xs border border-gray-200 rounded-lg focus:outline-none"
            />
            <div className="flex gap-2">
              <button onClick={handleSave} disabled={isPending || !reason}
                className="px-3 py-1 text-xs bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50">
                Zapisz
              </button>
              <button onClick={() => setEditing(false)}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200">
                Anuluj
              </button>
            </div>
          </div>
        ) : (
          <div className="flex items-center gap-2 group">
            <span className="text-sm text-gray-900 font-medium">{value ?? '—'}</span>
            <SourceBadge source={source} />
            <button onClick={() => { setDraft(String(value ?? '')); setEditing(true); }}
              className="opacity-0 group-hover:opacity-100 transition-opacity p-1 rounded text-gray-400 hover:text-blue-600">
              <Edit2 size={12} />
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

export function GoldenViewer() {
  const { locationHk } = useParams<{ locationHk: string }>();
  const navigate = useNavigate();
  const { data: golden, isLoading } = useGoldenLocation(locationHk!);
  const { data: auditLog } = useStewardshipLog(locationHk!);

  if (isLoading) {
    return <div className="p-8 flex justify-center"><div className="animate-spin h-8 w-8 border-2 border-blue-500 border-t-transparent rounded-full" /></div>;
  }

  if (!golden) {
    return <div className="p-8 text-center text-gray-400">Rekord nie znaleziony.</div>;
  }

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="flex items-center gap-3 mb-6">
        <button onClick={() => navigate(-1)}
          className="p-2 rounded-lg hover:bg-gray-100 text-gray-500">
          <ArrowLeft size={18} />
        </button>
        <div>
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            <MapPin size={20} className="text-blue-500" />
            {golden.name ?? 'Golden Record'}
          </h1>
          <p className="text-xs text-gray-400 font-mono">{locationHk}</p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Golden record fields */}
        <div className="md:col-span-2 bg-white rounded-xl border border-gray-100 shadow-sm p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
            <CheckCircle size={14} className="text-green-500" />
            Golden Record (hover na polu by edytować)
          </h3>

          {[
            { field: 'name',         label: 'Nazwa',   value: golden.name,        source: golden.nameSource    },
            { field: 'country',      label: 'Kraj',    value: golden.country,     source: golden.countrySource },
            { field: 'city',         label: 'Miasto',  value: golden.city,        source: golden.citySource    },
            { field: 'zip_code',     label: 'ZIP',     value: golden.zipCode                                   },
            { field: 'phone',        label: 'Tel',     value: golden.phone                                     },
            { field: 'website_url',  label: 'Strona',  value: golden.websiteUrl                                },
            { field: 'timezone',     label: 'TZ',      value: golden.timezone                                  },
            { field: 'currency_code',label: 'Waluta',  value: golden.currencyCode                              },
            { field: 'cost_center',  label: 'CC',      value: golden.costCenter                                },
            { field: 'region',       label: 'Region',  value: golden.region                                    },
          ].map(({ field, label, value, source }) => (
            <EditableField key={field}
              fieldName={field} label={label}
              value={value as string} source={source as MatchSource}
              locationHk={locationHk!}
            />
          ))}

          {golden.avgRating && (
            <div className="flex items-center gap-3 py-3 border-b border-gray-50">
              <div className="w-32 text-xs text-gray-400 uppercase">Rating</div>
              <Star size={14} className="text-yellow-400" />
              <span className="text-sm font-medium">{golden.avgRating.toFixed(1)} ({golden.reviewCount} reviews)</span>
              <SourceBadge source="yext" />
            </div>
          )}
        </div>

        {/* Sidebar: sources + audit */}
        <div className="space-y-4">
          {/* Source crosswalk */}
          <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-4">
            <h4 className="text-xs font-semibold text-gray-500 uppercase mb-3">Źródła danych</h4>
            <div className="space-y-2 text-xs">
              {golden.lightspeedBlId && (
                <div className="flex justify-between">
                  <SourceBadge source="lightspeed" />
                  <span className="text-gray-400 font-mono">BL:{golden.lightspeedBlId}</span>
                </div>
              )}
              {golden.yextId && (
                <div className="flex justify-between">
                  <SourceBadge source="yext" />
                  <span className="text-gray-400 font-mono">{golden.yextId}</span>
                </div>
              )}
              {golden.mcwinRestaurantId && (
                <div className="flex justify-between">
                  <SourceBadge source="mcwin" />
                  <span className="text-gray-400 font-mono">{golden.mcwinRestaurantId}</span>
                </div>
              )}
            </div>
          </div>

          {/* Audit log */}
          <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-4">
            <h4 className="text-xs font-semibold text-gray-500 uppercase mb-3">Historia zmian</h4>
            <div className="space-y-2">
              {(auditLog ?? []).slice(0, 8).map((entry) => (
                <div key={entry.logId} className="text-xs border-l-2 border-blue-200 pl-2">
                  <div className="font-semibold text-gray-700">{entry.action.replace(/_/g, ' ')}</div>
                  {entry.fieldName && (
                    <div className="text-gray-400">
                      {entry.fieldName}: <span className="line-through">{entry.oldValue}</span>
                      {' → '}<span className="text-green-600">{entry.newValue}</span>
                    </div>
                  )}
                  <div className="text-gray-300">{entry.changedBy} · {new Date(entry.changedAt).toLocaleDateString('pl')}</div>
                </div>
              ))}
              {!auditLog?.length && <p className="text-gray-300 text-xs">Brak historii zmian.</p>}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
