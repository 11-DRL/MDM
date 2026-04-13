import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useCreateLocation } from '../../hooks/useMdm';
import type { CreateLocationInput } from '../../api/mdmApi';
import { MapPin, ArrowLeft, ArrowRight, CheckCircle, Building2, Phone, Globe, DollarSign } from 'lucide-react';
import { cn } from '../../lib/utils';

// ─── Stałe ────────────────────────────────────────────────────────────────────

const COUNTRIES = [
  { code: 'DE', label: 'Niemcy' },
  { code: 'AT', label: 'Austria' },
  { code: 'CH', label: 'Szwajcaria' },
  { code: 'IT', label: 'Włochy' },
  { code: 'NL', label: 'Holandia' },
  { code: 'CZ', label: 'Czechy' },
];

const TIMEZONES = [
  'Europe/Berlin', 'Europe/Vienna', 'Europe/Zurich',
  'Europe/Rome', 'Europe/Amsterdam', 'Europe/Prague',
];

const CURRENCIES: Record<string, string> = {
  DE: 'EUR', AT: 'EUR', IT: 'EUR', NL: 'EUR', CZ: 'CZK', CH: 'CHF',
};

// ─── Typy ─────────────────────────────────────────────────────────────────────

type FormData = CreateLocationInput;

type Step = 'basic' | 'contact' | 'business' | 'summary';

const STEPS: { id: Step; label: string; icon: React.ReactNode }[] = [
  { id: 'basic',    label: 'Podstawowe',  icon: <MapPin size={16} /> },
  { id: 'contact',  label: 'Kontakt',     icon: <Phone size={16} /> },
  { id: 'business', label: 'Biznesowe',   icon: <DollarSign size={16} /> },
  { id: 'summary',  label: 'Podsumowanie',icon: <CheckCircle size={16} /> },
];

const STEP_ORDER: Step[] = ['basic', 'contact', 'business', 'summary'];

// ─── Helpers ──────────────────────────────────────────────────────────────────

function Field({ label, required, error, children }: {
  label: string; required?: boolean; error?: string; children: React.ReactNode;
}) {
  return (
    <div className="space-y-1">
      <label className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
        {label} {required && <span className="text-red-400">*</span>}
      </label>
      {children}
      {error && <p className="text-xs text-red-500">{error}</p>}
    </div>
  );
}

function Input({ value, onChange, placeholder, type = 'text', error }: {
  value: string; onChange: (v: string) => void;
  placeholder?: string; type?: string; error?: string;
}) {
  return (
    <input
      type={type}
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      className={cn(
        "w-full px-3 py-2 text-sm border rounded-lg focus:outline-none focus:ring-2 transition-colors",
        error
          ? "border-red-300 focus:ring-red-100"
          : "border-gray-200 focus:ring-blue-100 focus:border-blue-400"
      )}
    />
  );
}

function Select({ value, onChange, options, placeholder }: {
  value: string; onChange: (v: string) => void;
  options: { value: string; label: string }[]; placeholder?: string;
}) {
  return (
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-100 focus:border-blue-400 bg-white"
    >
      {placeholder && <option value="">{placeholder}</option>}
      {options.map(o => (
        <option key={o.value} value={o.value}>{o.label}</option>
      ))}
    </select>
  );
}

// ─── Walidacja ────────────────────────────────────────────────────────────────

function validateBasic(d: FormData): Record<string, string> {
  const e: Record<string, string> = {};
  if (!d.name?.trim())    e.name    = 'Nazwa jest wymagana';
  if (!d.country?.trim()) e.country = 'Kraj jest wymagany';
  if (!d.city?.trim())    e.city    = 'Miasto jest wymagane';
  if (d.name && d.name.length < 3) e.name = 'Nazwa musi mieć min. 3 znaki';
  return e;
}

function validateContact(d: FormData): Record<string, string> {
  const e: Record<string, string> = {};
  if (d.phone && !/^\+?[\d\s\-()]{7,}$/.test(d.phone))
    e.phone = 'Niepoprawny format numeru telefonu';
  if (d.websiteUrl && !/^https?:\/\/.+/.test(d.websiteUrl))
    e.websiteUrl = 'URL musi zaczynać się od https://';
  if (d.latitude !== undefined && (d.latitude < -90 || d.latitude > 90))
    e.latitude = 'Szerokość geograficzna: -90 do 90';
  if (d.longitude !== undefined && (d.longitude < -180 || d.longitude > 180))
    e.longitude = 'Długość geograficzna: -180 do 180';
  return e;
}

// ─── Kroki formularza ─────────────────────────────────────────────────────────

function StepBasic({ data, onChange, errors }: {
  data: FormData; onChange: (f: Partial<FormData>) => void; errors: Record<string, string>;
}) {
  return (
    <div className="space-y-4">
      <Field label="Nazwa restauracji" required error={errors.name}>
        <Input
          value={data.name ?? ''}
          onChange={v => onChange({ name: v })}
          placeholder="L'Osteria Warszawa Centrum"
          error={errors.name}
        />
      </Field>

      <div className="grid grid-cols-2 gap-4">
        <Field label="Kraj" required error={errors.country}>
          <Select
            value={data.country ?? ''}
            onChange={v => onChange({ country: v, currencyCode: CURRENCIES[v] ?? '' })}
            options={COUNTRIES.map(c => ({ value: c.code, label: `${c.code} — ${c.label}` }))}
            placeholder="Wybierz kraj..."
          />
        </Field>
        <Field label="Miasto" required error={errors.city}>
          <Input
            value={data.city ?? ''}
            onChange={v => onChange({ city: v })}
            placeholder="Warszawa"
            error={errors.city}
          />
        </Field>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <Field label="Kod pocztowy">
          <Input
            value={data.zipCode ?? ''}
            onChange={v => onChange({ zipCode: v })}
            placeholder="00-001"
          />
        </Field>
        <Field label="Strefa czasowa">
          <Select
            value={data.timezone ?? ''}
            onChange={v => onChange({ timezone: v })}
            options={TIMEZONES.map(tz => ({ value: tz, label: tz }))}
            placeholder="Wybierz..."
          />
        </Field>
      </div>

      <Field label="Adres ulicy">
        <Input
          value={data.address ?? ''}
          onChange={v => onChange({ address: v })}
          placeholder="ul. Marszałkowska 1"
        />
      </Field>
    </div>
  );
}

function StepContact({ data, onChange, errors }: {
  data: FormData; onChange: (f: Partial<FormData>) => void; errors: Record<string, string>;
}) {
  return (
    <div className="space-y-4">
      <Field label="Telefon" error={errors.phone}>
        <Input
          value={data.phone ?? ''}
          onChange={v => onChange({ phone: v })}
          placeholder="+48 22 123 45 67"
          error={errors.phone}
        />
      </Field>

      <Field label="Strona internetowa" error={errors.websiteUrl}>
        <Input
          value={data.websiteUrl ?? ''}
          onChange={v => onChange({ websiteUrl: v })}
          placeholder="https://losteria.net/pl/warszawa"
          error={errors.websiteUrl}
        />
      </Field>

      <div className="grid grid-cols-2 gap-4">
        <Field label="Szerokość geograficzna" error={errors.latitude}>
          <Input
            type="number"
            value={data.latitude !== undefined ? String(data.latitude) : ''}
            onChange={v => onChange({ latitude: v ? parseFloat(v) : undefined })}
            placeholder="52.2297"
            error={errors.latitude}
          />
        </Field>
        <Field label="Długość geograficzna" error={errors.longitude}>
          <Input
            type="number"
            value={data.longitude !== undefined ? String(data.longitude) : ''}
            onChange={v => onChange({ longitude: v ? parseFloat(v) : undefined })}
            placeholder="21.0122"
            error={errors.longitude}
          />
        </Field>
      </div>

      <p className="text-xs text-gray-400 flex items-center gap-1">
        <Globe size={12} />
        Możesz skopiować współrzędne z Google Maps — kliknij prawym na mapę i wybierz "Skopiuj współrzędne"
      </p>
    </div>
  );
}

function StepBusiness({ data, onChange }: {
  data: FormData; onChange: (f: Partial<FormData>) => void;
}) {
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <Field label="Cost Center">
          <Input
            value={data.costCenter ?? ''}
            onChange={v => onChange({ costCenter: v })}
            placeholder="PL-WAW-001"
          />
        </Field>
        <Field label="Region">
          <Input
            value={data.region ?? ''}
            onChange={v => onChange({ region: v })}
            placeholder="Poland"
          />
        </Field>
      </div>

      <Field label="Waluta">
        <Select
          value={data.currencyCode ?? ''}
          onChange={v => onChange({ currencyCode: v })}
          options={[
            { value: 'EUR', label: 'EUR — Euro' },
            { value: 'CHF', label: 'CHF — Franc szwajcarski' },
            { value: 'CZK', label: 'CZK — Korona czeska' },
            { value: 'PLN', label: 'PLN — Złoty' },
          ]}
          placeholder="Wybierz walutę..."
        />
      </Field>

      <Field label="Notatki dodatkowe">
        <textarea
          value={data.notes ?? ''}
          onChange={e => onChange({ notes: e.target.value })}
          placeholder="Dodatkowe informacje o lokalizacji (np. godziny otwarcia, specyfika)..."
          rows={3}
          className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-100 resize-none"
        />
      </Field>

      <div className="bg-blue-50 rounded-lg p-3 text-xs text-blue-700">
        <strong>ℹ Info:</strong> Ręcznie dodana lokalizacja trafia natychmiast do Golden Records jako rekord <span className="font-mono">source=manual</span>. Po podłączeniu systemów POS (Lightspeed, GoPOS) — system automatycznie zaproponuje match z tym rekordem.
      </div>
    </div>
  );
}

function SummaryRow({ label, value }: { label: string; value?: string | number }) {
  if (!value) return null;
  return (
    <div className="flex items-start gap-3 py-2 border-b border-gray-50 last:border-0 text-sm">
      <span className="w-36 shrink-0 text-xs text-gray-400 uppercase tracking-wide pt-0.5">{label}</span>
      <span className="text-gray-900 font-medium">{value}</span>
    </div>
  );
}

function StepSummary({ data }: { data: FormData }) {
  const countryLabel = COUNTRIES.find(c => c.code === data.country)?.label ?? data.country;
  const filled = [data.name, data.country, data.city, data.zipCode, data.address, data.phone, data.websiteUrl, data.timezone, data.currencyCode, data.costCenter].filter(Boolean).length;
  const completeness = Math.round((filled / 10) * 100);

  return (
    <div className="space-y-4">
      <div className="bg-green-50 border border-green-100 rounded-lg p-3 flex items-center gap-2">
        <CheckCircle size={16} className="text-green-500 shrink-0" />
        <div className="text-sm">
          <span className="font-semibold text-green-700">Gotowe do zapisania.</span>
          <span className="text-green-600 ml-1">Completeness score: <strong>{completeness}%</strong></span>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-100 p-4">
        <h4 className="text-xs font-semibold text-gray-500 uppercase mb-3">Dane lokalizacji</h4>
        <SummaryRow label="Nazwa"    value={data.name} />
        <SummaryRow label="Kraj"     value={`${data.country} — ${countryLabel}`} />
        <SummaryRow label="Miasto"   value={data.city} />
        <SummaryRow label="ZIP"      value={data.zipCode} />
        <SummaryRow label="Adres"    value={data.address} />
        <SummaryRow label="Telefon"  value={data.phone} />
        <SummaryRow label="Strona"   value={data.websiteUrl} />
        <SummaryRow label="TZ"       value={data.timezone} />
        <SummaryRow label="Waluta"   value={data.currencyCode} />
        <SummaryRow label="Cost CC"  value={data.costCenter} />
        <SummaryRow label="Region"   value={data.region} />
        {data.latitude && <SummaryRow label="Geo" value={`${data.latitude}, ${data.longitude}`} />}
        {data.notes && <SummaryRow label="Notatki" value={data.notes} />}
      </div>
    </div>
  );
}

// ─── Pasek kroków ─────────────────────────────────────────────────────────────

function StepBar({ current }: { current: Step }) {
  const currentIdx = STEP_ORDER.indexOf(current);
  return (
    <div className="flex items-center gap-0 mb-8">
      {STEPS.map((step, idx) => {
        const done    = idx < currentIdx;
        const active  = step.id === current;
        return (
          <React.Fragment key={step.id}>
            <div className="flex flex-col items-center">
              <div className={cn(
                "w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold transition-colors",
                done   ? "bg-blue-600 text-white" :
                active ? "bg-blue-600 text-white ring-4 ring-blue-100" :
                         "bg-gray-100 text-gray-400"
              )}>
                {done ? <CheckCircle size={14} /> : step.icon}
              </div>
              <span className={cn("mt-1 text-xs whitespace-nowrap",
                active ? "text-blue-600 font-semibold" : "text-gray-400"
              )}>{step.label}</span>
            </div>
            {idx < STEPS.length - 1 && (
              <div className={cn(
                "flex-1 h-0.5 mx-2 mb-5 transition-colors",
                idx < currentIdx ? "bg-blue-600" : "bg-gray-100"
              )} />
            )}
          </React.Fragment>
        );
      })}
    </div>
  );
}

// ─── Główny komponent ─────────────────────────────────────────────────────────

const EMPTY: FormData = {
  name: '', country: '', city: '',
  zipCode: '', address: '', phone: '', websiteUrl: '',
  timezone: '', currencyCode: '', costCenter: '', region: '', notes: '',
};

export function NewLocationForm() {
  const navigate = useNavigate();
  const [step, setStep] = useState<Step>('basic');
  const [data, setData] = useState<FormData>(EMPTY);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const { mutate: createLoc, isPending, isSuccess, data: created } = useCreateLocation();

  function patch(fields: Partial<FormData>) {
    setData(prev => ({ ...prev, ...fields }));
    // Czyść błędy przy edycji
    const cleared = { ...errors };
    Object.keys(fields).forEach(k => delete cleared[k]);
    setErrors(cleared);
  }

  function goNext() {
    let errs: Record<string, string> = {};
    if (step === 'basic')   errs = validateBasic(data);
    if (step === 'contact') errs = validateContact(data);
    if (Object.keys(errs).length > 0) { setErrors(errs); return; }

    const idx = STEP_ORDER.indexOf(step);
    if (idx < STEP_ORDER.length - 1) setStep(STEP_ORDER[idx + 1]);
  }

  function goBack() {
    const idx = STEP_ORDER.indexOf(step);
    if (idx > 0) setStep(STEP_ORDER[idx - 1]);
  }

  function handleSubmit() {
    createLoc(data, {
      onSuccess: (result) => {
        // Po chwili przekieruj do złotego rekordu
        setTimeout(() => navigate(`/golden/${result.locationHk}`), 1200);
      },
    });
  }

  // ─── Sukces ───
  if (isSuccess) {
    return (
      <div className="p-6 max-w-xl mx-auto flex flex-col items-center justify-center gap-4 pt-24">
        <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center">
          <CheckCircle size={32} className="text-green-500" />
        </div>
        <h2 className="text-xl font-bold text-gray-900">Lokalizacja dodana!</h2>
        <p className="text-gray-500 text-sm text-center">
          <strong>{data.name}</strong> ({data.city}, {data.country}) została zapisana jako Golden Record.
        </p>
        <p className="text-xs text-gray-400">Przekierowanie do Golden Viewer…</p>
      </div>
    );
  }

  return (
    <div className="p-6 max-w-2xl mx-auto">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <button onClick={() => navigate('/queue')}
          className="p-2 rounded-lg hover:bg-gray-100 text-gray-500">
          <ArrowLeft size={18} />
        </button>
        <div>
          <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            <Building2 size={20} className="text-blue-500" />
            Nowa lokalizacja
          </h1>
          <p className="text-xs text-gray-400 mt-0.5">Ręczne wprowadzenie do Golden Records</p>
        </div>
      </div>

      {/* Stepper */}
      <StepBar current={step} />

      {/* Karta formularza */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-6 mb-6">
        {step === 'basic'    && <StepBasic    data={data} onChange={patch} errors={errors} />}
        {step === 'contact'  && <StepContact  data={data} onChange={patch} errors={errors} />}
        {step === 'business' && <StepBusiness data={data} onChange={patch} />}
        {step === 'summary'  && <StepSummary  data={data} />}
      </div>

      {/* Nawigacja */}
      <div className="flex justify-between">
        <button
          onClick={goBack}
          disabled={step === 'basic'}
          className="flex items-center gap-2 px-4 py-2 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
        >
          <ArrowLeft size={14} /> Wstecz
        </button>

        {step === 'summary' ? (
          <button
            onClick={handleSubmit}
            disabled={isPending}
            className="flex items-center gap-2 px-6 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg disabled:opacity-60 transition-colors"
          >
            {isPending ? (
              <span className="animate-spin border-2 border-white border-t-transparent rounded-full w-4 h-4" />
            ) : (
              <CheckCircle size={14} />
            )}
            {isPending ? 'Zapisywanie…' : 'Zapisz lokalizację'}
          </button>
        ) : (
          <button
            onClick={goNext}
            className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors"
          >
            Dalej <ArrowRight size={14} />
          </button>
        )}
      </div>
    </div>
  );
}
