// Generic entity record creation form — renders fields dynamically from EntitySchema.
// Replaces NewLocationForm for v2 entities. NewLocationForm remains for business_location (backward compat).

import { useState, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { useEntity } from '../../hooks/useEntity';
import { getEntitySchema, createRecordV2 } from '../../api/v2Api';
import type { FieldSchema } from '../../types/v2.types';
import { cn } from '../../lib/utils';

export function EntityForm() {
  const { entityId, selectedEntity } = useEntity();
  const queryClient = useQueryClient();
  const [formData, setFormData] = useState<Record<string, string>>({});

  const { data: schema, isLoading: schemaLoading } = useQuery({
    queryKey: ['v2', 'schema', entityId],
    queryFn: () => getEntitySchema(entityId),
    staleTime: 5 * 60_000,
  });

  const mutation = useMutation({
    mutationFn: async () => {
      const attrs: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(formData)) {
        if (v.trim()) attrs[k] = v.trim();
      }
      return createRecordV2(entityId, attrs);
    },
    onSuccess: (result) => {
      toast.success(`Rekord utworzony (${result.hk.slice(0, 12)}…)`);
      setFormData({});
      queryClient.invalidateQueries({ queryKey: ['v2', entityId] });
    },
    onError: (err) => {
      toast.error(`Błąd: ${(err as Error).message}`);
    },
  });

  // Group fields by groupName
  const fieldGroups = useMemo(() => {
    if (!schema?.fields) return new Map<string, FieldSchema[]>();
    const groups = new Map<string, FieldSchema[]>();
    for (const f of schema.fields.filter(f => f.isGoldenField)) {
      const group = f.groupName ?? 'Inne';
      if (!groups.has(group)) groups.set(group, []);
      groups.get(group)!.push(f);
    }
    return groups;
  }, [schema]);

  if (schemaLoading) {
    return <div className="p-8 text-gray-400 animate-pulse">Ładowanie schematu…</div>;
  }

  if (!schema) {
    return <div className="p-8 text-red-500">Nie znaleziono schematu dla: {entityId}</div>;
  }

  const handleChange = (fieldName: string, value: string) => {
    setFormData(prev => ({ ...prev, [fieldName]: value }));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // Validate required fields
    const missing = schema.fields
      .filter(f => f.isRequired && f.isGoldenField)
      .filter(f => !formData[f.fieldName]?.trim());
    if (missing.length > 0) {
      toast.error(`Wymagane pola: ${missing.map(f => f.displayNamePl).join(', ')}`);
      return;
    }
    mutation.mutate();
  };

  return (
    <div className="max-w-2xl mx-auto p-8">
      <h1 className="text-xl font-bold text-gray-900 mb-1">
        Nowy rekord: {selectedEntity?.displayLabelPl ?? entityId}
      </h1>
      <p className="text-sm text-gray-400 mb-6">
        Pola oznaczone * są wymagane.
      </p>

      <form onSubmit={handleSubmit} className="space-y-6">
        {[...fieldGroups.entries()].map(([groupName, fields]) => (
          <fieldset key={groupName} className="space-y-3">
            <legend className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-2">
              {groupName}
            </legend>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {fields.map(field => (
                <FieldInput
                  key={field.fieldName}
                  field={field}
                  value={formData[field.fieldName] ?? ''}
                  onChange={v => handleChange(field.fieldName, v)}
                />
              ))}
            </div>
          </fieldset>
        ))}

        <div className="flex gap-3 pt-4 border-t border-gray-100">
          <button
            type="submit"
            disabled={mutation.isPending}
            className={cn(
              'px-6 py-2.5 rounded-xl font-semibold text-white text-sm transition-colors',
              mutation.isPending ? 'bg-gray-400' : 'bg-blue-600 hover:bg-blue-700'
            )}
          >
            {mutation.isPending ? 'Tworzenie…' : 'Utwórz rekord'}
          </button>
          <button
            type="button"
            onClick={() => setFormData({})}
            className="px-4 py-2.5 rounded-xl text-sm text-gray-500 hover:bg-gray-100 transition-colors"
          >
            Wyczyść
          </button>
        </div>
      </form>
    </div>
  );
}

function FieldInput({
  field,
  value,
  onChange,
}: {
  field: FieldSchema;
  value: string;
  onChange: (v: string) => void;
}) {
  const label = `${field.displayNamePl}${field.isRequired ? ' *' : ''}`;

  const baseClass = cn(
    'w-full px-3 py-2 rounded-lg border text-sm transition-colors',
    'border-gray-200 focus:border-blue-400 focus:ring-1 focus:ring-blue-200 outline-none'
  );

  switch (field.uiWidget) {
    case 'boolean':
      return (
        <label className="flex items-center gap-2 col-span-1 text-sm text-gray-700">
          <input
            type="checkbox"
            checked={value === 'true'}
            onChange={e => onChange(e.target.checked ? 'true' : 'false')}
            className="rounded border-gray-300 text-blue-600"
          />
          {label}
        </label>
      );

    case 'number':
      return (
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">{label}</label>
          <input
            type="number"
            step="any"
            value={value}
            onChange={e => onChange(e.target.value)}
            className={baseClass}
          />
        </div>
      );

    case 'date':
      return (
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">{label}</label>
          <input
            type="date"
            value={value}
            onChange={e => onChange(e.target.value)}
            className={baseClass}
          />
        </div>
      );

    case 'select': {
      // For select widgets with enum validators, use the enum values
      const enumValidator = field.validators.find(v => v.type === 'enum');
      const options = (enumValidator?.value as string[]) ?? [];
      return (
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">{label}</label>
          <select
            value={value}
            onChange={e => onChange(e.target.value)}
            className={baseClass}
          >
            <option value="">— wybierz —</option>
            {options.map(opt => (
              <option key={opt} value={opt}>{opt}</option>
            ))}
          </select>
        </div>
      );
    }

    case 'textarea':
      return (
        <div className="col-span-2">
          <label className="block text-xs font-medium text-gray-500 mb-1">{label}</label>
          <textarea
            value={value}
            onChange={e => onChange(e.target.value)}
            rows={3}
            className={baseClass}
          />
        </div>
      );

    case 'text':
    default:
      return (
        <div>
          <label className="block text-xs font-medium text-gray-500 mb-1">{label}</label>
          <input
            type="text"
            value={value}
            onChange={e => onChange(e.target.value)}
            className={baseClass}
          />
        </div>
      );
  }
}
