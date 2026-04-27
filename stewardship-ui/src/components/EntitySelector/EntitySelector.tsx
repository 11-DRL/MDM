// Entity selector dropdown for the sidebar — shows all active MDM entities.
// Switching entities changes the context for all v2-aware screens.

import { useEntity } from '../../hooks/useEntity';
import { Building2, MapPin, Users, Database, DollarSign, type LucideIcon } from 'lucide-react';
import { cn } from '../../lib/utils';

const ICON_MAP: Record<string, LucideIcon> = {
  MapPin,
  Building2,
  Users,
  Database,
  DollarSign,
};

export function EntitySelector() {
  const { entities, entityId, setEntityId, isLoading } = useEntity();

  if (isLoading) {
    return (
      <div className="px-2 py-1.5 text-xs text-gray-400 animate-pulse">
        Ładowanie domen…
      </div>
    );
  }

  return (
    <div className="space-y-0.5">
      <p className="px-2 text-[10px] uppercase tracking-wider text-gray-400 font-semibold mb-1">
        Domena MDM
      </p>
      {entities.map(entity => {
        const Icon = ICON_MAP[entity.icon] ?? Database;
        const isSelected = entity.entityId === entityId;
        return (
          <button
            key={entity.entityId}
            onClick={() => setEntityId(entity.entityId)}
            className={cn(
              'flex items-center gap-2 w-full px-2 py-1.5 rounded-lg text-sm transition-colors text-left',
              isSelected
                ? 'bg-blue-50 text-blue-700 font-semibold'
                : 'text-gray-500 hover:bg-gray-50 hover:text-gray-700'
            )}
          >
            <Icon size={14} />
            <span className="truncate">{entity.displayLabelPl || entity.entityName}</span>
          </button>
        );
      })}
    </div>
  );
}
