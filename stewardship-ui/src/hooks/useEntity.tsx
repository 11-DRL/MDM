// EntityContext — provides the currently selected entity across the app.
// Used by all v2-aware components to know which entity's data to show.

import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { listEntities } from '../api/v2Api';
import type { EntityInfo } from '../types/v2.types';

interface EntityContextValue {
  /** All active entities from mdm_config */
  entities: EntityInfo[];
  /** Currently selected entity */
  selectedEntity: EntityInfo | null;
  /** Currently selected entity ID */
  entityId: string;
  /** Switch to a different entity */
  setEntityId: (id: string) => void;
  /** Loading state */
  isLoading: boolean;
}

const EntityContext = createContext<EntityContextValue>({
  entities: [],
  selectedEntity: null,
  entityId: 'business_location',
  setEntityId: () => {},
  isLoading: true,
});

export function EntityProvider({ children }: { children: ReactNode }) {
  const [entityId, setEntityId] = useState(() => {
    // Restore from sessionStorage if available
    return sessionStorage.getItem('mdm_selected_entity') ?? 'business_location';
  });

  const { data: entities = [], isLoading } = useQuery({
    queryKey: ['v2', 'entities'],
    queryFn: listEntities,
    staleTime: 5 * 60_000,
  });

  // Persist selection
  useEffect(() => {
    sessionStorage.setItem('mdm_selected_entity', entityId);
  }, [entityId]);

  const selectedEntity = entities.find(e => e.entityId === entityId) ?? null;

  return (
    <EntityContext.Provider value={{ entities, selectedEntity, entityId, setEntityId, isLoading }}>
      {children}
    </EntityContext.Provider>
  );
}

export function useEntity() {
  return useContext(EntityContext);
}
