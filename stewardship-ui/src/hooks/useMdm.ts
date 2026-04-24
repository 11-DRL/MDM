import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import {
  getMatchCandidates, getQueueStats, getGoldenLocation, getGoldenLocations,
  getStewardshipLog, submitPairReview, overrideField, createLocation,
  getFieldConfigs, getSourcePriorities,
  ApiConflictError, ApiPreconditionError,
} from '../api/mdmApi';
import type { PairReviewAction } from '../types/mdm.types';

function formatReviewer(reviewedBy?: string, reviewedAt?: string): string {
  const who = reviewedBy ? ` przez ${reviewedBy}` : '';
  const when = reviewedAt ? ` o ${new Date(reviewedAt).toLocaleString('pl-PL')}` : '';
  return `${who}${when}`.trim();
}

function genericErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return 'Nieznany błąd';
}

// ---------- Queue ----------

export function useQueueStats() {
  return useQuery({
    queryKey: ['queue-stats'],
    queryFn: getQueueStats,
    refetchInterval: 30_000,  // odświeżaj co 30s
  });
}

export function useMatchCandidates(page = 1, pageSize = 20) {
  return useQuery({
    queryKey: ['match-candidates', page, pageSize],
    queryFn: () => getMatchCandidates(page, pageSize, 'pending'),
    placeholderData: (prev) => prev,
    refetchInterval: 20_000,
    refetchIntervalInBackground: false,
  });
}

// ---------- Pair review ----------

export function usePairReview() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (action: PairReviewAction) => submitPairReview(action),
    onSuccess: () => {
      // Invaliduj queue po każdej akcji
      queryClient.invalidateQueries({ queryKey: ['match-candidates'] });
      queryClient.invalidateQueries({ queryKey: ['queue-stats'] });
      toast.success('Recenzja zapisana');
    },
    onError: (err) => {
      if (err instanceof ApiConflictError) {
        toast.error(`Para została już zrecenzowana${formatReviewer(err.reviewedBy, err.reviewedAt)}. Odświeżam listę.`);
        queryClient.invalidateQueries({ queryKey: ['match-candidates'] });
        queryClient.invalidateQueries({ queryKey: ['queue-stats'] });
        return;
      }
      toast.error(`Nie udało się zapisać recenzji: ${genericErrorMessage(err)}`);
    },
  });
}

// ---------- Golden record ----------

export function useGoldenLocation(locationHk: string) {
  return useQuery({
    queryKey: ['golden-location', locationHk],
    queryFn: () => getGoldenLocation(locationHk),
    enabled: !!locationHk,
  });
}

export function useStewardshipLog(locationHk: string) {
  return useQuery({
    queryKey: ['stewardship-log', locationHk],
    queryFn: () => getStewardshipLog(locationHk),
    enabled: !!locationHk,
  });
}

// ---------- Field override ----------

export function useFieldOverride() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ locationHk, fieldName, newValue, reason, expectedOldValue }: {
      locationHk: string;
      fieldName: string;
      newValue: string;
      reason: string;
      expectedOldValue?: string | null;
    }) => overrideField(locationHk, fieldName, newValue, reason, expectedOldValue),
    onSuccess: (_, vars) => {
      queryClient.invalidateQueries({ queryKey: ['golden-location', vars.locationHk] });
      queryClient.invalidateQueries({ queryKey: ['stewardship-log', vars.locationHk] });
      toast.success('Zmiana zapisana');
    },
    onError: (err, vars) => {
      if (err instanceof ApiPreconditionError || err instanceof ApiConflictError) {
        toast.error('Rekord zmieniony przez innego użytkownika. Odświeżam dane.');
        queryClient.invalidateQueries({ queryKey: ['golden-location', vars.locationHk] });
        return;
      }
      toast.error(`Nie udało się zapisać: ${genericErrorMessage(err)}`);
    },
  });
}

// ---------- Create location ----------

export function useCreateLocation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: import('../api/mdmApi').CreateLocationInput) => createLocation(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['queue-stats'] });
      queryClient.invalidateQueries({ queryKey: ['golden-locations'] });
      toast.success('Lokalizacja dodana');
    },
    onError: (err) => {
      if (err instanceof ApiConflictError) {
        toast.error('Lokalizacja o tym business_key już istnieje');
        return;
      }
      toast.error(`Nie udało się utworzyć lokalizacji: ${genericErrorMessage(err)}`);
    },
  });
}

// ---------- Golden locations list ----------

export function useGoldenLocations(page = 1, pageSize = 25) {
  return useQuery({
    queryKey: ['golden-locations', page, pageSize],
    queryFn: () => getGoldenLocations(page, pageSize),
    placeholderData: (prev) => prev,
  });
}

// ---------- Config tables ----------

export function useFieldConfigs(entityId = 'business_location') {
  return useQuery({
    queryKey: ['field-configs', entityId],
    queryFn: () => getFieldConfigs(entityId),
    staleTime: 5 * 60_000,
  });
}

export function useSourcePriorities(entityId = 'business_location') {
  return useQuery({
    queryKey: ['source-priorities', entityId],
    queryFn: () => getSourcePriorities(entityId),
    staleTime: 5 * 60_000,
  });
}
