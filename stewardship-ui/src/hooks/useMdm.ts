import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getMatchCandidates, getQueueStats, getGoldenLocation,
  getStewardshipLog, submitPairReview, overrideField
} from '../api/mdmApi';
import type { PairReviewAction } from '../types/mdm.types';

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
    mutationFn: ({ locationHk, fieldName, newValue, reason }: {
      locationHk: string; fieldName: string; newValue: string; reason: string;
    }) => overrideField(locationHk, fieldName, newValue, reason),
    onSuccess: (_, vars) => {
      queryClient.invalidateQueries({ queryKey: ['golden-location', vars.locationHk] });
      queryClient.invalidateQueries({ queryKey: ['stewardship-log', vars.locationHk] });
    },
  });
}
