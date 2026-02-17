/**
 * Custom hook for managing the data cleaning pipeline state.
 */

import { useState, useCallback } from 'react';
import type { CleaningPlan, ActionResult } from '../types/cleaning';
import {
    analyzeCleaning,
    applyAction as apiApplyAction,
    applyAllDefinitive as apiApplyAllDefinitive,
    skipAction as apiSkipAction,
} from '../api/cleaning';

export type CleaningStatus = 'idle' | 'analyzing' | 'ready' | 'applying' | 'error';

interface CleaningState {
    status: CleaningStatus;
    plan: CleaningPlan | null;
    appliedResults: ActionResult[];
    error: string | null;
}

const initialState: CleaningState = {
    status: 'idle',
    plan: null,
    appliedResults: [],
    error: null,
};

export function useCleaning(fileId: string | undefined) {
    const [state, setState] = useState<CleaningState>(initialState);

    const analyze = useCallback(async () => {
        if (!fileId) return;
        setState(s => ({ ...s, status: 'analyzing', error: null }));
        try {
            const plan = await analyzeCleaning(fileId);
            setState({ status: 'ready', plan, appliedResults: [], error: null });
        } catch (err: unknown) {
            const error = err as { message?: string };
            setState(s => ({
                ...s,
                status: 'error',
                error: error?.message || 'Analysis failed',
            }));
        }
    }, [fileId]);

    const applyAction = useCallback(async (actionIndex: number, selectedOption?: string) => {
        if (!fileId) return;
        setState(s => ({ ...s, status: 'applying' }));
        try {
            const result = await apiApplyAction(fileId, actionIndex, selectedOption);
            setState(s => {
                const newPlan = s.plan ? { ...s.plan } : null;
                if (newPlan) {
                    newPlan.actions = newPlan.actions.map(a =>
                        a.index === actionIndex ? { ...a, status: 'applied' as const } : a
                    );
                }
                return {
                    ...s,
                    status: 'ready',
                    plan: newPlan,
                    appliedResults: [...s.appliedResults, result],
                };
            });
        } catch (err: unknown) {
            const error = err as { message?: string };
            setState(s => ({ ...s, status: 'ready', error: error?.message || 'Apply failed' }));
        }
    }, [fileId]);

    const skipActionHandler = useCallback(async (actionIndex: number) => {
        if (!fileId) return;
        try {
            await apiSkipAction(fileId, actionIndex);
            setState(s => {
                const newPlan = s.plan ? { ...s.plan } : null;
                if (newPlan) {
                    newPlan.actions = newPlan.actions.map(a =>
                        a.index === actionIndex ? { ...a, status: 'skipped' as const } : a
                    );
                }
                return { ...s, plan: newPlan };
            });
        } catch {
            // Silently handle skip errors
        }
    }, [fileId]);

    const applyAllDefinitive = useCallback(async () => {
        if (!fileId) return;
        setState(s => ({ ...s, status: 'applying' }));
        try {
            const { results } = await apiApplyAllDefinitive(fileId);
            setState(s => {
                const newPlan = s.plan ? { ...s.plan } : null;
                if (newPlan) {
                    const appliedIndices = new Set(results.map(r => r.action_index));
                    newPlan.actions = newPlan.actions.map(a =>
                        appliedIndices.has(a.index) ? { ...a, status: 'applied' as const } : a
                    );
                }
                return {
                    ...s,
                    status: 'ready',
                    plan: newPlan,
                    appliedResults: [...s.appliedResults, ...results],
                };
            });
        } catch (err: unknown) {
            const error = err as { message?: string };
            setState(s => ({ ...s, status: 'ready', error: error?.message || 'Batch apply failed' }));
        }
    }, [fileId]);

    const reset = useCallback(() => {
        setState(initialState);
    }, []);

    return {
        state,
        analyze,
        applyAction,
        skipAction: skipActionHandler,
        applyAllDefinitive,
        reset,
    };
}
