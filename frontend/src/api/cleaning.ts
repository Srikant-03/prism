/**
 * API client for the Data Cleaning Pipeline.
 */

import { API_BASE } from './ingestion';
import type { CleaningPlan, ActionResult } from '../types/cleaning';

export async function analyzeCleaning(fileId: string): Promise<CleaningPlan> {
    const res = await fetch(`${API_BASE}/cleaning/${fileId}/analyze`);
    if (!res.ok) throw new Error(`Analyze failed: ${res.statusText}`);
    return res.json();
}

export async function applyAction(
    fileId: string,
    actionIndex: number,
    selectedOption?: string,
): Promise<ActionResult> {
    const url = new URL(`${API_BASE}/cleaning/${fileId}/apply/${actionIndex}`);
    if (selectedOption) url.searchParams.set('selected_option', selectedOption);
    const res = await fetch(url.toString(), { method: 'POST' });
    if (!res.ok) throw new Error(`Apply failed: ${res.statusText}`);
    return res.json();
}

export async function applyAllDefinitive(fileId: string): Promise<{
    results: ActionResult[];
    total_applied: number;
    rows_after: number;
    columns_after: number;
}> {
    const res = await fetch(`${API_BASE}/cleaning/${fileId}/apply-all-definitive`, {
        method: 'POST',
    });
    if (!res.ok) throw new Error(`Apply-all failed: ${res.statusText}`);
    return res.json();
}

export async function skipAction(fileId: string, actionIndex: number): Promise<void> {
    const res = await fetch(`${API_BASE}/cleaning/${fileId}/skip/${actionIndex}`, {
        method: 'POST',
    });
    if (!res.ok) throw new Error(`Skip failed: ${res.statusText}`);
}

export async function previewAction(fileId: string, actionIndex: number): Promise<{
    action_index: number;
    action_type: string;
    preview: { before: Record<string, unknown>[]; after: Record<string, unknown>[] } | null;
    impact: Record<string, unknown>;
}> {
    const res = await fetch(`${API_BASE}/cleaning/${fileId}/preview/${actionIndex}`);
    if (!res.ok) throw new Error(`Preview failed: ${res.statusText}`);
    return res.json();
}
