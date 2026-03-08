/**
 * API client for the ingestion backend.
 * Handles file uploads, WebSocket progress, and all interaction endpoints.
 */

import axios from 'axios';
import type { AxiosProgressEvent } from 'axios';
import type {
    IngestionResult,
    MultiFileResult,
    MalformedComparison,
} from '../types/ingestion';

export const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';
export const API_KEY = import.meta.env.VITE_API_KEY || 'dev-secret-key-123';

const api = axios.create({
    baseURL: API_BASE,
    timeout: 300000, // 5 min for large files
    headers: { 'X-API-Key': API_KEY },
});

/**
 * Upload file(s) for ingestion.
 */
export async function uploadFiles(
    files: File[],
    onUploadProgress?: (pct: number) => void,
): Promise<IngestionResult | MultiFileResult> {
    const formData = new FormData();
    files.forEach((file) => formData.append('files', file));

    const response = await api.post('/api/upload', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        onUploadProgress: (event: AxiosProgressEvent) => {
            if (event.total && onUploadProgress) {
                onUploadProgress(Math.round((event.loaded / event.total) * 100));
            }
        },
    });

    return response.data;
}

/**
 * Select Excel sheet(s) for an uploaded file.
 */
export async function selectSheets(
    fileId: string,
    selectedSheets: number[],
): Promise<IngestionResult> {
    const response = await api.post('/api/upload/select-sheet', {
        file_id: fileId,
        selected_sheets: selectedSheets,
    });
    return response.data;
}

/**
 * Confirm malformed data handling.
 */
export async function confirmMalformed(
    fileId: string,
    acceptBestEffort: boolean,
    dropMalformedRows: boolean = false,
): Promise<unknown> {
    const response = await api.post('/api/upload/confirm-malformed', {
        file_id: fileId,
        accept_best_effort: acceptBestEffort,
        drop_malformed_rows: dropMalformedRows,
    });
    return response.data;
}

/**
 * Resolve multi-file schema decision.
 */
export async function resolveMultiFile(
    fileIds: string[],
    action: 'merge' | 'separate' | 'exclude',
): Promise<unknown> {
    const response = await api.post('/api/upload/resolve-multi', {
        file_ids: fileIds,
        action,
    });
    return response.data;
}

/**
 * Get malformed data side-by-side comparison.
 */
export async function getMalformedComparison(
    fileId: string,
): Promise<{ comparisons: MalformedComparison[]; total_issues: number; summary: string }> {
    const response = await api.get(`/api/upload/${fileId}/malformed-comparison`);
    return response.data;
}

/**
 * Connect to WebSocket for progress updates.
 */
export function connectProgress(
    fileId: string,
    onUpdate: (data: unknown) => void,
    onError?: (error: Event) => void,
): WebSocket {
    const wsUrl = `${API_BASE.replace(/^http/, 'ws')}/api/ws/progress?api_key=${API_KEY}`;
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        ws.send(JSON.stringify({ file_id: fileId }));
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            onUpdate(data);
        } catch {
            // Non-JSON message
        }
    };

    ws.onerror = (error) => {
        if (onError) onError(error);
    };

    return ws;
}

/**
 * Fetch profiling results for an ingested file.
 * Auto-computes if not already cached on the backend.
 */
export async function fetchProfile(fileId: string): Promise<unknown> {
    const response = await api.get(`/api/profile/${fileId}`);
    return response.data;
}

/**
 * Override the inferred semantic type for a specific column.
 */
export async function overrideSchema(fileId: string, column: string, newType: string): Promise<void> {
    const response = await api.post(`/api/schema-override/${fileId}`, { column, new_type: newType });
    return response.data;
}
