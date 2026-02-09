/**
 * Custom hook for managing file upload state and flows.
 */

import { useState, useCallback, useRef } from 'react';
import type {
    UploadState,
    UploadStatus,
    IngestionResult,
    MultiFileResult,
} from '../types/ingestion';
import {
    uploadFiles,
    selectSheets,
    confirmMalformed,
    resolveMultiFile,
} from '../api/ingestion';

const initialState: UploadState = {
    status: 'idle',
    progress: null,
    result: null,
    multiResult: null,
    error: null,
};

export function useUpload() {
    const [state, setState] = useState<UploadState>(initialState);
    const [uploadPct, setUploadPct] = useState(0);
    const wsRef = useRef<WebSocket | null>(null);

    const reset = useCallback(() => {
        setState(initialState);
        setUploadPct(0);
        if (wsRef.current) {
            wsRef.current.close();
            wsRef.current = null;
        }
    }, []);

    const handleUpload = useCallback(async (files: File[]) => {
        reset();
        setState((s) => ({ ...s, status: 'uploading' }));

        try {
            const data = await uploadFiles(files, (pct) => {
                setUploadPct(pct);
            });

            // Determine the type of result
            if ('results' in data) {
                // Multi-file result
                const multi = data as MultiFileResult;
                let status: UploadStatus = 'complete';

                if (multi.requires_schema_decision) {
                    status = 'awaiting_schema_decision';
                }

                setState({
                    status,
                    progress: null,
                    result: null,
                    multiResult: multi,
                    error: null,
                });
            } else {
                // Single file result
                const result = data as IngestionResult;
                let status: UploadStatus = 'complete';

                if (result.requires_sheet_selection) {
                    status = 'awaiting_sheet_selection';
                } else if (result.malformed_report?.has_issues) {
                    status = 'awaiting_malformed_review';
                }

                if (!result.success) {
                    status = 'error';
                }

                setState({
                    status,
                    progress: null,
                    result,
                    multiResult: null,
                    error: result.errors?.length ? result.errors.join('; ') : null,
                });
            }
        } catch (err: unknown) {
            const error = err as { response?: { data?: { detail?: string } }; message?: string };
            setState({
                status: 'error',
                progress: null,
                result: null,
                multiResult: null,
                error: error?.response?.data?.detail || error?.message || 'Upload failed',
            });
        }
    }, [reset]);

    const handleSelectSheets = useCallback(async (fileId: string, sheets: number[]) => {
        setState((s) => ({ ...s, status: 'processing' }));
        try {
            const result = await selectSheets(fileId, sheets);
            let status: UploadStatus = 'complete';
            if (result.malformed_report?.has_issues) {
                status = 'awaiting_malformed_review';
            }
            setState((s) => ({ ...s, status, result }));
        } catch (err: unknown) {
            const error = err as { response?: { data?: { detail?: string } } };
            setState((s) => ({
                ...s,
                status: 'error',
                error: error?.response?.data?.detail || 'Sheet selection failed',
            }));
        }
    }, []);

    const handleConfirmMalformed = useCallback(
        async (fileId: string, accept: boolean, drop: boolean = false) => {
            setState((s) => ({ ...s, status: 'processing' }));
            try {
                await confirmMalformed(fileId, accept, drop);
                setState((s) => ({ ...s, status: 'complete' }));
            } catch (err: unknown) {
                const error = err as { response?: { data?: { detail?: string } } };
                setState((s) => ({
                    ...s,
                    status: 'error',
                    error: error?.response?.data?.detail || 'Action failed',
                }));
            }
        },
        [],
    );

    const handleResolveMulti = useCallback(
        async (fileIds: string[], action: 'merge' | 'separate' | 'exclude') => {
            setState((s) => ({ ...s, status: 'processing' }));
            try {
                await resolveMultiFile(fileIds, action);
                setState((s) => ({ ...s, status: 'complete' }));
            } catch (err: unknown) {
                const error = err as { response?: { data?: { detail?: string } } };
                setState((s) => ({
                    ...s,
                    status: 'error',
                    error: error?.response?.data?.detail || 'Action failed',
                }));
            }
        },
        [],
    );

    return {
        state,
        uploadPct,
        handleUpload,
        handleSelectSheets,
        handleConfirmMalformed,
        handleResolveMulti,
        reset,
    };
}
