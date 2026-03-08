/**
 * Shared API client utilities — single source of truth for authenticated fetch.
 */

import { API_BASE, API_KEY } from './ingestion';

/**
 * Wrapper around fetch() that automatically attaches the X-API-Key header.
 * All API modules should import and use this instead of defining their own.
 */
export const fetchAuth = (url: RequestInfo | URL, init?: RequestInit) => {
    return fetch(url, {
        ...init,
        headers: {
            ...init?.headers,
            'X-API-Key': API_KEY,
        },
    });
};

export { API_BASE, API_KEY };
