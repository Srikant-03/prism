/**
 * Centralized utility functions for the Data Intelligence Platform.
 */

/**
 * Creates a debounced version of a function.
 */
export function debounce<T extends (...args: any[]) => any>(
    fn: T,
    delayMs: number,
): (...args: Parameters<T>) => void {
    let timer: ReturnType<typeof setTimeout> | null = null;
    return (...args: Parameters<T>) => {
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => fn(...args), delayMs);
    };
}

/**
 * Format bytes to human-readable string.
 */
export function formatBytes(bytes: number, decimals = 2): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${sizes[i]}`;
}

/**
 * Format seconds to human-readable duration.
 */
export function formatDuration(seconds: number): string {
    if (seconds < 60) return `${Math.round(seconds)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

/**
 * Revoke an object URL after a short delay (prevents memory leaks from downloads).
 */
export function revokeUrlAfterDownload(url: string, delayMs = 100): void {
    setTimeout(() => URL.revokeObjectURL(url), delayMs);
}

/**
 * Generate a UUID using crypto.randomUUID with Math.random fallback.
 */
export function safeUUID(): string {
    return crypto.randomUUID?.() ?? Math.random().toString(36).slice(2) + Date.now().toString(36);
}
