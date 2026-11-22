/**
 * Unified Number & Currency Formatting Utilities for PRISM Dashboards.
 * Eliminates IEEE-754 floating-point artifacts (e.g. 0.11670000000000001 or 26.299999999999997).
 */

export function cleanNumber(val: any): number | null {
    if (val === null || val === undefined || val === '') return null;
    const num = Number(val);
    if (isNaN(num)) return null;
    // Fix floating-point precision issues
    return Math.round(num * 1e8) / 1e8;
}

export function formatCompactNumber(value: number | null | undefined): string {
    if (value === null || value === undefined || isNaN(value)) return '—';
    const num = cleanNumber(value);
    if (num === null) return '—';

    const abs = Math.abs(num);
    const sign = num < 0 ? '-' : '';

    if (abs >= 1_000_000_000) {
        const trimmed = parseFloat((abs / 1_000_000_000).toFixed(2));
        return `${sign}${trimmed}B`;
    }
    if (abs >= 1_000_000) {
        const trimmed = parseFloat((abs / 1_000_000).toFixed(2));
        return `${sign}${trimmed}M`;
    }
    if (abs >= 1_000) {
        const trimmed = parseFloat((abs / 1_000).toFixed(1));
        return `${sign}${trimmed}K`;
    }
    return Number.isInteger(num) ? `${sign}${abs}` : `${sign}${parseFloat(abs.toFixed(2))}`;
}

export function formatValue(value: any, format?: string): string {
    if (value === null || value === undefined || value === '' || Number.isNaN(value)) return '—';
    const num = cleanNumber(value);
    if (num === null) return typeof value === 'string' ? value : '—';

    const fmt = (format || 'auto').toLowerCase();

    if (fmt === 'currency' || fmt.includes('$')) {
        const abs = Math.abs(num);
        const sign = num < 0 ? '-' : '';
        if (abs >= 1_000_000_000) return `${sign}$${(abs / 1_000_000_000).toFixed(2)}B`;
        if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(2)}M`;
        if (abs >= 1_000) return `${sign}$${(abs / 1_000).toFixed(1)}K`;
        return `${sign}$${abs.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }

    if (fmt === 'percent' || fmt === 'percentage' || fmt.includes('%')) {
        // If value is a ratio between -1 and 1, multiply by 100
        const pct = Math.abs(num) <= 1 && num !== 0 ? num * 100 : num;
        return `${pct.toFixed(1)}%`;
    }

    if (fmt === 'compact') {
        return formatCompactNumber(num);
    }

    if (fmt === 'integer') {
        return Math.round(num).toLocaleString('en-US');
    }

    if (fmt === 'decimal') {
        return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    // Auto format
    const abs = Math.abs(num);
    if (abs >= 10_000) {
        return formatCompactNumber(num);
    }
    return Number.isInteger(num)
        ? num.toLocaleString('en-US')
        : num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function formatAxisTick(val: any): string {
    if (val === null || val === undefined) return '';
    if (typeof val === 'number') {
        return formatCompactNumber(val);
    }
    const str = String(val);
    // If it's a date string like 2024-01-01 or 2024-01, format cleanly
    if (/^\d{4}-\d{2}(-\d{2})?$/.test(str)) {
        return str;
    }
    return str.length > 14 ? `${str.slice(0, 12)}…` : str;
}
