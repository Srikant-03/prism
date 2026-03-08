/**
 * Component tests for key frontend components.
 * Run with: npm test
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';


// ── ThemeToggle tests ──────────────────────────────────────────────

describe('ThemeToggle', () => {
    beforeEach(() => {
        localStorage.clear();
        document.documentElement.removeAttribute('data-theme');
    });

    it('renders without crashing', async () => {
        const ThemeToggle = (await import('../components/common/ThemeToggle')).default;
        const { container } = render(<ThemeToggle />);
        expect(container.querySelector('button')).toBeTruthy();
    });

    it('cycles through themes on click', async () => {
        const ThemeToggle = (await import('../components/common/ThemeToggle')).default;
        render(<ThemeToggle />);
        const button = screen.getByRole('button');

        // Default is dark → click → light
        fireEvent.click(button);
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');

        // light → cyberpunk
        fireEvent.click(button);
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

        // cyberpunk → dark
        fireEvent.click(button);
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('persists theme to localStorage', async () => {
        const ThemeToggle = (await import('../components/common/ThemeToggle')).default;
        render(<ThemeToggle />);
        const button = screen.getByRole('button');

        fireEvent.click(button);
        expect(localStorage.getItem('theme')).toBe('light');
    });
});


// ── useTheme hook tests ────────────────────────────────────────────

describe('useTheme hook', () => {
    it('exports a ThemeProvider and useTheme', async () => {
        const mod = await import('../hooks/useTheme');
        expect(mod.ThemeProvider).toBeDefined();
        expect(mod.useTheme).toBeDefined();
    });
});


// ── UploadProgress tests ───────────────────────────────────────────

describe('UploadProgress', () => {
    it('renders progress indicators', async () => {
        const UploadProgress = (await import('../components/upload/UploadProgress')).default;
        const { container } = render(
            <UploadProgress
                uploadPct={50}
                stage="uploading"
            />
        );
        expect(container.firstChild).toBeTruthy();
    });
});


// ── QualityScoreWidget tests ───────────────────────────────────────

describe('QualityScoreWidget', () => {
    it('renders with dimensions', async () => {
        const QualityScoreWidget = (await import('../components/common/QualityScoreWidget')).default;
        const dims = [
            { name: 'Completeness', score: 90, maxScore: 100 },
            { name: 'Consistency', score: 85, maxScore: 100 },
        ];
        const { container } = render(<QualityScoreWidget dimensions={dims} />);
        expect(container.firstChild).toBeTruthy();
    });
});
