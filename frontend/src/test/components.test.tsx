/**
 * Component tests for key frontend components and dashboard modules.
 * Run with: npm test
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { render, fireEvent, cleanup } from '@testing-library/react';

import ThemeToggle from '../components/common/ThemeToggle';
import { ThemeProvider, useTheme } from '../hooks/useTheme';
import UploadProgress from '../components/upload/UploadProgress';
import QualityScoreWidget from '../components/common/QualityScoreWidget';
import PromptBar from '../components/dashboard/PromptBar';
import GlobalFilterBar from '../components/dashboard/GlobalFilterBar';


// ── ThemeToggle tests ──────────────────────────────────────────────

describe('ThemeToggle', () => {
    beforeEach(() => {
        cleanup();
        localStorage.clear();
        document.documentElement.removeAttribute('data-theme');
        document.body.innerHTML = '';
    });

    it('renders without crashing', () => {
        const { container } = render(<ThemeToggle />);
        const button = container.querySelector('button');
        expect(button).toBeTruthy();
    });

    it('cycles through themes on click', () => {
        const { container } = render(<ThemeToggle />);
        const button = container.querySelector('button')!;
        expect(button).toBeTruthy();

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

    it('persists theme to localStorage', () => {
        const { container } = render(<ThemeToggle />);
        const button = container.querySelector('button')!;

        fireEvent.click(button);
        expect(localStorage.getItem('theme')).toBe('light');
    });
});


// ── useTheme hook tests ────────────────────────────────────────────

describe('useTheme hook', () => {
    it('exports a ThemeProvider and useTheme', () => {
        expect(ThemeProvider).toBeDefined();
        expect(useTheme).toBeDefined();
    });
});


// ── UploadProgress tests ───────────────────────────────────────────

describe('UploadProgress', () => {
    it('renders progress indicators', () => {
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
    it('renders with dimensions', () => {
        const dims = [
            { name: 'Completeness', score: 90, maxScore: 100 },
            { name: 'Consistency', score: 85, maxScore: 100 },
        ];
        const { container } = render(<QualityScoreWidget dimensions={dims} />);
        expect(container.firstChild).toBeTruthy();
    });
});


// ── Flagship Dashboard Component Tests ─────────────────────────────

describe('PromptBar', () => {
    it('renders input and submits user prompt', () => {
        let submittedPrompt = '';
        const { container } = render(
            <PromptBar
                onSubmit={(p) => { submittedPrompt = p; }}
                suggestions={['Monthly Revenue', 'Top Products']}
            />
        );

        const input = container.querySelector('input')!;
        expect(input).toBeTruthy();

        // Type in prompt
        fireEvent.change(input, { target: { value: 'Quarterly Profit by Region' } });

        // Submit via button or form
        const submitBtn = container.querySelector('button.ant-btn-primary')!;
        expect(submitBtn).toBeTruthy();
        fireEvent.click(submitBtn);

        expect(submittedPrompt).toBe('Quarterly Profit by Region');
    });

    it('renders clarification alert when provided', () => {
        const { getByText } = render(
            <PromptBar
                onSubmit={() => {}}
                clarification="Did you mean revenue by month or quarter?"
            />
        );

        expect(getByText(/Did you mean revenue by month or quarter/i)).toBeTruthy();
    });

    it('renders suggestion chips and fills prompt on click', () => {
        let submittedPrompt = '';
        const { getByText, container } = render(
            <PromptBar
                onSubmit={(p) => { submittedPrompt = p; }}
                suggestions={['Monthly Trend']}
            />
        );

        const chip = getByText('Monthly Trend');
        expect(chip).toBeTruthy();
        fireEvent.click(chip);

        const input = container.querySelector('input')!;
        expect(input.value).toBe('Monthly Trend');

        const submitBtn = container.querySelector('button.ant-btn-primary')!;
        fireEvent.click(submitBtn);
        expect(submittedPrompt).toBe('Monthly Trend');
    });
});


describe('GlobalFilterBar', () => {
    it('renders filter bar and active filter chips', () => {
        const filters = [
            { column: 'category', operator: '=', value: 'Electronics' }
        ];
        let updatedFilters: any = null;

        const { getByText, container } = render(
            <GlobalFilterBar
                filters={filters}
                columns={['date', 'category', 'revenue']}
                onFiltersChange={(f) => { updatedFilters = f; }}
            />
        );

        expect(getByText(/category = Electronics/i)).toBeTruthy();

        // Close/remove chip
        const closeIcon = container.querySelector('.ant-tag-close-icon')!;
        if (closeIcon) {
            fireEvent.click(closeIcon);
            expect(updatedFilters).toEqual([]);
        }
    });
});
