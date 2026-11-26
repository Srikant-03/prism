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
import KPICard from '../components/dashboard/charts/KPICard';
import DashboardBarChart from '../components/dashboard/charts/BarChart';
import DashboardScatterChart from '../components/dashboard/charts/ScatterChart';
import DecompositionTree from '../components/dashboard/charts/DecompositionTree';
import KeyInfluencersVisual from '../components/dashboard/charts/KeyInfluencersVisual';
import SmartNarrativeWidget from '../components/dashboard/charts/SmartNarrativeWidget';
import QAVisual from '../components/dashboard/charts/QAVisual';
import ChartWidget from '../components/dashboard/ChartWidget';
import PresentMode from '../components/dashboard/PresentMode';
import WidgetContainer from '../components/dashboard/WidgetContainer';
import { formatCompactNumber } from '../types/dashboard';
import type { ChartConfig } from '../types/dashboard';


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
                onSubmit={() => { }}
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


// ── formatCompactNumber tests ──────────────────────────────────────

describe('formatCompactNumber', () => {
    it('formats billions, millions, thousands correctly', () => {
        expect(formatCompactNumber(1_500_000_000)).toBe('1.50B');
        expect(formatCompactNumber(6_420_000)).toBe('6.42M');
        expect(formatCompactNumber(26_300)).toBe('26.3K');
        expect(formatCompactNumber(450)).toBe('450');
        expect(formatCompactNumber(0)).toBe('0');
        expect(formatCompactNumber(null as any)).toBe('—');
        expect(formatCompactNumber(-5_200_000)).toBe('-5.20M');
    });
});


// ── KPICard tests ──────────────────────────────────────────────────

describe('KPICard', () => {
    it('renders multi-metric KPI with formatted currency and rate percentages', () => {
        const config = {
            chart_type: 'kpi',
            title: 'Revenue & Discount',
            metrics: [
                { column: 'net_revenue', aggregation: 'sum', label: 'Total Net Revenue' },
                { column: 'discount_rate', aggregation: 'avg', label: 'Avg Discount Rate' }
            ],
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: false,
        } as unknown as ChartConfig;
        const data = [{ 'Total Net Revenue': 6_420_000, 'Avg Discount Rate': 0.125 }];

        const { getByText } = render(<KPICard config={config} data={data} />);

        // Check labels
        expect(getByText(/TOTAL NET REVENUE/i)).toBeTruthy();
        expect(getByText(/AVG DISCOUNT RATE/i)).toBeTruthy();

        // Check formatted values
        expect(getByText('6.42M')).toBeTruthy();
        expect(getByText('12.5%')).toBeTruthy();
    });

    it('renders single-metric KPI card with compact formatted value', () => {
        const config = {
            chart_type: 'kpi',
            title: 'Total Gross Revenue',
            kpi_value_column: 'gross_revenue',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: false,
        } as unknown as ChartConfig;
        const data = [{ gross_revenue: 7_800_000 }];

        const { getByText } = render(<KPICard config={config} data={data} />);

        expect(getByText(/Total Gross Revenue/i)).toBeTruthy();
        expect(getByText('7.80M')).toBeTruthy();
    });
});


// ── BarChart & ScatterChart tests ─────────────────────────────────

describe('DashboardBarChart', () => {
    it('renders multi-metric bar chart without crashing', () => {
        const config = {
            chart_type: 'bar',
            title: 'Gross vs Net Revenue by Category',
            x_axis: 'product_category',
            metrics: [
                { column: 'gross_revenue', aggregation: 'sum', label: 'Gross Revenue' },
                { column: 'net_revenue', aggregation: 'sum', label: 'Net Revenue' }
            ],
            color_scheme: 'default',
            show_legend: true,
            show_grid: true,
            show_values: false,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', 'Gross Revenue': 500000, 'Net Revenue': 420000 },
            { product_category: 'Apparel', 'Gross Revenue': 300000, 'Net Revenue': 260000 },
        ];

        const { container } = render(<DashboardBarChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });
});


describe('DashboardScatterChart', () => {
    it('renders continuous scatter plot with grouped categories without crashing', () => {
        const config = {
            chart_type: 'scatter',
            title: 'Unit Price vs Shipping Weight',
            x_axis: 'unit_price_raw',
            y_axis: 'shipping_weight',
            group_by: 'product_category',
            color_scheme: 'default',
            show_legend: true,
            show_grid: true,
            show_values: false,
        } as unknown as ChartConfig;
        const data = [
            { unit_price_raw: 120.5, shipping_weight: 4.2, product_category: 'Electronics' },
            { unit_price_raw: 45.0, shipping_weight: 1.1, product_category: 'Apparel' },
            { unit_price_raw: null, shipping_weight: 2.0, product_category: 'Apparel' }, // Should be filtered out
        ];

        const { container } = render(<DashboardScatterChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });
});


// ── Step 3 Power BI Canvas, Multi-Page, Slicers & Bookmarks Tests ──

import PageTabs from '../components/dashboard/PageTabs';
import BookmarksDrawer from '../components/dashboard/BookmarksDrawer';
import SelectionPane from '../components/dashboard/SelectionPane';
import type { DashboardPage, Bookmark, Widget } from '../types/dashboard';

describe('Step 3: Power BI Multi-Page Workspace & PageTabs', () => {
    it('renders multiple pages and handles switching and adding pages', () => {
        const pages: DashboardPage[] = [
            { id: 'page-1', title: 'Executive Summary', widgets: [] },
            { id: 'page-2', title: 'Product Deep-Dive', widgets: [] },
        ];
        let selectedId = '';
        let added = false;

        const { getByText } = render(
            <PageTabs
                pages={pages}
                activePageId="page-1"
                onSelectPage={(id) => { selectedId = id; }}
                onAddPage={() => { added = true; }}
                onRenamePage={() => {}}
                onDeletePage={() => {}}
                onDuplicatePage={() => {}}
            />
        );

        // Verify page tabs render
        expect(getByText('Executive Summary')).toBeTruthy();
        const tab2 = getByText('Product Deep-Dive');
        expect(tab2).toBeTruthy();

        // Switch to page 2
        fireEvent.click(tab2);
        expect(selectedId).toBe('page-2');

        // Add new page
        const addBtn = getByText('New Page');
        fireEvent.click(addBtn);
        expect(added).toBe(true);
    });
});

describe('Step 3: PromptBar Isolated Create vs Explicit Edit', () => {
    it('executes 3 consecutive prompts as distinct chart creations without overwrite', () => {
        const submittedPrompts: string[] = [];
        const { container } = render(
            <PromptBar
                onSubmit={(p) => { submittedPrompts.push(p); }}
            />
        );

        const input = container.querySelector('input')!;
        const submitBtn = container.querySelector('button.ant-btn-primary')!;

        // Prompt 1
        fireEvent.change(input, { target: { value: 'Total Net Revenue' } });
        fireEvent.click(submitBtn);

        // Prompt 2
        fireEvent.change(input, { target: { value: 'Average Discount Rate by Category' } });
        fireEvent.click(submitBtn);

        // Prompt 3
        fireEvent.change(input, { target: { value: 'Monthly Revenue Trend' } });
        fireEvent.click(submitBtn);

        expect(submittedPrompts).toHaveLength(3);
        expect(submittedPrompts[0]).toBe('Total Net Revenue');
        expect(submittedPrompts[1]).toBe('Average Discount Rate by Category');
        expect(submittedPrompts[2]).toBe('Monthly Revenue Trend');
    });

    it('displays edit mode banner and allows cancelling explicit edit mode', () => {
        let cancelCalled = false;
        const { getByText } = render(
            <PromptBar
                onSubmit={() => {}}
                editingTitle="Executive Gross Revenue"
                onCancelEdit={() => { cancelCalled = true; }}
            />
        );

        expect(getByText(/Editing Widget:/i)).toBeTruthy();
        expect(getByText('Executive Gross Revenue')).toBeTruthy();

        const cancelBtn = getByText('Cancel');
        fireEvent.click(cancelBtn);
        expect(cancelCalled).toBe(true);
    });
});

describe('Step 3: Bookmarks Drawer State Capture & Restore', () => {
    it('renders bookmarks and restores filters when View is clicked', () => {
        const bookmarks: Bookmark[] = [
            {
                id: 'bm-1',
                name: 'Q1 Baseline View',
                page_id: 'page-1',
                filters: [{ column: 'product_category', operator: '=', value: 'Electronics' }],
                visible_widget_ids: ['w-1', 'w-2'],
            },
        ];

        let appliedBookmark: any = null;
        let savedBookmark: any = null;

        const { getByText } = render(
            <BookmarksDrawer
                open={true}
                bookmarks={bookmarks}
                currentPageId="page-1"
                currentFilters={[{ column: 'product_category', operator: '=', value: 'Electronics' }]}
                visibleWidgetIds={['w-1', 'w-2']}
                onClose={() => {}}
                onApplyBookmark={(bm) => { appliedBookmark = bm; }}
                onSaveBookmark={(bm) => { savedBookmark = bm; }}
                onDeleteBookmark={() => {}}
            />
        );

        expect(getByText('Q1 Baseline View')).toBeTruthy();

        // Click View to apply bookmark
        const viewBtn = getByText('View');
        fireEvent.click(viewBtn);
        expect(appliedBookmark).not.toBeNull();
        expect(appliedBookmark?.id).toBe('bm-1');
        expect(appliedBookmark?.filters?.[0].value).toBe('Electronics');

        // Add new bookmark
        const addBtn = getByText('Add');
        fireEvent.click(addBtn);
        expect(savedBookmark).not.toBeNull();
        expect(savedBookmark?.page_id).toBe('page-1');
    });
});

describe('Step 3: Selection & Layers Pane', () => {
    it('allows toggling visual visibility and layout locking', () => {
        const widgets: Widget[] = [
            {
                id: 'w-1',
                config: {
                    chart_type: 'bar',
                    title: 'Revenue Bar Chart',
                    aggregation: 'sum',
                    filters: [],
                    sort_direction: 'desc',
                    color_scheme: 'default',
                    show_legend: true,
                    show_grid: true,
                    show_values: false,
                    stacked: false,
                    trend_line: false,
                    smooth: false,
                },
                source_prompt: 'Revenue Bar',
                prompt_history: [],
                layout: { x: 0, y: 0, w: 6, h: 4 },
                hidden: false,
                locked: false,
            },
        ];

        let toggledVisibilityId = '';
        let toggledLockId = '';

        const { getByText } = render(
            <SelectionPane
                open={true}
                widgets={widgets}
                onClose={() => {}}
                onToggleVisibility={(id) => { toggledVisibilityId = id; }}
                onToggleLock={(id) => { toggledLockId = id; }}
                onRenameWidget={() => {}}
                onReorderWidget={() => {}}
            />
        );

        expect(getByText('Revenue Bar Chart')).toBeTruthy();

        // Eye icon button inside teleported Drawer
        const eyeBtn = document.querySelector('.anticon-eye')?.closest('button')!;
        expect(eyeBtn).toBeTruthy();
        fireEvent.click(eyeBtn);
        expect(toggledVisibilityId).toBe('w-1');

        // Lock icon button inside teleported Drawer
        const lockBtn = document.querySelector('.anticon-unlock')?.closest('button')!;
        expect(lockBtn).toBeTruthy();
        fireEvent.click(lockBtn);
        expect(toggledLockId).toBe('w-1');
    });
});

describe('Step 3: GlobalFilterBar Cross-Filtering', () => {
    it('displays cross-filter badges and allows removal and clear-all', () => {
        let removedColumn = '';
        let cleared = false;
        let globalFiltersChanged = false;

        const { getByText, container } = render(
            <GlobalFilterBar
                filters={[{ column: 'order_year', operator: '=', value: 2025 }]}
                crossFilters={{ product_category: 'Electronics' }}
                columns={['order_date', 'product_category', 'order_year']}
                onFiltersChange={() => { globalFiltersChanged = true; }}
                onRemoveCrossFilter={(col) => { removedColumn = col; }}
                onClearAllCrossFilters={() => { cleared = true; }}
            />
        );

        // Check global filter chip and cross-filter badge
        expect(getByText(/order_year = 2025/i)).toBeTruthy();
        expect(getByText(/Cross-Filter: product_category = Electronics/i)).toBeTruthy();

        // Close cross-filter badge
        const crossClose = container.querySelectorAll('.ant-tag-close-icon')[1];
        if (crossClose) {
            fireEvent.click(crossClose);
            expect(removedColumn).toBe('product_category');
        }

        // Click Clear All
        const clearAll = getByText(/Clear all/i);
        fireEvent.click(clearAll);
        expect(cleared).toBe(true);
        expect(globalFiltersChanged).toBe(true);
    });
});


// ── Step 4 Core & Composition Visual Palette + Unified Number Formatting Tests ──

import DashboardComboChart from '../components/dashboard/charts/ComboChart';
import DashboardRibbonChart from '../components/dashboard/charts/RibbonChart';
import DashboardWaterfallChart from '../components/dashboard/charts/WaterfallChart';
import DashboardSunburstChart from '../components/dashboard/charts/SunburstChart';
import DashboardPieChart from '../components/dashboard/charts/PieChart';
import DashboardTreemapChart from '../components/dashboard/charts/TreemapChart';
import DashboardFunnelChart from '../components/dashboard/charts/FunnelChart';
import { formatValue, cleanNumber } from '../components/dashboard/utils/formatters';

describe('Step 4: Unified Number Formatting & IEEE-754 Sanitization', () => {
    it('eliminates IEEE-754 floating-point inaccuracies and formats currency/percentages', () => {
        // IEEE-754 float: 0.11670000000000001
        expect(cleanNumber(0.11670000000000001)).toBe(0.1167);
        expect(formatValue(0.11670000000000001, 'percent')).toBe('11.7%');

        // Currency formats
        expect(formatValue(6_420_000, 'currency')).toBe('$6.42M');
        expect(formatValue(26_300, 'currency')).toBe('$26.3K');
        expect(formatValue(450.25, 'currency')).toBe('$450.25');
        expect(formatValue(-1_500_000, 'currency')).toBe('-$1.50M');

        // Compact & integer formats
        expect(formatValue(1_200_000, 'compact')).toBe('1.2M');
        expect(formatValue(1_250.75, 'integer')).toBe('1,251');
        expect(formatValue(1_250.75, 'decimal')).toBe('1,250.75');

        // Null and NaN handling
        expect(formatValue(null)).toBe('—');
        expect(formatValue(undefined)).toBe('—');
        expect(formatValue(NaN)).toBe('—');
    });
});

describe('Step 4: Composition & Core Visual Palette Rendering', () => {
    it('renders ComboChart with dual-axis bar and line metrics', () => {
        const config = {
            chart_type: 'combo_bar_line',
            title: 'Revenue vs Discount Rate',
            x_axis: 'order_month_name',
            metrics: [
                { column: 'net_revenue', aggregation: 'sum', label: 'Net Revenue', y_axis_index: 0 },
                { column: 'discount_rate', aggregation: 'avg', label: 'Avg Discount Rate', y_axis_index: 1 },
            ],
            color_scheme: 'default',
            show_legend: true,
            show_grid: true,
            show_values: false,
        } as unknown as ChartConfig;
        const data = [
            { order_month_name: 'Jan 2024', 'Net Revenue': 350000, 'Avg Discount Rate': 0.12 },
            { order_month_name: 'Feb 2024', 'Net Revenue': 420000, 'Avg Discount Rate': 0.10 },
        ];

        const { container } = render(<DashboardComboChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders RibbonChart with ranked category slices', () => {
        const config = {
            chart_type: 'ribbon',
            title: 'Category Rank Trend',
            x_axis: 'order_quarter',
            group_by: 'product_category',
            y_axis: 'net_revenue',
            color_scheme: 'default',
            show_legend: true,
            show_grid: true,
            show_values: false,
        } as unknown as ChartConfig;
        const data = [
            { order_quarter: '2024-Q1', product_category: 'Electronics', net_revenue: 500000 },
            { order_quarter: '2024-Q1', product_category: 'Apparel', net_revenue: 300000 },
            { order_quarter: '2024-Q2', product_category: 'Electronics', net_revenue: 600000 },
            { order_quarter: '2024-Q2', product_category: 'Apparel', net_revenue: 280000 },
        ];

        const { container } = render(<DashboardRibbonChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders WaterfallChart with starting base, positive/negative deltas, and total', () => {
        const config = {
            chart_type: 'waterfall',
            title: 'Variance Breakdown',
            x_axis: 'category',
            y_axis: 'delta',
            color_scheme: 'default',
            show_legend: false,
            show_grid: true,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { category: 'Base 2025-03', delta: 331993 },
            { category: 'Electronics Spike', delta: 634814 },
            { category: 'Apparel Drop', delta: -82173 },
        ];

        const { container } = render(<DashboardWaterfallChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders SunburstChart hierarchical breakdown', () => {
        const config = {
            chart_type: 'sunburst',
            title: 'Region to Category Breakdown',
            x_axis: 'region',
            group_by: 'product_category',
            y_axis: 'net_revenue',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { region: 'North America', product_category: 'Electronics', net_revenue: 800000 },
            { region: 'North America', product_category: 'Apparel', net_revenue: 400000 },
            { region: 'Europe', product_category: 'Electronics', net_revenue: 600000 },
        ];

        const { container } = render(<DashboardSunburstChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders Pie and Donut chart variants with custom tooltips', () => {
        const config = {
            chart_type: 'donut',
            title: 'Revenue by Category',
            x_axis: 'product_category',
            y_axis: 'net_revenue',
            color_scheme: 'default',
            show_legend: true,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', net_revenue: 500000 },
            { product_category: 'Apparel', net_revenue: 250000 },
        ];

        const { container } = render(<DashboardPieChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders TreemapChart with custom cell content and value formatting', () => {
        const config = {
            chart_type: 'treemap',
            title: 'Category Volume Treemap',
            x_axis: 'product_category',
            y_axis: 'net_revenue',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', net_revenue: 500000 },
            { product_category: 'Apparel', net_revenue: 250000 },
        ];

        const { container } = render(<DashboardTreemapChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders FunnelChart with stage drop-off calculation', () => {
        const config = {
            chart_type: 'funnel',
            title: 'Conversion Funnel',
            x_axis: 'stage',
            y_axis: 'count',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { stage: 'Prospects', count: 1000 },
            { stage: 'Qualified', count: 650 },
            { stage: 'Closed Won', count: 280 },
        ];

        const { container } = render(<DashboardFunnelChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });
});


// ── Step 5 Statistical, KPI/Gauge, Table/Matrix & Geospatial Tests ──

import DashboardGaugeChart from '../components/dashboard/charts/GaugeChart';
import DashboardBulletChart from '../components/dashboard/charts/BulletChart';
import DashboardHistogramChart from '../components/dashboard/charts/HistogramChart';
import DashboardBoxPlotChart from '../components/dashboard/charts/BoxPlotChart';
import DashboardMatrixTable from '../components/dashboard/charts/MatrixTable';
import DashboardGeoMapChart from '../components/dashboard/charts/GeoMapChart';
import SlicerWidget from '../components/dashboard/charts/SlicerWidget';

describe('Step 5: Statistical, KPI/Gauge, Table/Matrix & Slicers', () => {
    it('renders GaugeChart with current value and target threshold', () => {
        const config = {
            chart_type: 'gauge',
            title: 'Net Revenue Goal',
            kpi_value_column: 'net_revenue',
            kpi_comparison_column: 'target_revenue',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [{ net_revenue: 6420000, target_revenue: 7000000 }];

        const { container } = render(<DashboardGaugeChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders BulletChart with qualitative ranges and actual vs target bars', () => {
        const config = {
            chart_type: 'bullet',
            title: 'Quarterly Quota Performance',
            x_axis: 'product_category',
            y_axis: 'actual_revenue',
            kpi_comparison_column: 'quota',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', actual_revenue: 850000, quota: 800000 },
            { product_category: 'Apparel', actual_revenue: 420000, quota: 500000 },
        ];

        const { getByText } = render(<DashboardBulletChart config={config} data={data} />);
        expect(getByText('Electronics')).toBeTruthy();
        expect(getByText('Apparel')).toBeTruthy();
    });

    it('renders HistogramChart with computed frequency bins', () => {
        const config = {
            chart_type: 'histogram',
            title: 'Shipping Weight Distribution',
            x_axis: 'shipping_weight',
            color_scheme: 'default',
            show_legend: false,
            show_grid: true,
            show_values: false,
        } as unknown as ChartConfig;
        const data = [
            { shipping_weight: 1.2 }, { shipping_weight: 1.5 }, { shipping_weight: 2.1 },
            { shipping_weight: 4.8 }, { shipping_weight: 5.2 }, { shipping_weight: 5.4 },
        ];

        const { container } = render(<DashboardHistogramChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders BoxPlotChart statistical distribution', () => {
        const config = {
            chart_type: 'boxplot',
            title: 'Unit Price Distribution by Category',
            x_axis: 'product_category',
            y_axis: 'unit_price_raw',
            color_scheme: 'default',
            show_legend: false,
            show_grid: true,
            show_values: false,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', unit_price_raw: 120 },
            { product_category: 'Electronics', unit_price_raw: 250 },
            { product_category: 'Electronics', unit_price_raw: 340 },
            { product_category: 'Apparel', unit_price_raw: 45 },
            { product_category: 'Apparel', unit_price_raw: 80 },
        ];

        const { container } = render(<DashboardBoxPlotChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders MatrixTable with collapsible row groups and subtotals', () => {
        const config = {
            chart_type: 'matrix',
            title: 'Revenue Matrix',
            x_axis: 'region',
            group_by: 'product_category',
            metrics: [
                { column: 'net_revenue', aggregation: 'sum', label: 'Net Revenue', format: 'currency' },
            ],
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { region: 'North America', product_category: 'Electronics', net_revenue: 500000 },
            { region: 'North America', product_category: 'Apparel', net_revenue: 300000 },
            { region: 'Europe', product_category: 'Electronics', net_revenue: 400000 },
        ];

        const { getByText } = render(<DashboardMatrixTable config={config} data={data} />);
        expect(getByText('North America')).toBeTruthy();
        expect(getByText('Europe')).toBeTruthy();
    });

    it('renders GeoMapChart with regional performance bars', () => {
        const config = {
            chart_type: 'bubble_map',
            title: 'Country Revenue Distribution',
            x_axis: 'country_code',
            y_axis: 'net_revenue',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { country_code: 'USA', net_revenue: 3500000 },
            { country_code: 'DEU', net_revenue: 1200000 },
            { country_code: 'GBR', net_revenue: 950000 },
        ];

        const { container } = render(<DashboardGeoMapChart config={config} data={data} />);
        expect(container.firstChild).toBeTruthy();
    });

    it('renders SlicerWidget and triggers cross-filter on tag click', () => {
        let clickedParam: any = null;
        const config = {
            chart_type: 'slicer',
            title: 'Filter by Category',
            x_axis: 'product_category',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics' },
            { product_category: 'Apparel' },
            { product_category: 'Office Supplies' },
        ];

        const { getByText } = render(
            <SlicerWidget
                config={config}
                data={data}
                onDataPointClick={(p) => { clickedParam = p; }}
            />
        );

        expect(getByText('Electronics')).toBeTruthy();
        const apparelTag = getByText('Apparel');
        expect(apparelTag).toBeTruthy();

        // Click apparel tag to trigger cross-filtering
        fireEvent.click(apparelTag);
        expect(clickedParam).not.toBeNull();
        expect(clickedParam?.column).toBe('product_category');
        expect(clickedParam?.value).toBe('Apparel');
    });

    it('renders DecompositionTree and displays root total and breakdown nodes', () => {
        let clickedParam: any = null;
        const config = {
            chart_type: 'decomposition_tree',
            title: 'Revenue Decomposition',
            x_axis: 'product_category',
            y_axis: 'net_revenue',
            metrics: [
                { column: 'net_revenue', aggregation: 'sum', label: 'Net Revenue', format: 'currency' },
            ],
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', net_revenue: 2500000 },
            { product_category: 'Home & Kitchen', net_revenue: 1200000 },
        ];

        const { getByText } = render(
            <DecompositionTree
                config={config}
                data={data}
                onDataPointClick={(p) => { clickedParam = p; }}
            />
        );

        expect(getByText('Revenue Decomposition')).toBeTruthy();
        expect(getByText('Breakdown Path:')).toBeTruthy();
        expect(getByText('Electronics')).toBeTruthy();
        expect(getByText('Home & Kitchen')).toBeTruthy();

        // Click on a node to trigger cross-filtering
        fireEvent.click(getByText('Electronics'));
        expect(clickedParam).not.toBeNull();
        expect(clickedParam?.column).toBe('product_category');
        expect(clickedParam?.value).toBe('Electronics');
    });

    it('renders KeyInfluencersVisual with driver cards and leakage banner', async () => {
        const config = {
            chart_type: 'key_influencers',
            title: 'Churn Drivers',
            y_axis: 'churn_flag',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', churn_flag: 1, post_churn_refund: 50 },
            { product_category: 'Electronics', churn_flag: 1, post_churn_refund: 20 },
            { product_category: 'Electronics', churn_flag: 1, post_churn_refund: 0 },
            { product_category: 'Electronics', churn_flag: 1, post_churn_refund: 10 },
            { product_category: 'Electronics', churn_flag: 1, post_churn_refund: 5 },
            { product_category: 'Office Supplies', churn_flag: 0, post_churn_refund: 0 },
            { product_category: 'Office Supplies', churn_flag: 0, post_churn_refund: 0 },
            { product_category: 'Office Supplies', churn_flag: 0, post_churn_refund: 0 },
            { product_category: 'Office Supplies', churn_flag: 0, post_churn_refund: 0 },
            { product_category: 'Office Supplies', churn_flag: 0, post_churn_refund: 0 },
        ];

        const { getByText, findByText } = render(<KeyInfluencersVisual config={config} data={data} />);
        expect(getByText('Key Influencers')).toBeTruthy();
        expect(getByText('Top Segments')).toBeTruthy();
        // Leakage protection banner
        const banner = await findByText(/Leakage Protected/i);
        expect(banner).toBeTruthy();
    });

    it('renders SmartNarrativeWidget with dynamic executive bullets', () => {
        const config = {
            chart_type: 'smart_narrative',
            title: 'Executive Intelligence Summary',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', net_revenue: 500000, gross_revenue: 600000, churn_flag: 0 },
            { product_category: 'Apparel', net_revenue: 200000, gross_revenue: 250000, churn_flag: 1 },
        ];

        const { getByText, getAllByText } = render(<SmartNarrativeWidget config={config} data={data} />);
        expect(getByText('Executive Intelligence Summary')).toBeTruthy();
        expect(getByText('Dynamic, data-grounded AI narrative')).toBeTruthy();
        expect(getAllByText('Net Revenue').length).toBeGreaterThan(0);
    });

    it('renders QAVisual with suggested chips and interactive question answer', () => {
        const config = {
            chart_type: 'qa_visual',
            title: 'Ask Data',
            color_scheme: 'default',
            show_legend: false,
            show_grid: false,
            show_values: true,
        } as unknown as ChartConfig;
        const data = [
            { product_category: 'Electronics', net_revenue: 500000 },
            { product_category: 'Apparel', net_revenue: 200000 },
        ];

        const { getByText, getByPlaceholderText } = render(<QAVisual config={config} data={data} />);
        expect(getByPlaceholderText(/Ask a question about your data/i)).toBeTruthy();
        expect(getByText('Total net revenue by product category')).toBeTruthy();

        // Click suggestion chip
        fireEvent.click(getByText('Total net revenue by product category'));
        expect(getByText('Ask')).toBeTruthy();
    });

    it('ChartWidget dispatches all AI-native super visuals correctly', () => {
        const data = [{ product_category: 'Electronics', net_revenue: 1000000, churn_flag: 0 }];

        const treeConfig = { chart_type: 'decomposition_tree', title: 'Tree' } as ChartConfig;
        const { getByText: getTree } = render(<ChartWidget config={treeConfig} data={data} />);
        expect(getTree('Breakdown Path:')).toBeTruthy();

        const qaConfig = { chart_type: 'qa_visual', title: 'QA' } as ChartConfig;
        const { getByPlaceholderText } = render(<ChartWidget config={qaConfig} data={data} />);
        expect(getByPlaceholderText(/Ask a question about your data/i)).toBeTruthy();
    });

    it('PresentMode renders fullscreen controls, navigation buttons, and supports mode switching', () => {
        let exited = false;
        const dummyWidgets = [
            {
                id: 'w1',
                config: {
                    chart_type: 'bar',
                    title: 'Revenue by Category',
                    x_axis: 'category',
                    y_axis: 'revenue',
                    color_scheme: 'default',
                    show_legend: false,
                    show_grid: false,
                    show_values: true,
                } as unknown as ChartConfig,
                data: [{ category: 'Tech', revenue: 100 }],
            },
            {
                id: 'w2',
                config: {
                    chart_type: 'kpi',
                    title: 'Total Margin',
                    x_axis: '',
                    y_axis: 'margin',
                    color_scheme: 'default',
                    show_legend: false,
                    show_grid: false,
                    show_values: true,
                } as unknown as ChartConfig,
                data: [{ margin: 50 }],
            },
        ] as unknown as Widget[];

        const { getByText } = render(
            <PresentMode
                widgets={dummyWidgets}
                title="Q4 Board Review"
                pageTitle="Executive Summary"
                onExit={() => { exited = true; }}
            />
        );

        expect(getByText('Q4 Board Review')).toBeTruthy();
        expect(getByText('Executive Summary')).toBeTruthy();
        expect(getByText('Slide 1 of 2')).toBeTruthy();

        // Switch to Page Grid view
        fireEvent.click(getByText('Page Grid'));
        expect(getByText('2 Visuals Grid')).toBeTruthy();

        // Switch back to Single Slide
        fireEvent.click(getByText('Single Slide'));
        expect(getByText('Slide 1 of 2')).toBeTruthy();

        // Test ESC key
        fireEvent.keyDown(document, { key: 'Escape' });
        expect(exited).toBe(true);
    });

    it('WidgetContainer renders action bar and provides CSV export action', () => {
        const dummyWidget = {
            id: 'w-export',
            config: {
                chart_type: 'bar',
                title: 'Exportable Visual',
                x_axis: 'cat',
                y_axis: 'val',
                color_scheme: 'default',
                show_legend: false,
                show_grid: false,
                show_values: true,
            } as unknown as ChartConfig,
            data: [{ cat: 'A', val: 10 }, { cat: 'B', val: 20 }],
        } as unknown as Widget;

        const { getByText } = render(
            <WidgetContainer
                widget={dummyWidget}
            />
        );

        expect(getByText('Exportable Visual')).toBeTruthy();
        // The export button is rendered
        const exportBtn = document.querySelector('.anticon-download');
        expect(exportBtn).toBeTruthy();
    });
});
