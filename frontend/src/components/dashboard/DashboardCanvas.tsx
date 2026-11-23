/**
 * DashboardCanvas — react-grid-layout responsive grid for dashboard widgets.
 * Supports layout locks, widget hiding, selection state, and cross-filtering.
 */
import React from 'react';
import RGL, { WidthProvider } from 'react-grid-layout/legacy';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import type { Widget } from '../../types/dashboard';
import WidgetContainer from './WidgetContainer';

const GridLayout = WidthProvider(RGL) as any;

interface LayoutItem {
    i: string;
    x: number;
    y: number;
    w: number;
    h: number;
    minW?: number;
    minH?: number;
    static?: boolean;
}

interface Props {
    widgets: Widget[];
    editingWidgetId?: string | null;
    selectedWidgetId?: string | null;
    onLayoutChange?: (layout: LayoutItem[]) => void;
    onEditWidget?: (id: string) => void;
    onSelectWidget?: (id: string) => void;
    onDuplicateWidget?: (id: string) => void;
    onDeleteWidget?: (id: string) => void;
    onFullscreenWidget?: (id: string) => void;
    onTitleChange?: (id: string, title: string) => void;
    onFormatWidget?: (id: string) => void;
    onDrillUp?: (id: string) => void;
    onDrillDown?: (id: string) => void;
    onExplainWidget?: (widget: Widget) => void;
    onCrossFilter?: (column: string, value: any) => void;
}

const DashboardCanvas: React.FC<Props> = ({
    widgets,
    editingWidgetId,
    selectedWidgetId,
    onLayoutChange,
    onEditWidget,
    onSelectWidget,
    onDuplicateWidget,
    onDeleteWidget,
    onFullscreenWidget,
    onTitleChange,
    onFormatWidget,
    onDrillUp,
    onDrillDown,
    onExplainWidget,
    onCrossFilter,
}) => {
    // Filter out hidden widgets
    const visibleWidgets = widgets.filter(w => !w.hidden);

    const layout: LayoutItem[] = visibleWidgets.map(w => ({
        i: w.id,
        x: w.layout.x,
        y: w.layout.y,
        w: w.layout.w,
        h: w.layout.h,
        minW: 3,
        minH: 2,
        static: Boolean(w.locked),
    }));

    return (
        <div style={{ position: 'relative' }}>
            <GridLayout
                className="dashboard-grid-layout"
                layout={layout}
                cols={12}
                rowHeight={80}
                draggableHandle=".widget-drag-handle"
                onLayoutChange={(newLayout: any) => onLayoutChange?.(newLayout)}
                margin={[16, 16]}
                containerPadding={[0, 0]}
                isResizable
                isDraggable
                useCSSTransforms
            >
                {visibleWidgets.map(widget => (
                    <div key={widget.id} style={{ overflow: 'hidden' }}>
                        <WidgetContainer
                            widget={widget}
                            isEditing={widget.id === editingWidgetId}
                            isSelected={widget.id === selectedWidgetId}
                            onEdit={onEditWidget}
                            onSelect={onSelectWidget}
                            onDuplicate={onDuplicateWidget}
                            onDelete={onDeleteWidget}
                            onFullscreen={onFullscreenWidget}
                            onTitleChange={onTitleChange}
                            onFormat={onFormatWidget}
                            onDrillUp={onDrillUp}
                            onDrillDown={onDrillDown}
                            onExplain={onExplainWidget}
                            onCrossFilter={onCrossFilter}
                        />
                    </div>
                ))}
            </GridLayout>
        </div>
    );
};

export default DashboardCanvas;
