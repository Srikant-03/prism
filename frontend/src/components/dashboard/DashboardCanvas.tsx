/**
 * DashboardCanvas — react-grid-layout responsive grid for dashboard widgets.
 */
import React from 'react';
import RGL, { WidthProvider } from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import type { Widget } from '../../types/dashboard';
import WidgetContainer from './WidgetContainer';

const GridLayout = WidthProvider(RGL as any);

interface LayoutItem {
    i: string;
    x: number;
    y: number;
    w: number;
    h: number;
    minW?: number;
    minH?: number;
}

interface Props {
    widgets: Widget[];
    onLayoutChange?: (layout: LayoutItem[]) => void;
    onEditWidget?: (id: string) => void;
    onDuplicateWidget?: (id: string) => void;
    onDeleteWidget?: (id: string) => void;
    onFullscreenWidget?: (id: string) => void;
    onTitleChange?: (id: string, title: string) => void;
}

const DashboardCanvas: React.FC<Props> = ({
    widgets, onLayoutChange, onEditWidget, onDuplicateWidget,
    onDeleteWidget, onFullscreenWidget, onTitleChange,
}) => {
    const layout: LayoutItem[] = widgets.map(w => ({
        i: w.id,
        x: w.layout.x,
        y: w.layout.y,
        w: w.layout.w,
        h: w.layout.h,
        minW: 3,
        minH: 2,
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
                {widgets.map(widget => (
                    <div key={widget.id} style={{ overflow: 'hidden' }}>
                        <WidgetContainer
                            widget={widget}
                            onEdit={onEditWidget}
                            onDuplicate={onDuplicateWidget}
                            onDelete={onDeleteWidget}
                            onFullscreen={onFullscreenWidget}
                            onTitleChange={onTitleChange}
                        />
                    </div>
                ))}
            </GridLayout>
        </div>
    );
};

export default DashboardCanvas;
