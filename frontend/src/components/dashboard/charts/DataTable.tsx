/**
 * DataTable — AG Grid lite wrapper for tabular query results.
 */
import React, { useMemo } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { ChartConfig } from '../../types/dashboard';

interface Props { config: ChartConfig; data: Record<string, any>[]; }

const DashboardDataTable: React.FC<Props> = ({ config, data }) => {
    const columnDefs = useMemo(() => {
        if (data.length === 0) return [];
        return Object.keys(data[0]).map(key => ({
            field: key,
            headerName: key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
            sortable: true,
            filter: true,
            resizable: true,
            flex: 1,
            minWidth: 100,
        }));
    }, [data]);

    return (
        <div style={{ width: '100%', height: '100%' }} className="ag-theme-alpine-dark">
            <AgGridReact
                rowData={data}
                columnDefs={columnDefs}
                defaultColDef={{ resizable: true, sortable: true }}
                pagination={data.length > 50}
                paginationPageSize={50}
                animateRows
                domLayout="normal"
            />
        </div>
    );
};

export default DashboardDataTable;
