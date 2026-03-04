/**
 * App — Main application component.
 * Orchestrates the upload flow and conditionally renders appropriate UI.
 */

import React, { useState } from 'react';
import { ConfigProvider, theme, Alert, Button, Result, Tabs, FloatButton } from 'antd';
import { ReloadOutlined, CodeOutlined, TableOutlined, FileTextOutlined, AppstoreOutlined, RobotOutlined } from '@ant-design/icons';
import Layout from './components/common/Layout';
import FileUploader from './components/upload/FileUploader';
import UploadProgress from './components/upload/UploadProgress';
import SheetSelector from './components/upload/SheetSelector';
import MalformedViewer from './components/upload/MalformedViewer';
import MultiFileResolver from './components/upload/MultiFileResolver';
import DataPreview from './components/common/DataPreview';
import ProfileDashboard from './components/profiling/ProfileDashboard';
import QueryWorkbench from './components/sql/QueryWorkbench';
import ReportPanel from './components/reporting/ReportPanel';
import LiveDataGrid from './components/grid/LiveDataGrid';
import ChatSidebar from './components/chat/ChatSidebar';
import ThemeToggle from './components/common/ThemeToggle';
import OnboardingWalkthrough from './components/common/OnboardingWalkthrough';
import RelationshipGraph from './components/insights/RelationshipGraph';
import { ThemeProvider, useTheme } from './hooks/useTheme';
import { useUpload } from './hooks/useUpload';

const App: React.FC = () => {
  const {
    state,
    uploadPct,
    handleUpload,
    handleSelectSheets,
    handleConfirmMalformed,
    handleResolveMulti,
    reset,
  } = useUpload();
  const [chatOpen, setChatOpen] = useState(false);

  const handleChatAction = (action: any) => {
    if (action.type === 'sql') {
      // Could navigate to SQL tab and populate query
      console.log('SQL action:', action.payload);
    }
  };

  const renderContent = () => {
    switch (state.status) {
      case 'idle':
        return <FileUploader onUpload={handleUpload} />;

      case 'uploading':
        return (
          <UploadProgress
            uploadPct={uploadPct}
            stage="uploading"
            message="Uploading your file to the analysis engine..."
          />
        );

      case 'processing':
        return (
          <UploadProgress
            uploadPct={100}
            stage="parsing"
            progressPct={state.progress?.progress_pct}
            etaSeconds={state.progress?.eta_seconds}
            memoryMb={state.progress?.memory_usage_mb}
            message={state.progress?.message || 'Processing your data...'}
            bytesRead={state.progress?.bytes_read}
            totalBytes={state.progress?.total_bytes}
          />
        );

      case 'awaiting_sheet_selection':
        if (state.result?.metadata.sheets) {
          return (
            <SheetSelector
              sheets={state.result.metadata.sheets}
              onSelect={(indices) =>
                handleSelectSheets(state.result!.file_id, indices)
              }
            />
          );
        }
        return null;

      case 'awaiting_malformed_review':
        if (state.result?.malformed_report) {
          return (
            <MalformedViewer
              report={state.result.malformed_report}
              onAcceptBestEffort={() =>
                handleConfirmMalformed(state.result!.file_id, true)
              }
              onDropMalformed={() =>
                handleConfirmMalformed(state.result!.file_id, false, true)
              }
            />
          );
        }
        return null;

      case 'awaiting_schema_decision':
        if (state.multiResult?.schema_comparison) {
          const fileIds = Object.keys(state.multiResult.results);
          return (
            <MultiFileResolver
              comparison={state.multiResult.schema_comparison}
              fileIds={fileIds}
              onResolve={handleResolveMulti}
            />
          );
        }
        return null;

      case 'complete':
        if (state.result) {
          return (
            <div className="complete-view">
              {state.result.warnings.length > 0 && (
                <div className="warnings-container">
                  {state.result.warnings.map((w, i) => (
                    <Alert
                      key={i}
                      message={w}
                      type="warning"
                      showIcon
                      closable
                      className="warning-alert"
                    />
                  ))}
                </div>
              )}
              {state.result.profile && (
                <div style={{ marginBottom: 24 }}>
                  <ProfileDashboard profile={state.result.profile} fileId={state.result.file_id} />
                </div>
              )}
              <Tabs
                defaultActiveKey="preview"
                type="card"
                style={{ marginBottom: 20 }}
                items={[
                  {
                    key: 'preview',
                    label: <span><TableOutlined /> Data Preview</span>,
                    children: <DataPreview result={state.result} />,
                  },
                  {
                    key: 'grid',
                    label: <span><AppstoreOutlined /> Data Grid</span>,
                    children: (
                      <LiveDataGrid
                        data={state.result.preview_data || []}
                        columns={state.result.metadata.columns?.map((c: any) => c.name) || []}
                        columnTypes={state.result.metadata.columns?.map((c: any) => c.dtype || 'text') || []}
                        editable={false}
                      />
                    ),
                  },
                  {
                    key: 'sql',
                    label: <span><CodeOutlined /> SQL Query Engine</span>,
                    children: <QueryWorkbench />,
                  },
                  {
                    key: 'reporting',
                    label: <span><FileTextOutlined /> Reporting & Export</span>,
                    children: <ReportPanel fileId={state.result.file_id} />,
                  },
                  {
                    key: 'graph',
                    label: <span><AppstoreOutlined /> Relationship Graph</span>,
                    children: <RelationshipGraph data={null} loading={false} />,
                  },
                ]}
              />

              <div style={{ position: 'fixed', right: 24, bottom: 140, display: 'flex', flexDirection: 'column', gap: 12, zIndex: 1000 }}>
                <ThemeToggle />
                <FloatButton
                  icon={<RobotOutlined />}
                  type="primary"
                  tooltip="Talk to your data"
                  onClick={() => setChatOpen(true)}
                  style={{ position: 'static' }}
                />
              </div>

              <OnboardingWalkthrough />
              <ChatSidebar
                open={chatOpen}
                onClose={() => setChatOpen(false)}
                onAction={handleChatAction}
              />

              <div className="reset-container">
                <Button
                  icon={<ReloadOutlined />}
                  onClick={reset}
                  size="large"
                  className="reset-button"
                >
                  Upload Another File
                </Button>
              </div>
            </div>
          );
        }
        return (
          <Result
            status="success"
            title="Files Processed Successfully"
            subTitle="Your data has been ingested and is ready for analysis."
            extra={
              <Button icon={<ReloadOutlined />} onClick={reset} type="primary">
                Upload More Files
              </Button>
            }
          />
        );

      case 'error':
        return (
          <Result
            status="error"
            title="Ingestion Failed"
            subTitle={state.error || 'An unexpected error occurred.'}
            extra={
              <Button icon={<ReloadOutlined />} onClick={reset} type="primary">
                Try Again
              </Button>
            }
          />
        );

      default:
        return <FileUploader onUpload={handleUpload} />;
    }
  };

  const { isDark } = useTheme();

  const darkToken = {
    colorPrimary: '#6366f1',
    colorBgContainer: 'rgba(17, 24, 39, 0.7)',
    colorBgElevated: 'rgba(30, 41, 59, 0.9)',
    borderRadius: 12,
    fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    colorText: 'rgba(255, 255, 255, 0.88)',
    colorTextSecondary: 'rgba(255, 255, 255, 0.55)',
    colorBorder: 'rgba(255, 255, 255, 0.08)',
    colorBgLayout: 'transparent',
  };

  const lightToken = {
    colorPrimary: '#4f46e5',
    colorBgContainer: 'rgba(255, 255, 255, 0.92)',
    colorBgElevated: 'rgba(248, 250, 252, 0.98)',
    borderRadius: 12,
    fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    colorText: 'rgba(15, 23, 42, 0.88)',
    colorTextSecondary: 'rgba(71, 85, 105, 0.75)',
    colorBorder: 'rgba(15, 23, 42, 0.1)',
    colorBgLayout: 'transparent',
  };

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? theme.darkAlgorithm : theme.defaultAlgorithm,
        token: isDark ? darkToken : lightToken,
        components: {
          Card: {
            colorBgContainer: isDark ? 'rgba(17, 24, 39, 0.6)' : 'rgba(255, 255, 255, 0.85)',
            boxShadowTertiary: isDark ? '0 4px 24px rgba(0, 0, 0, 0.2)' : '0 4px 24px rgba(0, 0, 0, 0.06)',
          },
          Table: {
            colorBgContainer: 'transparent',
            headerBg: isDark ? 'rgba(99, 102, 241, 0.1)' : 'rgba(79, 70, 229, 0.06)',
            rowHoverBg: isDark ? 'rgba(99, 102, 241, 0.08)' : 'rgba(79, 70, 229, 0.04)',
          },
          Tag: { borderRadiusSM: 6 },
          Button: { borderRadius: 10 },
        },
      }}
    >
      <Layout>{renderContent()}</Layout>
    </ConfigProvider>
  );
};

/* Outer wrapper to provide ThemeContext before App uses useTheme */
const AppWithTheme: React.FC = () => (
  <ThemeProvider>
    <App />
  </ThemeProvider>
);

export default AppWithTheme;
