/**
 * App — Main application component.
 * Orchestrates the upload flow and conditionally renders appropriate UI.
 */

import React, { useState, useEffect } from 'react';
import { ConfigProvider, theme, Alert, Button, Result, Tabs, FloatButton, Modal, message } from 'antd';
import { ReloadOutlined, CodeOutlined, TableOutlined, FileTextOutlined, AppstoreOutlined, RobotOutlined, BulbOutlined, DashboardOutlined, PlusOutlined } from '@ant-design/icons';
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
import HypothesisCards from './components/insights/HypothesisCards';
import DashboardPage from './components/dashboard/DashboardPage';
import { ThemeProvider, useTheme } from './hooks/useTheme';
import { fetchAuth, API_BASE } from './api/client';
import { useUpload } from './hooks/useUpload';
import { uploadFiles } from './api/ingestion';

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
  const [activeTabKey, setActiveTabKey] = useState('preview');
  const [pendingSqlQuery, setPendingSqlQuery] = useState<string | undefined>(undefined);

  // Secondary upload state
  const [secondaryModalOpen, setSecondaryModalOpen] = useState(false);
  const [secondaryUploading, setSecondaryUploading] = useState(false);

  // Graph and hypothesis state
  const [graphData, setGraphData] = useState<any>(null);
  const [graphLoading, setGraphLoading] = useState(false);
  const [graphThreshold, setGraphThreshold] = useState(0.3);
  const [hypotheses, setHypotheses] = useState<any[]>([]);

  // Fetch graph data when file is ready
  useEffect(() => {
    if (state.status === 'complete' && state.result?.file_id) {
      setGraphLoading(true);
      fetchAuth(`${API_BASE}/api/graph/${state.result.file_id}?threshold=${graphThreshold}`)
        .then(r => r.ok ? r.json() : null)
        .then(data => { if (data) setGraphData(data); })
        .catch(() => { })
        .finally(() => setGraphLoading(false));
    }
  }, [state.status, state.result?.file_id, graphThreshold]);

  // Fetch hypotheses when file is ready
  useEffect(() => {
    if (state.status === 'complete' && state.result?.file_id) {
      fetchAuth(`${API_BASE}/api/hypotheses/${state.result.file_id}`)
        .then(r => r.ok ? r.json() : null)
        .then(data => { if (data?.hypotheses) setHypotheses(data.hypotheses); })
        .catch(() => { });
    }
  }, [state.status, state.result?.file_id]);

  const handleChatAction = (action: any) => {
    if (action.type === 'sql' && action.payload) {
      // Navigate to SQL tab and populate the query
      setPendingSqlQuery(action.payload);
      setActiveTabKey('sql');
    } else if (action.type === 'grid') {
      setActiveTabKey('grid');
    } else if (action.type === 'navigate' && action.payload) {
      setActiveTabKey(action.payload);
    }
  };

  const handleSecondaryUpload = async (files: File[]) => {
    setSecondaryUploading(true);
    try {
      await uploadFiles(files);
      message.success('Datasets have been loaded into the SQL Engine automatically!');
      setSecondaryModalOpen(false);
    } catch (err: any) {
      message.error(err?.response?.data?.detail || err.message || 'Failed to upload additional files');
    } finally {
      setSecondaryUploading(false);
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
                activeKey={activeTabKey}
                onChange={setActiveTabKey}
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
                    children: <QueryWorkbench initialQuery={pendingSqlQuery} />,
                  },
                  {
                    key: 'reporting',
                    label: <span><FileTextOutlined /> Reporting & Export</span>,
                    children: <ReportPanel fileId={state.result.file_id} />,
                  },
                  {
                    key: 'graph',
                    label: <span><AppstoreOutlined /> Relationship Graph</span>,
                    children: (
                      <RelationshipGraph
                        data={graphData}
                        loading={graphLoading}
                        threshold={graphThreshold}
                        onThresholdChange={setGraphThreshold}
                      />
                    ),
                  },
                  {
                    key: 'hypotheses',
                    label: <span><BulbOutlined /> Hypotheses</span>,
                    children: (
                      <HypothesisCards
                        hypotheses={hypotheses}
                        onStatusChange={(id, status) => {
                          setHypotheses(prev =>
                            prev.map(h => h.id === id ? { ...h, status } : h)
                          );
                        }}
                        onAction={(action) => {
                          if (action.type === 'sql') {
                            setPendingSqlQuery(action.payload);
                            setActiveTabKey('sql');
                          }
                        }}
                      />
                    ),
                  },
                  {
                    key: 'dashboard',
                    label: <span><DashboardOutlined /> AI Dashboard</span>,
                    children: (
                      <DashboardPage
                        fileId={state.result.file_id}
                        columns={state.result.metadata.columns?.map((c: any) => c.name) || []}
                      />
                    ),
                  },
                ]}
              />

              <div style={{ position: 'fixed', right: 24, bottom: 140, display: 'flex', flexDirection: 'column', gap: 12, zIndex: 1000 }}>
                <ThemeToggle />
                <FloatButton
                  icon={<PlusOutlined />}
                  tooltip="Add Another Dataset"
                  onClick={() => setSecondaryModalOpen(true)}
                  style={{ position: 'static' }}
                />
                <FloatButton
                  icon={<RobotOutlined />}
                  type="primary"
                  tooltip="Talk to your data"
                  onClick={() => setChatOpen(true)}
                  style={{ position: 'static' }}
                />
              </div>

              <Modal
                title="Add Another Dataset"
                open={secondaryModalOpen}
                onCancel={() => !secondaryUploading && setSecondaryModalOpen(false)}
                footer={null}
                width={600}
                centered
              >
                <div style={{ padding: '20px 0' }}>
                  {secondaryUploading ? (
                    <UploadProgress
                      uploadPct={100}
                      stage="uploading"
                      message="Uploading and ingesting new datasets into SQL Engine..."
                    />
                  ) : (
                    <FileUploader onUpload={handleSecondaryUpload} disabled={secondaryUploading} />
                  )}
                </div>
              </Modal>

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

