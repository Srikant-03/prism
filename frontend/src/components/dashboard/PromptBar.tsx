import React, { useState, useRef, useEffect } from 'react';
import { Input, Button, Tag } from 'antd';
import {
  UndoOutlined, HistoryOutlined,
  BulbOutlined, ReloadOutlined, PlusOutlined, BookOutlined,
} from '@ant-design/icons';
import { motion, AnimatePresence } from 'framer-motion';

interface PromptEntry {
  prompt: string;
  timestamp: number;
  chartTypeBefore?: string;
  chartTypeAfter?: string;
}

interface Props {
  onSubmit: (prompt: string) => void;
  loading?: boolean;
  history?: PromptEntry[];
  suggestions?: string[];
  onUndo?: () => void;
  canUndo?: boolean;
  clarification?: string;
  editingTitle?: string;
  onCancelEdit?: () => void;
  onSaveToSlide?: () => void;
}

export const PromptBar: React.FC<Props> = ({
  onSubmit,
  loading,
  history = [],
  suggestions = [],
  onUndo,
  canUndo,
  clarification,
  editingTitle,
  onCancelEdit,
  onSaveToSlide,
}) => {
  const [prompt, setPrompt] = useState(
    'Compare gross_revenue vs net_revenue across product_category where churn_flag is 0'
  );
  const [showHistory, setShowHistory] = useState(false);
  const [surgeActive, setSurgeActive] = useState(false);
  const inputRef = useRef<any>(null);

  useEffect(() => {
    if (!loading) inputRef.current?.focus();
  }, [loading, editingTitle]);

  const handleSubmit = () => {
    if (!prompt.trim() || loading) return;
    setSurgeActive(true);
    setTimeout(() => setSurgeActive(false), 900);
    onSubmit(prompt.trim());
  };

  return (
    <div style={{ width: '100%', marginBottom: 20 }}>
      {/* Edit Mode Banner */}
      {editingTitle && (
        <div
          style={{
            marginBottom: 8,
            display: 'inline-flex',
            alignItems: 'center',
            gap: 8,
            padding: '4px 12px',
            borderRadius: 16,
            background: 'rgba(59,130,246,0.15)',
            border: '1px solid rgba(59,130,246,0.4)',
            color: '#60a5fa',
            fontSize: 12,
            fontWeight: 500,
          }}
        >
          <span>✏️ Editing Widget: <strong>{editingTitle}</strong></span>
          <Button
            size="small"
            type="text"
            onClick={onCancelEdit}
            style={{ color: '#93c5fd', padding: '0 4px', height: 'auto', fontSize: 11 }}
          >
            Cancel
          </Button>
        </div>
      )}

      {/* Clarification banner */}
      {clarification && (
        <div
          style={{
            marginBottom: 10,
            padding: '10px 16px',
            borderRadius: 10,
            background: 'rgba(251,191,36,0.1)',
            border: '1px solid rgba(251,191,36,0.3)',
            color: '#fbbf24',
            fontSize: 13,
          }}
        >
          🤔 {clarification}
        </div>
      )}

      {/* Iridescent Luminous Border Wrapper */}
      <div className="iridescent-border-wrapper" style={{ position: 'relative' }}>
        {/* On-Submit Light Surge Wave */}
        <AnimatePresence>
          {surgeActive && (
            <motion.div
              initial={{ scale: 0.98, opacity: 0.9, filter: 'blur(10px)' }}
              animate={{ scale: 1.05, opacity: 0, filter: 'blur(0px)' }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.8, ease: [0.16, 1, 0.24, 1] }}
              style={{
                position: 'absolute',
                inset: -4,
                borderRadius: 22,
                background:
                  'radial-gradient(circle, rgba(244, 63, 94, 0.6) 0%, rgba(251, 191, 36, 0.5) 40%, rgba(34, 211, 238, 0.4) 70%, transparent 100%)',
                pointerEvents: 'none',
                zIndex: 3,
              }}
            />
          )}
        </AnimatePresence>

        <div
          className="iridescent-border-content"
          style={{
            padding: '12px 18px 10px',
            display: 'flex',
            flexDirection: 'column',
            gap: 10,
          }}
        >
          {/* Top Row: Diamond Icon + Prompt Input */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span
              style={{
                fontSize: 18,
                color: '#fbbf24',
                textShadow: '0 0 10px rgba(251, 191, 36, 0.6)',
                lineHeight: 1,
              }}
            >
              ✦
            </span>
            <Input
              ref={inputRef}
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              onPressEnter={handleSubmit}
              placeholder="Ask PRISM anything in natural language..."
              disabled={loading}
              variant="borderless"
              style={{
                color: '#f8fafc',
                fontSize: 14,
                fontFamily: "'Inter', sans-serif",
                padding: 0,
                flex: 1,
                background: 'transparent',
              }}
            />
          </div>

          {/* Bottom Row: Metadata Badges + Action Buttons */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: 8,
              paddingTop: 4,
              borderTop: '1px solid rgba(255, 255, 255, 0.06)',
            }}
          >
            {/* Left Metadata Status Pills */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 11, color: 'rgba(255, 255, 255, 0.5)' }}>
              <span
                style={{
                  padding: '2px 8px',
                  borderRadius: 10,
                  background: 'rgba(255, 255, 255, 0.05)',
                  border: '1px solid rgba(255, 255, 255, 0.08)',
                }}
              >
                SQL compiled in 14ms
              </span>
              <span>•</span>
              <span
                style={{
                  padding: '2px 8px',
                  borderRadius: 10,
                  background: 'rgba(255, 255, 255, 0.05)',
                  border: '1px solid rgba(255, 255, 255, 0.08)',
                }}
              >
                16 temporal buckets active
              </span>
              <span>•</span>
              <span
                style={{
                  padding: '2px 8px',
                  borderRadius: 10,
                  background: 'rgba(244, 63, 94, 0.12)',
                  border: '1px solid rgba(244, 63, 94, 0.25)',
                  color: '#fb7185',
                }}
              >
                Filter active: churn_flag = 0
              </span>
            </div>

            {/* Right Action Buttons */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              {canUndo && (
                <Button
                  size="small"
                  type="text"
                  icon={<UndoOutlined />}
                  onClick={onUndo}
                  style={{ color: '#94a3b8', fontSize: 12 }}
                >
                  Undo
                </Button>
              )}

              {history.length > 0 && (
                <Button
                  size="small"
                  type="text"
                  icon={<HistoryOutlined />}
                  onClick={() => setShowHistory(!showHistory)}
                  style={{ color: '#94a3b8', fontSize: 12 }}
                >
                  History ({history.length})
                </Button>
              )}

              <Button
                size="small"
                icon={<ReloadOutlined />}
                onClick={handleSubmit}
                loading={loading}
                style={{
                  background: 'rgba(255, 255, 255, 0.06)',
                  border: '1px solid rgba(255, 255, 255, 0.12)',
                  color: '#f8fafc',
                  borderRadius: 8,
                  fontSize: 12,
                }}
              >
                Regenerate
              </Button>

              <Button
                size="small"
                type="primary"
                icon={<PlusOutlined />}
                onClick={handleSubmit}
                loading={loading}
                style={{
                  background: 'linear-gradient(135deg, #f43f5e 0%, #fb923c 100%)',
                  border: 'none',
                  borderRadius: 8,
                  fontSize: 12,
                  fontWeight: 500,
                  boxShadow: '0 2px 10px rgba(244, 63, 94, 0.3)',
                }}
              >
                Add to Canvas +
              </Button>

              <Button
                size="small"
                icon={<BookOutlined />}
                onClick={onSaveToSlide}
                style={{
                  background: 'rgba(255, 255, 255, 0.06)',
                  border: '1px solid rgba(255, 255, 255, 0.12)',
                  color: '#f8fafc',
                  borderRadius: 8,
                  fontSize: 12,
                }}
              >
                Save to Slide
              </Button>
            </div>
          </div>
        </div>
      </div>

      {/* Suggested prompts pills below */}
      {suggestions.length > 0 && (
        <div style={{ marginTop: 8, display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
          <BulbOutlined style={{ color: '#fbbf24', fontSize: 12 }} />
          {suggestions.slice(0, 4).map((s) => (
            <Tag
              key={s}
              onClick={() => {
                setPrompt(s);
                inputRef.current?.focus();
              }}
              style={{
                cursor: 'pointer',
                background: 'rgba(255, 255, 255, 0.04)',
                border: '1px solid rgba(255, 255, 255, 0.08)',
                color: 'rgba(255, 255, 255, 0.75)',
                borderRadius: 14,
                padding: '2px 10px',
                fontSize: 11,
              }}
            >
              {s}
            </Tag>
          ))}
        </div>
      )}

      {/* Prompt History dropdown */}
      {showHistory && history.length > 0 && (
        <div
          style={{
            marginTop: 8,
            maxHeight: 180,
            overflowY: 'auto',
            background: 'rgba(15, 23, 42, 0.95)',
            backdropFilter: 'blur(16px)',
            borderRadius: 12,
            border: '1px solid rgba(255, 255, 255, 0.1)',
            padding: 8,
          }}
        >
          {history.slice().reverse().map((entry) => (
            <div
              key={entry.timestamp}
              onClick={() => {
                setPrompt(entry.prompt);
                setShowHistory(false);
              }}
              style={{
                padding: '6px 12px',
                cursor: 'pointer',
                borderRadius: 6,
                color: '#94a3b8',
                fontSize: 12,
                display: 'flex',
                justifyContent: 'space-between',
              }}
              onMouseEnter={(e) => (e.currentTarget.style.background = 'rgba(244, 63, 94, 0.1)')}
              onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}
            >
              <span style={{ flex: 1 }}>💬 {entry.prompt}</span>
              {entry.chartTypeAfter && (
                <span style={{ color: '#fb923c', fontSize: 11 }}>{entry.chartTypeAfter}</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default PromptBar;
