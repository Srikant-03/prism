/**
 * RecipeBrowser — Browse and apply reusable data processing recipes.
 */

import React, { useState, useCallback } from 'react';
import { Button, Space, Tag, Input, Modal, Steps, Row, Col } from 'antd';
import {
    BookOutlined, PlayCircleOutlined,
    SearchOutlined, StarOutlined, StarFilled,
    PlusOutlined,
} from '@ant-design/icons';

interface RecipeStep {
    action: string;
    column?: string;
    params: Record<string, any>;
    description: string;
}

interface Recipe {
    id: string;
    name: string;
    description: string;
    category: string;
    steps: RecipeStep[];
    tags: string[];
    author: string;
    starred: boolean;
    uses: number;
}

interface Props {
    fileId: string;
    columns: string[];
    onApply: (steps: RecipeStep[]) => void;
}

const BUILT_IN_RECIPES: Recipe[] = [
    {
        id: 'clean_basic',
        name: 'Basic Data Cleaning',
        description: 'Drop duplicates, fill numeric nulls with median, encode categoricals',
        category: 'Cleaning',
        steps: [
            { action: 'drop_duplicates', params: {}, description: 'Remove duplicate rows' },
            { action: 'fill_nulls', params: { strategy: 'median' }, description: 'Fill numeric nulls with median' },
            { action: 'encode_categorical', params: { method: 'label' }, description: 'Label-encode categoricals' },
        ],
        tags: ['cleaning', 'beginner', 'quick'],
        author: 'System',
        starred: false,
        uses: 1250,
    },
    {
        id: 'ml_prep',
        name: 'ML Feature Preparation',
        description: 'Standardize numerics, handle outliers, encode categoricals, log-transform skewed',
        category: 'ML Prep',
        steps: [
            { action: 'remove_outliers', params: { method: 'iqr', threshold: 1.5 }, description: 'Remove IQR outliers' },
            { action: 'fill_nulls', params: { strategy: 'mean' }, description: 'Mean imputation' },
            { action: 'standardize', params: { method: 'zscore' }, description: 'Z-score standardization' },
            { action: 'encode_categorical', params: { method: 'label' }, description: 'Label encoding' },
        ],
        tags: ['ml', 'intermediate', 'standardize'],
        author: 'System',
        starred: false,
        uses: 890,
    },
    {
        id: 'eda_prep',
        name: 'EDA Preparation',
        description: 'Fill nulls, normalize, create derived features',
        category: 'Analysis',
        steps: [
            { action: 'fill_nulls', params: { strategy: 'mode' }, description: 'Fill with mode' },
            { action: 'normalize', params: { method: 'minmax' }, description: 'Min-max normalize' },
        ],
        tags: ['eda', 'beginner', 'analysis'],
        author: 'System',
        starred: false,
        uses: 620,
    },
];

const RecipeBrowser: React.FC<Props> = ({ onApply }) => {
    const [recipes, setRecipes] = useState<Recipe[]>(BUILT_IN_RECIPES);
    const [search, setSearch] = useState('');
    const [selectedRecipe, setSelectedRecipe] = useState<Recipe | null>(null);
    const [previewOpen, setPreviewOpen] = useState(false);
    const [saveOpen, setSaveOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [newDesc, setNewDesc] = useState('');
    const [applying, setApplying] = useState(false);

    const filtered = recipes.filter(r =>
        r.name.toLowerCase().includes(search.toLowerCase()) ||
        r.tags.some(t => t.includes(search.toLowerCase()))
    );

    const toggleStar = useCallback((id: string) => {
        setRecipes(prev => prev.map(r =>
            r.id === id ? { ...r, starred: !r.starred } : r
        ));
    }, []);

    const applyRecipe = useCallback(async (recipe: Recipe) => {
        setApplying(true);
        try {
            onApply(recipe.steps);
            setRecipes(prev => prev.map(r =>
                r.id === recipe.id ? { ...r, uses: r.uses + 1 } : r
            ));
        } finally {
            setApplying(false);
            setPreviewOpen(false);
        }
    }, [onApply]);

    const saveAsRecipe = useCallback((steps: RecipeStep[]) => {
        if (!newName) return;
        const recipe: Recipe = {
            id: Date.now().toString(),
            name: newName,
            description: newDesc,
            category: 'Custom',
            steps,
            tags: ['custom'],
            author: 'You',
            starred: false,
            uses: 0,
        };
        setRecipes(prev => [recipe, ...prev]);
        setSaveOpen(false);
        setNewName('');
        setNewDesc('');
    }, [newName, newDesc]);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div className="glass-panel" style={{
                padding: '8px 12px', display: 'flex', justifyContent: 'space-between',
                alignItems: 'center', gap: 8,
            }}>
                <Space>
                    <BookOutlined style={{ color: '#6366f1', fontSize: 18 }} />
                    <strong>Data Recipes</strong>
                    <Tag color="purple">{recipes.length}</Tag>
                </Space>
                <Space>
                    <Input
                        size="small"
                        prefix={<SearchOutlined />}
                        placeholder="Search recipes..."
                        value={search}
                        onChange={e => setSearch(e.target.value)}
                        style={{ width: 180 }}
                        allowClear
                    />
                    <Button size="small" icon={<PlusOutlined />} onClick={() => setSaveOpen(true)}>
                        Save As Recipe
                    </Button>
                </Space>
            </div>

            {/* Recipe cards */}
            <Row gutter={[8, 8]}>
                {filtered.map(recipe => (
                    <Col xs={24} sm={12} md={8} key={recipe.id}>
                        <div className="glass-panel" style={{
                            padding: 12, height: '100%', cursor: 'pointer',
                            borderLeft: `3px solid ${recipe.category === 'Cleaning' ? '#52c41a' : recipe.category === 'ML Prep' ? '#6366f1' : '#f59e0b'}`,
                        }}
                            onClick={() => { setSelectedRecipe(recipe); setPreviewOpen(true); }}
                        >
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                                <strong style={{ fontSize: 13 }}>{recipe.name}</strong>
                                <span style={{ cursor: 'pointer' }}
                                    onClick={e => { e.stopPropagation(); toggleStar(recipe.id); }}>
                                    {recipe.starred ? <StarFilled style={{ color: '#fbbf24' }} /> : <StarOutlined />}
                                </span>
                            </div>
                            <div style={{ fontSize: 11, opacity: 0.6, marginBottom: 8 }}>
                                {recipe.description}
                            </div>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <Space size={4}>
                                    <Tag style={{ fontSize: 10 }}>{recipe.category}</Tag>
                                    <Tag style={{ fontSize: 10 }}>{recipe.steps.length} steps</Tag>
                                </Space>
                                <span style={{ fontSize: 10, opacity: 0.4 }}>{recipe.uses} uses</span>
                            </div>
                        </div>
                    </Col>
                ))}
            </Row>

            {/* Recipe Preview Modal */}
            <Modal
                title={selectedRecipe?.name}
                open={previewOpen}
                onCancel={() => setPreviewOpen(false)}
                width={500}
                footer={[
                    <Button key="cancel" onClick={() => setPreviewOpen(false)}>Cancel</Button>,
                    <Button key="apply" type="primary" icon={<PlayCircleOutlined />}
                        loading={applying}
                        onClick={() => selectedRecipe && applyRecipe(selectedRecipe)}>
                        Apply Recipe
                    </Button>,
                ]}
            >
                {selectedRecipe && (
                    <>
                        <p style={{ opacity: 0.6 }}>{selectedRecipe.description}</p>
                        <Steps
                            direction="vertical"
                            size="small"
                            current={-1}
                            items={selectedRecipe.steps.map((step) => ({
                                title: step.description,
                                description: `${step.action}${step.column ? ` → ${step.column}` : ''}`,
                            }))}
                        />
                        <div style={{ marginTop: 12, display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                            {selectedRecipe.tags.map(t => (
                                <Tag key={t} style={{ fontSize: 10 }}>{t}</Tag>
                            ))}
                        </div>
                    </>
                )}
            </Modal>

            {/* Save Recipe Modal */}
            <Modal
                title="Save Current Steps as Recipe"
                open={saveOpen}
                onCancel={() => setSaveOpen(false)}
                onOk={() => saveAsRecipe([])}
                okText="Save Recipe"
            >
                <Space orientation="vertical" style={{ width: '100%' }}>
                    <Input
                        value={newName}
                        onChange={e => setNewName(e.target.value)}
                        placeholder="Recipe name"
                    />
                    <Input.TextArea
                        value={newDesc}
                        onChange={e => setNewDesc(e.target.value)}
                        placeholder="Description"
                        rows={3}
                    />
                </Space>
            </Modal>
        </div>
    );
};

export default RecipeBrowser;

