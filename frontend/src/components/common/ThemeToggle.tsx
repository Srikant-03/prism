/**
 * ThemeToggle.tsx — Switch between Dark, Light, and Cyberpunk modes.
 */

import React, { useState, useEffect } from 'react';
import { Button, Tooltip } from 'antd';
import { SunOutlined, MoonOutlined, RocketOutlined } from '@ant-design/icons';

const ThemeToggle: React.FC = () => {
    const [theme, setTheme] = useState<'dark' | 'light' | 'cyberpunk'>(
        (localStorage.getItem('theme') as any) || 'dark'
    );

    useEffect(() => {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);

        // Dynamic Ant Design theme switching would normally require ConfigProvider
        // For this demo, we assume the CSS variables in index.css handle it
    }, [theme]);

    const toggleTheme = () => {
        if (theme === 'dark') setTheme('light');
        else if (theme === 'light') setTheme('cyberpunk');
        else setTheme('dark');
    };

    return (
        <Tooltip title={`Switch Theme (Current: ${theme})`}>
            <Button
                shape="circle"
                icon={
                    theme === 'dark' ? <MoonOutlined /> :
                        theme === 'light' ? <SunOutlined /> :
                            <RocketOutlined style={{ color: '#f59e0b' }} />
                }
                onClick={toggleTheme}
                className="theme-toggle-btn"
                style={{
                    background: theme === 'cyberpunk' ? 'rgba(245, 158, 11, 0.1)' : undefined,
                    borderColor: theme === 'cyberpunk' ? '#f59e0b' : undefined,
                }}
            />
        </Tooltip>
    );
};

export default ThemeToggle;

