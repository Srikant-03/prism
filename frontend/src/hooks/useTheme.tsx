/**
 * ThemeContext — Provides dark/light mode toggle with localStorage persistence.
 */

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';

type ThemeMode = 'dark' | 'light' | 'cyberpunk';

interface ThemeContextType {
    mode: ThemeMode;
    toggle: () => void;
    setMode: (m: ThemeMode) => void;
    isDark: boolean;
}

const ThemeContext = createContext<ThemeContextType>({
    mode: 'dark',
    toggle: () => { },
    setMode: () => { },
    isDark: true,
});

const STORAGE_KEY = 'dip_theme_mode_v2';

export const ThemeProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const [mode, setMode] = useState<ThemeMode>(() => {
        const saved = localStorage.getItem(STORAGE_KEY);
        return (saved === 'light' || saved === 'dark' || saved === 'cyberpunk') ? (saved as ThemeMode) : 'dark';
    });

    useEffect(() => {
        localStorage.setItem(STORAGE_KEY, mode);
        document.documentElement.setAttribute('data-theme', mode);
    }, [mode]);

    const toggle = useCallback(() => {
        setMode(prev => {
            if (prev === 'dark') return 'light';
            if (prev === 'light') return 'cyberpunk';
            return 'dark';
        });
    }, []);

    return (
        <ThemeContext.Provider value={{ mode, toggle, setMode, isDark: mode !== 'light' }}>
            {children}
        </ThemeContext.Provider>
    );
};

export const useTheme = () => useContext(ThemeContext);
export default ThemeContext;

