import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
    css: true,
    testTimeout: 20000,
  },
  build: {
    chunkSizeWarningLimit: 1400,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) {
            return undefined; // Let Rollup manage application source code graph naturally
          }

          // AG Grid — Excel-like data grid
          if (id.includes('ag-grid')) return 'ag-grid';

          // ECharts + zrender — visualization engine
          if (id.includes('echarts') || id.includes('zrender')) return 'echarts';

          // Ant Design ecosystem (UI + icons + rc-components)
          if (
            id.includes('antd') ||
            id.includes('@ant-design') ||
            id.includes('@rc-component') ||
            id.includes('rc-')
          ) return 'antd-ecosystem';

          // React ecosystem
          if (
            id.includes('/react/') ||
            id.includes('react-dom') ||
            id.includes('scheduler')
          ) return 'react-vendor';

          // Recharts + D3
          if (id.includes('recharts') || id.includes('/d3-')) return 'charts';

          return 'vendor';
        },
      },
    },
  },
})
