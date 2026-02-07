import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    chunkSizeWarningLimit: 1400,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) {
            if (id.includes('/components/sql/')) return 'feature-sql';
            if (id.includes('/components/profiling/') || id.includes('/components/insights/')) return 'feature-profiling';
            if (id.includes('/components/cleaning/')) return 'feature-cleaning';
            if (id.includes('/components/reporting/')) return 'feature-reporting';
            if (id.includes('/components/comparison/') || id.includes('/components/collab/')) return 'feature-collab';
            if (id.includes('/components/chat/') || id.includes('/components/grid/')) return 'feature-grid';
            return undefined;
          }

          // AG Grid — Excel-like data grid
          if (id.includes('ag-grid')) return 'ag-grid';

          // ECharts + zrender — biggest single dep (~1MB minified)
          if (id.includes('echarts') || id.includes('zrender')) return 'echarts';

          // Ant Design icons
          if (id.includes('@ant-design/icons')) return 'antd-icons';

          // Ant Design core + internal components
          if (
            id.includes('/antd/') ||
            id.includes('@ant-design/') ||
            id.includes('@rc-component/') ||
            id.includes('async-validator') ||
            id.includes('classnames') ||
            id.includes('copy-to-clipboard') ||
            id.includes('scroll-into-view') ||
            id.includes('compute-scroll-into-view') ||
            id.includes('toggle-selection') ||
            id.includes('json2mq') ||
            id.includes('throttle-debounce')
          ) return 'antd-ui';

          // Recharts + D3
          if (id.includes('recharts') || id.includes('/d3-')) return 'charts';

          // React
          if (id.includes('react-dom')) return 'react-dom';

          // Runtime libs
          if (
            id.includes('@emotion') ||
            id.includes('stylis') ||
            id.includes('@babel/runtime') ||
            id.includes('tslib') ||
            id.includes('dayjs')
          ) return 'runtime';

          return 'vendor';
        },
      },
    },
  },
})
