import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { '@': path.resolve(__dirname, './src') },
  },
  server: {
    port: 3000,
    proxy: {
      // Proxy write calls do Azure Function lokalnie (dev only)
      '/api': {
        target: process.env.VITE_WRITE_API_URL ?? 'http://localhost:7071',
        changeOrigin: true,
      },
    },
  },
});
