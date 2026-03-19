import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    port: 4173,
    proxy: {
      '/forecast': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/health': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
});
