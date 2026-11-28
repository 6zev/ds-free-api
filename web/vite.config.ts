import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'
import fs from 'fs'

const configPath = path.resolve(__dirname, '../config.toml')
let backendPort = 22217
try {
  const configToml = fs.readFileSync(configPath, 'utf-8')
  const portMatch = configToml.match(/^port\s*=\s*(\d+)/m)
  if (portMatch) backendPort = parseInt(portMatch[1], 10)
} catch {
  // config.toml 不存在时使用默认端口
}

// 构建期注入版本号：读 web/package.json（发布时与 Cargo.toml 同步 bump），
// 避免 Layout / LoginPage 里硬编码的旧版本号（曾长期停在 v0.2.6）。
const pkg = JSON.parse(fs.readFileSync(path.resolve(__dirname, 'package.json'), 'utf-8')) as {
  version?: string
}
const appVersion = pkg.version ?? '0.0.0'

export default defineConfig({
  base: '/admin/',
  define: {
    __APP_VERSION__: JSON.stringify(`v${appVersion}`),
  },
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    proxy: {
      '/admin/api': `http://127.0.0.1:${backendPort}`,
      '/v1': `http://127.0.0.1:${backendPort}`,
      '/anthropic': `http://127.0.0.1:${backendPort}`,
    },
  },
})
