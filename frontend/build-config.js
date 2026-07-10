const fs = require('fs');

const apiBase = String(process.env.API_BASE_URL || '').replace(/\/+$/, '');
if (!apiBase) {
  throw new Error('API_BASE_URL must be configured in Vercel');
}

fs.writeFileSync(
  'config.js',
  `window.__API_BASE_URL = ${JSON.stringify(apiBase)};\nwindow.__CLOUD_DEPLOYMENT = true;\n`,
  'utf8',
);
console.log(`Configured API base: ${apiBase}`);
