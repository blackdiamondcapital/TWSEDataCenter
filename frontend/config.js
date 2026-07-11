// Local/desktop defaults. Vercel build overwrites this file via build-config.js.
window.__API_BASE_URL = window.__API_BASE_URL || (
    window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
        ? 'http://localhost:5003'
        : ''
);
window.__CLOUD_DEPLOYMENT = false;
