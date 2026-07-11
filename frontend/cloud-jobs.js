(() => {
  if (!window.__CLOUD_DEPLOYMENT) return;

  const apiBase = String(window.__API_BASE_URL || '').replace(/\/+$/, '');
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const value = (id, fallback = '') => document.getElementById(id)?.value || fallback;
  const checked = (id) => Boolean(document.getElementById(id)?.checked);

  function getToken() {
    let token = sessionStorage.getItem('TWSE_ADMIN_TOKEN') || '';
    if (!token) {
      token = window.prompt('請輸入雲端管理 Token 以啟動抓取工作：') || '';
      if (token) sessionStorage.setItem('TWSE_ADMIN_TOKEN', token);
    }
    return token;
  }

  async function api(path, options = {}) {
    const token = getToken();
    if (!token) throw new Error('未提供管理 Token');
    const response = await fetch(`${apiBase}${path}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'X-Admin-Token': token,
        ...(options.headers || {}),
      },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.success) {
      if (response.status === 401) sessionStorage.removeItem('TWSE_ADMIN_TOKEN');
      throw new Error(payload.error || `HTTP ${response.status}`);
    }
    return payload;
  }

  function statusPanel() {
    let panel = document.getElementById('cloudJobStatusPanel');
    if (panel) return panel;
    panel = document.createElement('aside');
    panel.id = 'cloudJobStatusPanel';
    panel.style.cssText = [
      'position:fixed', 'right:18px', 'bottom:18px', 'z-index:9999',
      'width:min(420px,calc(100vw - 36px))', 'padding:14px 16px',
      'border-radius:12px', 'background:rgba(15,23,42,.96)',
      'border:1px solid rgba(96,165,250,.45)', 'color:#e2e8f0',
      'box-shadow:0 12px 35px rgba(0,0,0,.4)', 'font-size:14px',
    ].join(';');
    panel.innerHTML = `
      <div style="display:flex;justify-content:space-between;gap:10px">
        <strong>雲端抓取工作</strong>
        <button id="cloudJobClose" style="border:0;background:transparent;color:#94a3b8;cursor:pointer">×</button>
      </div>
      <div id="cloudJobMessage" style="margin-top:8px">準備就緒</div>
      <div style="height:7px;background:#334155;border-radius:999px;margin-top:10px;overflow:hidden">
        <div id="cloudJobProgress" style="height:100%;width:0;background:#38bdf8;transition:width .3s"></div>
      </div>
      <div id="cloudJobMeta" style="margin-top:8px;color:#94a3b8;font-size:12px"></div>`;
    document.body.appendChild(panel);
    panel.querySelector('#cloudJobClose').onclick = () => panel.remove();
    return panel;
  }

  function renderStatus(message, progress = 0, meta = '') {
    const panel = statusPanel();
    panel.querySelector('#cloudJobMessage').textContent = message;
    panel.querySelector('#cloudJobProgress').style.width = `${Math.max(0, Math.min(100, progress))}%`;
    panel.querySelector('#cloudJobMeta').textContent = meta;
  }

  async function createJob(jobType, params) {
    const payload = await api('/api/jobs', {
      method: 'POST',
      body: JSON.stringify({ job_type: jobType, params }),
    });
    return payload.job;
  }

  async function waitForJob(job) {
    while (true) {
      const payload = await api(`/api/jobs/${job.id}`);
      const current = payload.job;
      renderStatus(
        current.message || current.status,
        Number(current.progress || 0),
        `${current.job_type}｜${current.current_item || current.id}`,
      );
      if (current.status === 'succeeded') return current;
      if (current.status === 'failed' || current.status === 'cancelled') {
        throw new Error(current.error || `工作狀態：${current.status}`);
      }
      await sleep(3000);
    }
  }

  async function submitJobs(specs, button) {
    button.disabled = true;
    try {
      const jobs = [];
      for (let index = 0; index < specs.length; index += 1) {
        const spec = specs[index];
        renderStatus(`建立工作 ${index + 1}/${specs.length}…`, 2);
        jobs.push(await createJob(spec.type, spec.params));
      }
      for (let index = 0; index < jobs.length; index += 1) {
        renderStatus(`等待工作 ${index + 1}/${jobs.length}…`, 3, jobs[index].id);
        await waitForJob(jobs[index]);
      }
      renderStatus(`全部完成，共 ${jobs.length} 個工作`, 100);
    } catch (error) {
      renderStatus(`工作失敗：${error.message}`, 0);
      console.error('[CloudJobs]', error);
    } finally {
      button.disabled = false;
    }
  }

  function statementParams(prefix) {
    const params = {
      year: value(`${prefix}Year`),
      season: value(`${prefix}Season`, '1'),
      code_from: value(`${prefix}CodeFrom`),
      code_to: value(`${prefix}CodeTo`),
      pause_every: value(`${prefix}BatchSize`),
      pause_minutes: value(`${prefix}BatchRestMinutes`),
      retry_max: value(`${prefix}RetryMax`, '1'),
      retry_wait_minutes: value(`${prefix}RetryWaitMinutes`, '5'),
    };
    return Object.fromEntries(Object.entries(params).filter(([, item]) => item !== ''));
  }

  function multiStatementSpecs(prefix, type) {
    const yearFrom = Number(value(`${prefix}YearFrom`));
    const yearTo = Number(value(`${prefix}YearTo`));
    const seasons = [1, 2, 3, 4].filter((season) => checked(`${prefix}MultiSeason${season}`));
    if (!yearFrom || !yearTo || yearFrom > yearTo || !seasons.length) {
      throw new Error('請設定有效的多期年度與季別');
    }
    const common = statementParams(prefix);
    const specs = [];
    for (let year = yearFrom; year <= yearTo; year += 1) {
      seasons.forEach((season) => specs.push({
        type,
        params: { ...common, year, season },
      }));
    }
    return specs;
  }

  function jobSpecsFor(buttonId) {
    if (buttonId === 'executeUpdate') {
      const selectedCount = document.querySelector('.count-option.active')?.dataset.count || '50';
      const scope = document.querySelector('.advanced-option.selected')?.dataset.type || 'count';
      return [{
        type: 'stock_prices',
        params: {
          start_date: value('startDate'),
          end_date: value('endDate'),
          stock_count: Number(selectedCount),
          stock_scope: scope,
          range_from: value('rangeFrom'),
          range_to: value('rangeTo'),
          update_prices: true,
          update_returns: true,
        },
      }];
    }
    if (buttonId === 'computeReturnsBtn') {
      return [{
        type: 'returns',
        params: {
          symbol: value('returnsSymbol'),
          start: value('returnsStartDate'),
          end: value('returnsEndDate'),
          fill_missing: checked('returnsFillMissing'),
          batch_size: Number(value('returnsBatchSize', '50')),
        },
      }];
    }
    if (buttonId === 't86FetchBtn') {
      return [{ type: 't86', params: {
        start: value('t86StartDate'), end: value('t86EndDate'),
        market: value('t86MarketSelect', 'both'), sleep: value('t86SleepSeconds', '0.6'),
      } }];
    }
    if (buttonId === 'marginFetchBtn') {
      return [{ type: 'margin', params: {
        start: value('marginStartDate'), end: value('marginEndDate'),
        market: value('marginMarketSelect', 'both'), sleep: value('marginSleepSeconds', '0.6'),
      } }];
    }
    if (buttonId === 'revenueFetchBtn' || buttonId === 'revenueDownloadMopsBtn') {
      return [{ type: 'revenue', params: {
        start: value('revenueStartYm'), end: value('revenueEndYm'),
        market: value('revenueMarketSelect', 'both'), sleep: '1',
      } }];
    }
    if (buttonId === 'incomeFetchBtn') return [{ type: 'income_statement', params: statementParams('income') }];
    if (buttonId === 'balanceFetchBtn') return [{ type: 'balance_sheet', params: statementParams('balance') }];
    if (buttonId === 'cashflowFetchBtn') return [{ type: 'cash_flow', params: statementParams('cashflow') }];
    if (buttonId === 'incomeMultiFetchBtn') return multiStatementSpecs('income', 'income_statement');
    if (buttonId === 'balanceMultiFetchBtn') return multiStatementSpecs('balance', 'balance_sheet');
    if (buttonId === 'cashflowMultiFetchBtn') return multiStatementSpecs('cashflow', 'cash_flow');
    return null;
  }

  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('#dbTargetToggle, #returnsDbToggle, .module-db-toggle').forEach((element) => {
      element.style.display = 'none';
    });
    document.querySelectorAll('input[name="dbTarget"][value="remote"], input[name="returnsDbTarget"][value="neon"]')
      .forEach((radio) => { radio.checked = true; });

    document.addEventListener('click', (event) => {
      const button = event.target.closest('button');
      if (!button) return;
      if (button.id === 'incomeSingleFetchBtn') return;
      if (button.id === 'cashflowFetchBtn' && value('cashflowSingleCode')) return;
      let specs;
      try {
        specs = jobSpecsFor(button.id);
      } catch (error) {
        event.preventDefault();
        event.stopImmediatePropagation();
        renderStatus(error.message, 0);
        return;
      }
      if (!specs) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      submitJobs(specs, button);
    }, true);
  });
})();
