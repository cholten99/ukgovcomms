document.addEventListener('DOMContentLoaded', () => {
  // -------- Lightbox for charts --------
  const overlay  = document.getElementById('img-modal');
  const imgEl    = document.getElementById('img-modal-img');
  const caption  = document.getElementById('img-modal-caption');
  const closeBtn = document.getElementById('img-modal-close');

  document.querySelectorAll('.global-viz-row .viz img').forEach(img => {
    img.style.cursor = 'zoom-in';
    img.addEventListener('click', () => {
      imgEl.src = img.src;
      imgEl.alt = img.alt || '';
      const cap = img.parentElement.querySelector('figcaption');
      caption.textContent = cap ? cap.textContent : '';
      overlay.classList.add('open');
      overlay.setAttribute('aria-hidden', 'false');
      document.body.style.overflow = 'hidden';
    });
  });

  const close = () => {
    overlay.classList.remove('open');
    overlay.setAttribute('aria-hidden', 'true');
    imgEl.src = '';
    document.body.style.overflow = '';
  };
  if (overlay) {
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
  }
  if (closeBtn) closeBtn.addEventListener('click', close);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });

  // -------- Sortable leaderboard --------
  const table = document.getElementById('leaderboard-table');
  const tbody = table ? table.querySelector('tbody') : null;
  const headers = table ? table.querySelectorAll('thead th[data-sort]') : [];
  let sortState = { key: 'last', dir: 'desc' }; // default: most recent first

  function getKeyValue(tr, key) {
    if (key === 'name') {
      return tr.cells[0].innerText.trim().toLowerCase();
    }
    if (key === 'last') {
      const v = tr.cells[1].getAttribute('data-value') || '';
      return v ? new Date(v).getTime() : -Infinity; // missing dates sort oldest
    }
    if (key === 'total') {
      return parseInt(tr.cells[2].getAttribute('data-value') || '0', 10);
    }
    return '';
  }

  function applySort(key, dir) {
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll('tr'));
    rows.sort((a, b) => {
      const av = getKeyValue(a, key);
      const bv = getKeyValue(b, key);
      if (av < bv) return dir === 'asc' ? -1 : 1;
      if (av > bv) return dir === 'asc' ? 1 : -1;
      // tie-break by name asc
      const an = getKeyValue(a, 'name');
      const bn = getKeyValue(b, 'name');
      return an.localeCompare(bn);
    });
    rows.forEach(r => tbody.appendChild(r));
    headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
    const active = table.querySelector(`thead th[data-sort="${key}"]`);
    if (active) active.classList.add(dir === 'asc' ? 'sort-asc' : 'sort-desc');
    sortState = { key, dir };
  }

  headers.forEach(h => {
    h.style.cursor = 'pointer';
    h.addEventListener('click', () => {
      const key = h.getAttribute('data-sort');
      let dir = 'desc';
      if (sortState.key === key) {
        dir = sortState.dir === 'desc' ? 'asc' : 'desc';
      } else {
        dir = (key === 'name') ? 'asc' : 'desc';
      }
      applySort(key, dir);
    });
  });

  // initial sort
  if (table) applySort(sortState.key, sortState.dir);

  // -------- Swap charts when clicking a blog name --------
  const scopeLabel = document.getElementById('viz-scope-label');
  const resetBtn   = document.getElementById('viz-reset');

  const imgMonthly  = document.getElementById('img-monthly');
  const imgRolling  = document.getElementById('img-rolling');
  const imgCloud    = document.getElementById('img-wordcloud');

  const GLOBAL = {
    monthly:  '/assets/global/monthly_bars_all-sources.png',
    rolling:  '/assets/global/rolling_avg_90d_all-sources.png',
    cloud:    '/assets/global/wordcloud_all-sources.png',
    label:    'All sources'
  };

  function sourcesForSlug(slug) {
    const base = `/assets/sources/${slug}`;
    return {
      monthly: `${base}/monthly_bars_${slug}.png`,
      rolling: `${base}/rolling_avg_90d_${slug}.png`,
      cloud:   `${base}/wordcloud_${slug}.png`,
    };
  }

  function showGlobal() {
    imgMonthly.src = GLOBAL.monthly;
    imgRolling.src = GLOBAL.rolling;
    imgCloud.src   = GLOBAL.cloud;
    scopeLabel.textContent = GLOBAL.label;
    resetBtn.hidden = true;
  }

  function showBlog(slug, name) {
    const srcs = sourcesForSlug(slug);
    imgMonthly.src = srcs.monthly;
    imgRolling.src = srcs.rolling;
    imgCloud.src   = srcs.cloud;
    scopeLabel.textContent = name;
    resetBtn.hidden = false;
  }

  if (resetBtn) {
    resetBtn.addEventListener('click', (e) => {
      e.preventDefault();
      showGlobal();
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
  }

  if (tbody) {
    tbody.addEventListener('click', (e) => {
      const a = e.target.closest('a.js-swap-viz');
      if (!a) return;
      e.preventDefault();
      const slug = a.getAttribute('data-slug');
      const name = a.getAttribute('data-name') || 'Blog';
      showBlog(slug, name);
      // helpful scroll to the charts area on small screens
      document.querySelector('.global-viz-row')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  }
});

