(function(){
  const $ = (s)=>document.querySelector(s);
  const $$ = (s)=>Array.from(document.querySelectorAll(s));
  const input = $('#search');
  const tiles = $$('.tile');
  const sentinel = document.getElementById('sentinel');
  const PAGE = 20; // initial batch size
  let visibleCount = 0;
  const countEl = $('#count');
  const selectAll = $('#selectAll');
  const downloadBtn = $('#downloadBtn');
  const form = $('#downloadForm');
  const orderBy = $('#orderBy');
  const orderDir = $('#orderDir');
  const filterCountry = $('#filterCountry');
  const filterCity = $('#filterCity');
  const filterYear = $('#filterYear');
  const votesMin = $('#votesMin');
  const votesMax = $('#votesMax');
  const projectsMin = $('#projectsMin');
  const projectsMax = $('#projectsMax');
  const lenMin = $('#lenMin');
  const lenMax = $('#lenMax');
  const filterType = $('#filterType');
  const excludeFully = $('#excludeFully');
  const excludeExperimental = $('#excludeExperimental');
  const requireGeo = $('#requireGeo');
  const filtersClear = document.getElementById('filtersClear');
  const filtersPanel = document.getElementById('filtersPanel');
  const openFiltersBtn = document.getElementById('openFilters');
  const closeFiltersBtn = document.getElementById('closeFilters');
  let filtersBackdrop = null;

  function normalize(s){ return (s||'').toString().toLowerCase(); }

  function initOptions(){
    const setCountry = new Set(), setCity = new Set(), setYear = new Set();
    tiles.forEach(t=>{
      if(t.dataset.country) setCountry.add(t.dataset.country);
      if(t.dataset.city) setCity.add(t.dataset.city);
      if(t.dataset.year) setYear.add(t.dataset.year);
    });
    [...setCountry].sort().forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; filterCountry.appendChild(o); });
    [...setCity].sort().forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; filterCity.appendChild(o); });
    [...setYear].sort((a,b)=>Number(a||0)-Number(b||0)).forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; filterYear.appendChild(o); });
  }

  // Disable options that are not compatible with current selections across selects (exclusive filters)
  function updateSelectStates(){
    const selCountry = normalize(filterCountry.value);
    const selCity = normalize(filterCity.value);
    const selYear = (filterYear.value || '');
    const selType = normalize(filterType.value);

    function eligibleTiles(excludeKey){
      return tiles.filter(t => {
        if(excludeKey !== 'country' && selCountry && normalize(t.dataset.country) !== selCountry) return false;
        if(excludeKey !== 'city' && selCity && normalize(t.dataset.city) !== selCity) return false;
        if(excludeKey !== 'year' && selYear && (t.dataset.year !== selYear)) return false;
        if(excludeKey !== 'type' && selType && normalize(t.dataset.type) !== selType) return false;
        return true;
      });
    }

    function disableOptions(selectEl, key){
      if(!selectEl) return;
      const allowed = new Set(
        eligibleTiles(key).map(t => {
          if(key === 'country') return t.dataset.country || '';
          if(key === 'city') return t.dataset.city || '';
          if(key === 'year') return t.dataset.year || '';
          if(key === 'type') return (t.dataset.type || '');
          return '';
        }).filter(Boolean)
      );
      Array.from(selectEl.options).forEach(opt => {
        if(opt.value === ''){ opt.disabled = false; return; }
        // For type select we compare normalized because its options are predefined
        if(key === 'type'){
          opt.disabled = !Array.from(allowed).some(v => normalize(v) === normalize(opt.value));
        } else {
          opt.disabled = !allowed.has(opt.value);
        }
      });
    }

    disableOptions(filterCountry, 'country');
    disableOptions(filterCity, 'city');
    disableOptions(filterYear, 'year');
    disableOptions(filterType, 'type');
  }

  function passesNumeric(val, min, max){
    const v = Number(val);
    const hasMin = min.value.trim() !== '' && !isNaN(Number(min.value));
    const hasMax = max.value.trim() !== '' && !isNaN(Number(max.value));
    if(hasMin && v < Number(min.value)) return false;
    if(hasMax && v > Number(max.value)) return false;
    return true;
  }

  function filter(){
    const q = normalize(input.value);
    const country = normalize(filterCountry.value);
    const city = normalize(filterCity.value);
    const year = filterYear.value;
    const type = normalize(filterType.value);
    let visible = 0;
    // hide all by default; reveal during pagination
    tiles.forEach(t=>{
      const hay = [t.dataset.title, t.dataset.webpage, t.dataset.desc, t.dataset.file]
        .map(normalize).join(' ');
      if(q && !hay.includes(q)) { t.hidden = true; return; }
      if(country && normalize(t.dataset.country) !== country) { t.hidden=true; return; }
      if(city && normalize(t.dataset.city) !== city) { t.hidden=true; return; }
      if(year && (t.dataset.year !== year)) { t.hidden=true; return; }
      if(!passesNumeric(t.dataset.votes || 0, votesMin, votesMax)) { t.hidden=true; return; }
      if(!passesNumeric(t.dataset.projects || 0, projectsMin, projectsMax)) { t.hidden=true; return; }
      if(!passesNumeric(t.dataset.vlen || NaN, lenMin, lenMax)) { t.hidden=true; return; }
      if(type && normalize(t.dataset.type) !== type) { t.hidden=true; return; }
      if(excludeFully.checked && t.dataset.fully === '1') { t.hidden=true; return; }
      if(excludeExperimental.checked && t.dataset.experimental === '1') { t.hidden=true; return; }
      if(requireGeo && requireGeo.checked && t.dataset.geo !== '1') { t.hidden=true; return; }
      t.hidden = false;
      visible++;
    });
    countEl.textContent = String(visible);
    selectAll.checked = false; // avoid confusion after filter
    updateChecks();
    sortTiles();
    // reset pagination after filtering/sorting
    visibleCount = 0;
    revealNext();
  }

  function sortTiles(){
    const dir = orderDir.dataset.dir === 'desc' ? -1 : 1;
    const key = orderBy.value;
    // Sort within the tiles' actual parent (the form), not the outer container
    const container = document.querySelector('#downloadForm') || document.querySelector('.container');
    const items = Array.from(container.querySelectorAll('.tile'));
    items.sort((a,b)=>{
      const av = (key==='quality')?Number(a.dataset.quality||0):
                 (key==='votes')?Number(a.dataset.votes||0):
                 (key==='projects')?Number(a.dataset.projects||0):
                 (key==='budget')?Number(a.dataset.budget||0):
                 (key==='year')?Number(a.dataset.year||0):
                 0;
      const bv = (key==='quality')?Number(b.dataset.quality||0):
                 (key==='votes')?Number(b.dataset.votes||0):
                 (key==='projects')?Number(b.dataset.projects||0):
                 (key==='budget')?Number(b.dataset.budget||0):
                 (key==='year')?Number(b.dataset.year||0):
                 0;
      if(av === bv){
        // stable-ish secondary sort by title asc
        const at = (a.dataset.title||'').toLowerCase();
        const bt = (b.dataset.title||'').toLowerCase();
        return at.localeCompare(bt);
      }
      return (av < bv ? -1 : 1) * dir;
    });
    items.forEach(it=>container.appendChild(it));
  }

  function visibleRowChecks(){
    return $$('.tile').filter(t => !t.hidden && t.style.display !== 'none').map(t => t.querySelector('.row-check'));
  }

  function allFilteredRowChecks(){
    return $$('.tile').filter(t => !t.hidden).map(t => t.querySelector('.row-check'));
  }

  function updateChecks(){
    const visibleChecks = visibleRowChecks();
    const allFilteredChecks = allFilteredRowChecks();
    const anyChecked = allFilteredChecks.some(ch => ch.checked);
    const allChecked = allFilteredChecks.length > 0 && allFilteredChecks.every(ch => ch.checked);
    const selectedCount = allFilteredChecks.filter(ch => ch.checked).length;
    
    // Update download button text to show number of selected files
    if (selectedCount === 0) {
      downloadBtn.textContent = 'Download selected';
    } else {
      downloadBtn.textContent = `Download ${selectedCount} selected file${selectedCount === 1 ? '' : 's'}`;
    }
    
    downloadBtn.disabled = !anyChecked;
    selectAll.checked = allChecked;
    selectAll.indeterminate = anyChecked && !allChecked;
    if(allFilteredChecks.length){ selectAll.disabled = false; } else { selectAll.disabled = true; selectAll.checked = false; selectAll.indeterminate = false; }
  }

  selectAll.addEventListener('change', () => {
    const checks = allFilteredRowChecks();
    checks.forEach(ch => ch.checked = selectAll.checked);
    updateChecks();
  });
  document.addEventListener('change', (e) => {
    if(e.target && e.target.classList.contains('row-check')){
      updateChecks();
    }
  });

  form.addEventListener('submit', async (e) => {
    // client-side: start background job instead of direct POST to get progress
    e.preventDefault();
    const selected = $$('.row-check:checked');
    if(!selected.length){ return; }

    // Build form data; prefer exclude-mode when selection is large
    const fd = new FormData();
    const allFilteredChecks = allFilteredRowChecks();
    const allFilteredSelected = allFilteredChecks.length > 0 && allFilteredChecks.every(ch => ch.checked);
    const selectAllChecked = selectAll.checked && allFilteredSelected;
    // Determine if any filters are active (query text or filter controls)
    const anyFiltersActive = (
      (input && input.value.trim() !== '') ||
      (filterCountry && filterCountry.value) ||
      (filterCity && filterCity.value) ||
      (filterYear && filterYear.value) ||
      (votesMin && votesMin.value) || (votesMax && votesMax.value) ||
      (projectsMin && projectsMin.value) || (projectsMax && projectsMax.value) ||
      (lenMin && lenMin.value) || (lenMax && lenMax.value) ||
      (filterType && filterType.value) ||
      (excludeFully && excludeFully.checked) ||
      (excludeExperimental && excludeExperimental.checked) ||
      (requireGeo && requireGeo.checked)
    );

    if (selectAllChecked && !anyFiltersActive) {
      // True global select-all with no filters: let server use cache or full set
      fd.append('select_all', 'true');
    } else {
      // If user selected most of the filtered list, send exclude list instead to keep request tiny
      const totalFiltered = allFilteredChecks.length;
      const selectedCount = selected.length;
      const excluded = allFilteredChecks.filter(ch => !ch.checked);
      // Use exclude-mode when excluded are fewer than selected and total is reasonably large
  const useExclude = !anyFiltersActive && excluded.length > 0 && excluded.length < selectedCount && totalFiltered >= 50;
      if (useExclude) {
        fd.append('select_all', 'true');
        excluded.forEach(ch => fd.append('exclude', ch.dataset.file));
      } else {
        // Normal mode: send selected names
        selected.forEach(ch => fd.append('files', ch.dataset.file));
      }
    }

    // Show progress UI
    const box = document.getElementById('downloadProgress');
    const bar = document.getElementById('dlBar');
    const phase = document.getElementById('dlPhase');
    const text = document.getElementById('dlText');
    const pct = document.getElementById('dlPercent');
    const fileCount = document.getElementById('dlFileCount');
    const curSpan = document.getElementById('dlCurrent');
    const totSpan = document.getElementById('dlTotal');
    const nameSpan = document.getElementById('dlFileName');
    const hint = document.getElementById('dlHint');
    if(box){
      box.classList.remove('hidden');
      try { box.scrollIntoView({ behavior: 'smooth', block: 'center' }); } catch(_){}
      if(bar) bar.style.width = '0%';
      if(pct) pct.textContent = '0%';
      if(text) text.textContent = 'Preparing download...';
      if(phase){ phase.textContent = 'Preparing'; phase.className = 'px-2 py-1 rounded-full text-xs font-medium bg-indigo-100 text-indigo-700'; }
      if(fileCount) fileCount.classList.add('hidden');
      if(nameSpan) nameSpan.classList.add('hidden');
      if(hint) hint.textContent = 'We are zipping your selection on the server. This can take a moment.';
    }

    // Start job
    let startResp;
    try{
  const allFilteredChecks = allFilteredRowChecks();
  const allFilteredSelected = allFilteredChecks.length > 0 && allFilteredChecks.every(ch => ch.checked);
  const selectAllChecked = selectAll.checked && allFilteredSelected;
  const qs = selectAllChecked ? '?select_all=true' : '';
      startResp = await fetch('/download-selected/start' + qs, { method: 'POST', body: fd });
    }catch(err){
      if(box) box.classList.add('hidden');
      alert('Failed to start download');
      return;
    }
    if(!startResp.ok){
      if(box) box.classList.add('hidden');
      try { const data = await startResp.json(); alert(data && data.error ? data.error : 'Failed to start download'); } catch(_){ alert('Failed to start download'); }
      return;
    }
    const startData = await startResp.json();
    const token = startData.token;
    const progUrl = startData.progress_url || `/download-selected/progress/${token}`;
    const fileUrl = startData.file_url || `/download-selected/file/${token}`;

    // Poll progress
    let tries = 0;
    const maxTries = 60 * 10; // ~10 minutes @ 1s
    while(tries < maxTries){
      tries++;
      let prog;
      try{
        const r = await fetch(progUrl, { cache: 'no-store' });
        if(!r.ok){ await new Promise(res=>setTimeout(res, 1000)); continue; }
        prog = await r.json();
      }catch(_){ await new Promise(res=>setTimeout(res, 1000)); continue; }
      if(!prog || !prog.ok){ await new Promise(res=>setTimeout(res, 800)); continue; }
      const d = prog;
      const total = Number(d.total||0);
      const current = Number(d.current||0);
      const percent = Number(d.percent||0);
      const currentName = d.current_name || '';
      if(bar) bar.style.width = Math.max(0, Math.min(100, percent)) + '%';
      if(pct) pct.textContent = Math.max(0, Math.min(100, percent)) + '%';
      if(phase){ phase.textContent = (d.status === 'ready') ? 'Ready' : 'Zipping'; phase.className = (d.status === 'ready') ? 'px-2 py-1 rounded-full text-xs font-medium bg-emerald-100 text-emerald-700' : 'px-2 py-1 rounded-full text-xs font-medium bg-indigo-100 text-indigo-700'; }
      if(text) text.textContent = (d.status === 'ready') ? 'Download ready' : 'Zipping files...';
      if(fileCount){
        if(total>0){ fileCount.classList.remove('hidden'); } else { fileCount.classList.add('hidden'); }
        if(totSpan) totSpan.textContent = String(total);
        if(curSpan) curSpan.textContent = String(current);
      }
      if(nameSpan){
        if(currentName){
          nameSpan.classList.remove('hidden');
          nameSpan.textContent = currentName;
          nameSpan.title = currentName;
        } else {
          nameSpan.classList.add('hidden');
        }
      }
      if(d.error){
        if(box) box.classList.add('hidden');
        alert('Error while preparing download: ' + d.error);
        return;
      }
      if(d.done){
        // Trigger file download
        try{
          if(bar) bar.style.width = '100%';
          if(pct) pct.textContent = '100%';
          if(text) text.textContent = 'Starting download...';
          const a = document.createElement('a');
          a.href = fileUrl;
          a.download = (d.download_name || 'pb_selected.zip');
          document.body.appendChild(a);
          a.click();
          a.remove();
        }finally{
          setTimeout(()=>{ if(box) box.classList.add('hidden'); }, 1000);
        }
        return;
      }
      await new Promise(res=>setTimeout(res, 1000));
    }
    // Timeout
    if(box) box.classList.add('hidden');
    alert('Preparing the download is taking too long. Please try again.');
  });

  // listeners for controls
  // simple debounce for input-heavy changes
  let tHandle;
  function debounced(){
    clearTimeout(tHandle); tHandle = setTimeout(()=>{ updateSelectStates(); filter(); }, 100);
  }

  [input, filterCountry, filterCity, filterYear, votesMin, votesMax, projectsMin, projectsMax, lenMin, lenMax, filterType, excludeFully, excludeExperimental, requireGeo]
    .forEach(el => el.addEventListener('input', debounced));
  orderBy.addEventListener('change', ()=>{ sortTiles(); visibleCount = 0; revealNext(); });
  orderDir.addEventListener('click', ()=>{
    orderDir.dataset.dir = (orderDir.dataset.dir === 'desc') ? 'asc' : 'desc';
    orderDir.textContent = (orderDir.dataset.dir === 'desc') ? '↓' : '↑';
    sortTiles();
    visibleCount = 0;
    revealNext();
  });

  // initial: default to Quality, descending (bigger score first)
  orderBy.value = 'quality';
  orderDir.dataset.dir = 'desc';
  orderDir.textContent = '↓';
  initOptions();
  updateSelectStates();
  updateChecks();
  sortTiles();
  if(filtersClear){
    filtersClear.addEventListener('click', ()=>{
      input.value='';
      filterCountry.value='';
      filterCity.value='';
      filterYear.value='';
      votesMin.value='';
      votesMax.value='';
      projectsMin.value='';
      projectsMax.value='';
      lenMin.value='';
      lenMax.value='';
      filterType.value='';
      excludeFully.checked=false;
      excludeExperimental.checked=false;
  if(requireGeo) requireGeo.checked=false;
      updateSelectStates();
      filter();
      input.focus();
    });
  }
  // pagination
  function revealNext(){
    // Use current DOM order so pagination respects the latest sort order
    const parent = document.querySelector('#downloadForm') || document;
    const eligible = Array.from(parent.querySelectorAll('.tile')).filter(t => !t.hidden);
    const end = Math.min(eligible.length, visibleCount + PAGE);
    eligible.forEach((t, idx) => {
      t.style.display = (idx < end) ? '' : 'none';
    });
    visibleCount = end;
  }
  revealNext();

  const io = new IntersectionObserver((entries)=>{
    if(entries.some(e=>e.isIntersecting)){
      revealNext();
    }
  });
  if(sentinel) io.observe(sentinel);

  // Fallback: also load when near bottom on scroll/resize (covers cases where IO doesn't retrigger)
  function nearBottom(){
    const doc = document.documentElement;
    const bottomGap = doc.scrollHeight - (window.scrollY + window.innerHeight);
    return bottomGap < 300; // px
  }
  function maybeLoadMore(){ if(nearBottom()) revealNext(); }
  window.addEventListener('scroll', maybeLoadMore, {passive:true});
  window.addEventListener('resize', maybeLoadMore);

  // Hover chmurka with quick dataset summary from tile dataset (non-blocking)
  let mini = null, hideMiniT = null;
  function getMini(){
    if(mini) return mini;
    mini = document.createElement('div');
    mini.className = 'mini-pop';
    mini.innerHTML = '<div class="mini-head"></div><div class="mini-sub"></div><div class="mini-grid"></div><div class="mini-actions"><a target="_blank" rel="noopener">Open full preview →</a></div>';
    document.body.appendChild(mini);
    mini.addEventListener('mouseenter', ()=>{ if(hideMiniT){ clearTimeout(hideMiniT); hideMiniT=null; } });
    mini.addEventListener('mouseleave', ()=> scheduleMiniHide());
    return mini;
  }
  function scheduleMiniHide(){ if(hideMiniT){ clearTimeout(hideMiniT); } hideMiniT = setTimeout(()=>{ if(mini) mini.classList.remove('show'); }, 80); }
  function positionMini(anchor){
    const r = anchor.getBoundingClientRect();
    const top = Math.max(8, r.bottom + 8);
    const left = Math.min(window.innerWidth - 16 - 360, Math.max(8, r.right - 360));
    // mini is position: fixed -> use viewport coordinates, no scroll offsets
    mini.style.top = `${top}px`;
    mini.style.left = `${left}px`;
  }
  function fillMini(tile, href){
    const head = mini.querySelector('.mini-head');
    const sub = mini.querySelector('.mini-sub');
    const grid = mini.querySelector('.mini-grid');
    const link = mini.querySelector('.mini-actions a');
    // Hide title/sub; present key details in the info grid instead
    head.textContent = '';
    head.style.display = 'none';
    sub.textContent = '';
    sub.style.display = 'none';
    grid.innerHTML = '';
    const esc = (s)=>String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const toTitle = (s)=>String(s||'').toLowerCase().replace(/\b\w/g, c=>c.toUpperCase());
    function row(k,v){ const dk=document.createElement('div'); dk.className='k'; dk.textContent=k; const dv=document.createElement('div'); dv.className='v'; dv.textContent=v; grid.appendChild(dk); grid.appendChild(dv); }
    function rowHtml(k,vHtml){ const dk=document.createElement('div'); dk.className='k'; dk.textContent=k; const dv=document.createElement('div'); dv.className='v'; dv.innerHTML=vHtml; grid.appendChild(dk); grid.appendChild(dv); }
    // Primary details first: Country / Unit / Year (of voting) with bold values
    if(tile.dataset.country) rowHtml('Country', `<strong>${esc(toTitle(tile.dataset.country))}</strong>`);
    if(tile.dataset.city) rowHtml('Unit', `<strong>${esc(tile.dataset.city)}</strong>`);
    if(tile.dataset.year) rowHtml('Year (of voting)', `<strong>${esc(tile.dataset.year)}</strong>`);
    // Show items that are NOT already shown in the tile grid itself
  if(tile.dataset.rule) row('Rule', tile.dataset.rule); // keep leading capital for consistency in grid
  if(tile.dataset.edition) row('Edition', tile.dataset.edition);
  if(tile.dataset.language) row('Language', tile.dataset.language);
  if(tile.dataset.selected && tile.dataset.selected !== '0') row('# selected projects', tile.dataset.selected);
    if(tile.dataset.fully === '1') row('Funding status', 'Fully funded');
    if(tile.dataset.experimental === '1') row('Flag', 'Experimental');
    // Keep a short description only if tile's description is empty (fallback)
    if(!tile.dataset.desc && tile.dataset.webpage) row('Webpage', tile.dataset.webpage);
    link.href = href;
  }
  // Use mouseover/mouseout for reliable delegation
  document.addEventListener('mouseover', (e)=>{
    const a = e.target && e.target.closest && e.target.closest('a.doc');
    if(!a) return;
    const tile = a.closest('.tile');
    if(!tile) return;
    const href = a.href || `/preview/${encodeURIComponent(tile.dataset.file || '')}`;
    getMini();
    fillMini(tile, href);
    positionMini(a);
    mini.classList.add('show');
  }, true);
  document.addEventListener('mouseout', (e)=>{
    const a = e.target && e.target.closest && e.target.closest('a.doc');
    if(!a) return;
    // If moving into the mini popover itself, keep it shown
    const toEl = e.relatedTarget;
    if(toEl && mini && (toEl === mini || (toEl.closest && toEl.closest('.mini-pop')))) return;
    scheduleMiniHide();
  }, true);
  window.addEventListener('scroll', ()=>{ if(mini) mini.classList.remove('show'); }, {passive:true});

  // Mobile Filters Drawer ---------------------------------------------------
  function ensureBackdrop(){
    if (filtersBackdrop) return filtersBackdrop;
    filtersBackdrop = document.createElement('div');
    filtersBackdrop.className = 'filters-backdrop';
    document.body.appendChild(filtersBackdrop);
    filtersBackdrop.addEventListener('click', closeDrawer);
    return filtersBackdrop;
  }
  function openDrawer(){
    if(!filtersPanel) return;
    ensureBackdrop().classList.add('show');
    filtersPanel.classList.add('drawer-open');
    filtersPanel.setAttribute('aria-hidden','false');
    // prevent body scroll under drawer
    document.documentElement.style.overflow = 'hidden';
    document.body.style.overflow = 'hidden';
    if(openFiltersBtn) openFiltersBtn.setAttribute('aria-expanded','true');
  }
  function closeDrawer(){
    if(!filtersPanel) return;
    if(filtersBackdrop) filtersBackdrop.classList.remove('show');
    filtersPanel.classList.remove('drawer-open');
    filtersPanel.setAttribute('aria-hidden','true');
    document.documentElement.style.overflow = '';
    document.body.style.overflow = '';
    if(openFiltersBtn) openFiltersBtn.setAttribute('aria-expanded','false');
  }
  if(openFiltersBtn && filtersPanel){
    openFiltersBtn.addEventListener('click', (e)=>{
      e.preventDefault();
      if(filtersPanel.classList.contains('drawer-open')) closeDrawer(); else openDrawer();
    });
    if(closeFiltersBtn){ closeFiltersBtn.addEventListener('click', closeDrawer); }
    // close drawer on Escape
    document.addEventListener('keydown', (e)=>{
      if(e.key === 'Escape') closeDrawer();
    });
    // On desktop resize, make sure drawer isn't open state interfering
    window.addEventListener('resize', ()=>{
      if(window.innerWidth > 900) closeDrawer();
    });
  }
})();
