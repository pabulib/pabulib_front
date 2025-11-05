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
  const requireTarget = $('#requireTarget');
  const requireCategory = $('#requireCategory');
  const filtersClear = document.getElementById('filtersClear');
  const filtersPanel = document.getElementById('filtersPanel');
  const openFiltersBtn = document.getElementById('openFilters');
  const closeFiltersBtn = document.getElementById('closeFilters');
  let filtersBackdrop = null;

  // URL parameter handling
  let updateURLTimeout = null;
  
  function getURLParams() {
    const params = new URLSearchParams(window.location.search);
    return {
      search: params.get('search') || '',
      country: params.get('country') || '',
      city: params.get('city') || '',
      year: params.get('year') || '',
      votesMin: params.get('votes_min') || '',
      votesMax: params.get('votes_max') || '',
      projectsMin: params.get('projects_min') || '',
      projectsMax: params.get('projects_max') || '',
      lenMin: params.get('len_min') || '',
      lenMax: params.get('len_max') || '',
      type: params.get('type') || '',
      excludeFully: params.get('exclude_fully') === 'true',
      excludeExperimental: params.get('exclude_experimental') === 'true',
      requireGeo: params.get('require_geo') === 'true',
      requireTarget: params.get('require_target') === 'true',
      requireCategory: params.get('require_category') === 'true',
      orderBy: params.get('order_by') || 'quality',
      orderDir: params.get('order_dir') || 'desc'
    };
  }
  
  function updateURL() {
    // Debounce URL updates to avoid too many history entries
    if (updateURLTimeout) clearTimeout(updateURLTimeout);
    updateURLTimeout = setTimeout(() => {
      const params = new URLSearchParams();
      
      // Add parameters only if they have values
      if (input && input.value.trim()) params.set('search', input.value.trim());
      if (filterCountry && filterCountry.value) params.set('country', filterCountry.value);
      if (filterCity && filterCity.value) params.set('city', filterCity.value);
      if (filterYear && filterYear.value) params.set('year', filterYear.value);
      if (votesMin && votesMin.value) params.set('votes_min', votesMin.value);
      if (votesMax && votesMax.value) params.set('votes_max', votesMax.value);
      if (projectsMin && projectsMin.value) params.set('projects_min', projectsMin.value);
      if (projectsMax && projectsMax.value) params.set('projects_max', projectsMax.value);
      if (lenMin && lenMin.value) params.set('len_min', lenMin.value);
      if (lenMax && lenMax.value) params.set('len_max', lenMax.value);
      if (filterType && filterType.value) params.set('type', filterType.value);
      if (excludeFully && excludeFully.checked) params.set('exclude_fully', 'true');
      if (excludeExperimental && excludeExperimental.checked) params.set('exclude_experimental', 'true');
      if (requireGeo && requireGeo.checked) params.set('require_geo', 'true');
      if (requireTarget && requireTarget.checked) params.set('require_target', 'true');
      if (requireCategory && requireCategory.checked) params.set('require_category', 'true');
      if (orderBy && orderBy.value !== 'quality') params.set('order_by', orderBy.value);
      if (orderDir && orderDir.dataset.dir !== 'desc') params.set('order_dir', orderDir.dataset.dir);
      
      const newURL = window.location.pathname + (params.toString() ? '?' + params.toString() : '');
      history.replaceState({}, '', newURL);
    }, 300);
  }
  
  function applyURLParams() {
    const params = getURLParams();
    
    // Apply parameters to form controls
    if (input) input.value = params.search;
    if (filterCountry) filterCountry.value = params.country;
    if (filterCity) filterCity.value = params.city;
    if (filterYear) filterYear.value = params.year;
    if (votesMin) votesMin.value = params.votesMin;
    if (votesMax) votesMax.value = params.votesMax;
    if (projectsMin) projectsMin.value = params.projectsMin;
    if (projectsMax) projectsMax.value = params.projectsMax;
    if (lenMin) lenMin.value = params.lenMin;
    if (lenMax) lenMax.value = params.lenMax;
    if (filterType) filterType.value = params.type;
    if (excludeFully) excludeFully.checked = params.excludeFully;
    if (excludeExperimental) excludeExperimental.checked = params.excludeExperimental;
    if (requireGeo) requireGeo.checked = params.requireGeo;
    if (requireTarget) requireTarget.checked = params.requireTarget;
    if (requireCategory) requireCategory.checked = params.requireCategory;
    if (orderBy) orderBy.value = params.orderBy;
    if (orderDir) {
      orderDir.dataset.dir = params.orderDir;
      orderDir.textContent = params.orderDir === 'desc' ? '↓' : '↑';
    }
  }

  function normalize(s){
    try{
      return (s||'')
        .toString()
        .toLowerCase()
        // remove diacritics
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        // treat underscores/hyphens as spaces
        .replace(/[_-]+/g, ' ')
        // collapse whitespace
        .replace(/\s+/g, ' ')
        .trim();
    }catch(_){
      return (s||'').toString().toLowerCase();
    }
  }

  function initOptions(){
    const setCountry = new Set(), setCity = new Set(), setYear = new Set();
    tiles.forEach(t=>{
      if(t.dataset.country) setCountry.add(t.dataset.country);
      if(t.dataset.city) setCity.add(t.dataset.city);
      if(t.dataset.year) setYear.add(t.dataset.year);
    });
    [...setCountry].sort().forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; filterCountry.appendChild(o); });
    [...setCity].sort().forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; filterCity.appendChild(o); });
    [...setYear].sort((a,b)=>Number(b||0)-Number(a||0)).forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; filterYear.appendChild(o); });
  }

  // Disable options that are not compatible with current selections across selects (exclusive filters)
  function updateSelectStates(){
    const q = normalize(input && input.value);
    const selCountry = normalize(filterCountry.value);
    const selCity = normalize(filterCity.value);
    const selYear = (filterYear.value || '');
    const selType = normalize(filterType.value);

    // Helper that mirrors the main filter() predicate, but allows excluding a specific key
    function tilePasses(t, excludeKey){
      // text search
      if(q){
        const hay = [t.dataset.title, t.dataset.webpage, t.dataset.desc, t.dataset.comments, t.dataset.file].map(normalize).join(' ');
        if(!hay.includes(q)) return false;
      }
      // select-based filters (skip the one we're evaluating options for)
      if(excludeKey !== 'country' && selCountry && normalize(t.dataset.country) !== selCountry) return false;
      if(excludeKey !== 'city' && selCity && normalize(t.dataset.city) !== selCity) return false;
      if(excludeKey !== 'year' && selYear && (t.dataset.year !== selYear)) return false;
      if(excludeKey !== 'type' && selType && normalize(t.dataset.type) !== selType) return false;
      // numeric ranges
      if(!passesNumeric(t.dataset.votes || 0, votesMin, votesMax)) return false;
      if(!passesNumeric(t.dataset.projects || 0, projectsMin, projectsMax)) return false;
      if(!passesNumeric(t.dataset.vlen || NaN, lenMin, lenMax)) return false;
      // include/exclude checkboxes
      if(excludeFully.checked && t.dataset.fully === '1') return false;
      if(excludeExperimental.checked && t.dataset.experimental === '1') return false;
      if(requireGeo && requireGeo.checked && t.dataset.geo !== '1') return false;
      if(requireTarget && requireTarget.checked && t.dataset.target !== '1') return false;
      if(requireCategory && requireCategory.checked && t.dataset.category !== '1') return false;
      return true;
    }

    function eligibleTiles(excludeKey){
      return tiles.filter(t => tilePasses(t, excludeKey));
    }

    function disableOptions(selectEl, key){
      if(!selectEl) return;
      const currentValue = selectEl.value;
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
        // Keep currently selected option enabled to avoid trapping the user
        if(opt.value === currentValue){ opt.disabled = false; return; }
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
    
    // Track search queries (debounced to avoid too many events)
    if (q && window.pabulibTrack) {
      clearTimeout(window.searchTrackTimeout);
      window.searchTrackTimeout = setTimeout(() => {
        window.pabulibTrack.search(input.value.trim(), visible);
      }, 1000);
    }
    // hide all by default; reveal during pagination
    tiles.forEach(t=>{
      const hay = [t.dataset.title, t.dataset.webpage, t.dataset.desc, t.dataset.comments, t.dataset.file]
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
      if(requireTarget && requireTarget.checked && t.dataset.target !== '1') { t.hidden=true; return; }
      if(requireCategory && requireCategory.checked && t.dataset.category !== '1') { t.hidden=true; return; }
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
    
    // Track filter usage
    if (window.pabulibTrack) {
      const activeFilters = [];
      if (country) activeFilters.push(`country:${country}`);
      if (city) activeFilters.push(`city:${city}`);
      if (year) activeFilters.push(`year:${year}`);
      if (type) activeFilters.push(`type:${type}`);
      if (excludeFully.checked) activeFilters.push('exclude_fully_funded');
      if (excludeExperimental.checked) activeFilters.push('exclude_experimental');
      if (requireGeo && requireGeo.checked) activeFilters.push('require_geo');
      if (requireTarget && requireTarget.checked) activeFilters.push('require_target');
      if (requireCategory && requireCategory.checked) activeFilters.push('require_category');
      
      if (activeFilters.length > 0) {
        window.pabulibTrack.filterUsage(activeFilters, visible);
      }
    }
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
  const selectionSize = selected.length;
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
      (requireGeo && requireGeo.checked) ||
      (requireTarget && requireTarget.checked) ||
      (requireCategory && requireCategory.checked)
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
        // Trigger file download with snapshot notification
        try{
          if(bar) bar.style.width = '100%';
          if(pct) pct.textContent = '100%';
          if(text) text.textContent = 'Starting download...';
          
          // Fetch the file to get snapshot headers
          const response = await fetch(fileUrl);
          if (!response.ok) {
            throw new Error(`Download failed: ${response.status}`);
          }
          const blob = await response.blob();
          
          // Check for snapshot headers and show notification (skip for single-file)
          if (selectionSize > 1) {
            handleSnapshotInfo(response);
          }
          
          // Trigger download
          const url = window.URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = url;
          a.download = (d.download_name || 'pb_selected.zip');
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          window.URL.revokeObjectURL(url);
        } catch (error) {
          console.error('Download error:', error);
          // Fallback to simple download link
          const a = document.createElement('a');
          a.href = fileUrl;
          a.download = (d.download_name || 'pb_selected.zip');
          document.body.appendChild(a);
          a.click();
          a.remove();
        } finally{
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
    clearTimeout(tHandle); tHandle = setTimeout(()=>{ updateSelectStates(); filter(); updateURL(); }, 100);
  }

  [input, filterCountry, filterCity, filterYear, votesMin, votesMax, projectsMin, projectsMax, lenMin, lenMax, filterType, excludeFully, excludeExperimental, requireGeo, requireTarget, requireCategory]
    .forEach(el => {
      el.addEventListener('input', debounced);
      // Also listen for 'change' event on checkboxes and selects
      if (el.type === 'checkbox' || el.tagName === 'SELECT') {
        el.addEventListener('change', debounced);
      }
    });
  orderBy.addEventListener('change', ()=>{ 
    if (window.pabulibTrack) {
      window.pabulibTrack.sortChange(orderBy.value, orderDir.dataset.dir);
    }
    sortTiles(); visibleCount = 0; revealNext(); updateURL(); 
  });
  orderDir.addEventListener('click', ()=>{
    orderDir.dataset.dir = (orderDir.dataset.dir === 'desc') ? 'asc' : 'desc';
    orderDir.textContent = (orderDir.dataset.dir === 'desc') ? '↓' : '↑';
    if (window.pabulibTrack) {
      window.pabulibTrack.sortChange(orderBy.value, orderDir.dataset.dir);
    }
    sortTiles();
    visibleCount = 0;
    revealNext();
    updateURL();
  });

  // initial: default to Quality, descending (bigger score first)
  orderBy.value = 'quality';
  orderDir.dataset.dir = 'desc';
  orderDir.textContent = '↓';
  initOptions();
  
  // Apply URL parameters if present
  applyURLParams();
  
  updateSelectStates();
  filter(); // Apply filters after setting URL parameters
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
      if(requireTarget) requireTarget.checked=false;
      if(requireCategory) requireCategory.checked=false;
      updateSelectStates();
      filter();
      updateURL();
      input.focus();
    });
  }
  // pagination
  function revealNext(){
    // Use current DOM order so pagination respects the latest sort order
    const parent = document.querySelector('#downloadForm') || document;
    const eligible = Array.from(parent.querySelectorAll('.tile')).filter(t => !t.hidden);
    const end = Math.min(eligible.length, visibleCount + PAGE);
    const itemsLoaded = end - visibleCount;
    eligible.forEach((t, idx) => {
      t.style.display = (idx < end) ? '' : 'none';
    });
    visibleCount = end;
    
    // Track pagination/lazy loading when new items are loaded
    if (itemsLoaded > 0 && window.pabulibTrack) {
      window.pabulibTrack.paginationLoad(itemsLoaded, eligible.length);
    }
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
    
    // Responsive width for mini popup
    const isMobile = window.innerWidth <= 768;
    const popupWidth = isMobile ? Math.min(320, window.innerWidth - 32) : 360;
    const left = Math.min(window.innerWidth - 16 - popupWidth, Math.max(8, r.right - popupWidth));
    
    // mini is position: fixed -> use viewport coordinates, no scroll offsets
    mini.style.top = `${top}px`;
    mini.style.left = `${left}px`;
    mini.style.maxWidth = `${popupWidth}px`;
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
    
    // Track dataset hover
    if (window.pabulibTrack && tile.dataset.file) {
      window.hoverStartTime = Date.now();
      window.pabulibTrack.datasetHover(tile.dataset.file);
    }
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

  // Handle browser back/forward navigation
  window.addEventListener('popstate', () => {
    applyURLParams();
    updateSelectStates();
    filter();
  });

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
      if(window.innerWidth > 768) closeDrawer();
    });
  }
  // Snapshot Management ---------------------------------------------------
  
  /**
   * Check for snapshot information in download response headers
   * and display a notification with the permanent link
   */
  function handleSnapshotInfo(response) {
    const snapshotId = response.headers.get('X-Download-Snapshot-ID');
    const snapshotUrl = response.headers.get('X-Download-Snapshot-URL');
    
    if (snapshotId && snapshotUrl) {
      showSnapshotNotification(snapshotId, snapshotUrl);
    }
  }
  
  /**
   * Show a notification with snapshot information
   */
  function showSnapshotNotification(snapshotId, snapshotUrl) {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = 'snapshot-notification';
    notification.innerHTML = `
      <div class="snapshot-content">
        <h4>Download Saved!</h4>
        <p>Your download has been saved with a permanent link:</p>
        <div class="snapshot-link-container">
          <input type="text" readonly value="${snapshotUrl}" class="snapshot-link" id="snapshot-link-${snapshotId}">
          <button type="button" class="copy-btn" data-target="snapshot-link-${snapshotId}">Copy</button>
        </div>
        <small>This link will always point to the exact same files, even if they're updated later.</small>
        <button type="button" class="close-btn" onclick="this.parentElement.parentElement.remove()">×</button>
      </div>
    `;
    
    // Add styles if not already present
    if (!document.querySelector('#snapshot-styles')) {
      const styles = document.createElement('style');
      styles.id = 'snapshot-styles';
      styles.textContent = `
        .snapshot-notification {
          position: fixed;
          top: 20px;
          right: 20px;
          background: #fff;
          border: 1px solid #ddd;
          border-radius: 8px;
          box-shadow: 0 4px 12px rgba(0,0,0,0.15);
          padding: 16px;
          max-width: 400px;
          z-index: 1000;
          animation: slideIn 0.3s ease;
        }
        .snapshot-content h4 {
          margin: 0 0 8px 0;
          color: #059669;
        }
        .snapshot-content p {
          margin: 0 0 12px 0;
          font-size: 14px;
        }
        .snapshot-link-container {
          display: flex;
          gap: 8px;
          margin-bottom: 8px;
        }
        .snapshot-link {
          flex: 1;
          padding: 6px 8px;
          border: 1px solid #ddd;
          border-radius: 4px;
          font-family: monospace;
          font-size: 12px;
        }
        .copy-btn {
          padding: 6px 12px;
          background: #059669;
          color: white;
          border: none;
          border-radius: 4px;
          cursor: pointer;
          font-size: 12px;
        }
        .copy-btn:hover {
          background: #047857;
        }
        .close-btn {
          position: absolute;
          top: 8px;
          right: 12px;
          background: none;
          border: none;
          font-size: 18px;
          cursor: pointer;
          color: #666;
        }
        .close-btn:hover {
          color: #333;
        }
        .snapshot-content small {
          color: #666;
          font-size: 12px;
          display: block;
        }
        @keyframes slideIn {
          from { transform: translateX(100%); }
          to { transform: translateX(0); }
        }
      `;
      document.head.appendChild(styles);
    }
    
    // Add to page
    document.body.appendChild(notification);
    
    // Add copy functionality
    notification.querySelector('.copy-btn').addEventListener('click', function() {
      const input = notification.querySelector('.snapshot-link');
      input.select();
      document.execCommand('copy');
      
      // Show feedback
      this.textContent = 'Copied!';
      setTimeout(() => {
        this.textContent = 'Copy';
      }, 2000);
    });
    
    // Auto-remove after 30 seconds
    setTimeout(() => {
      if (notification.parentElement) {
        notification.remove();
      }
    }, 30000);
  }
  
  /**
   * Intercept download links to check for snapshot headers
   */
  function interceptDownloads() {
    // Intercept single file downloads
    $$('a[href^="/download/"]').forEach(link => {
      if (link.href.includes('/download/snapshot/')) return; // Skip snapshot links
      
      link.addEventListener('click', async function(e) {
        e.preventDefault();
        const isSingle = this.dataset && this.dataset.downloadType === 'single';
        const hrefUrl = this.href;

        function parseContentDispositionFilename(cd) {
          if (!cd) return null;
          try {
            // filename* (RFC 5987)
            const mStar = cd.match(/filename\*=([^;]+)/i);
            if (mStar && mStar[1]) {
              const v = mStar[1].trim();
              const parts = v.split("''");
              const rest = parts.length > 1 ? parts.slice(1).join("''") : v;
              try { return decodeURIComponent(rest); } catch(_) { return rest; }
            }
            // filename="..."
            const mQuoted = cd.match(/filename="([^"]+)"/i);
            if (mQuoted && mQuoted[1]) return mQuoted[1];
            // filename=...
            const mPlain = cd.match(/filename=([^;]+)/i);
            if (mPlain && mPlain[1]) return mPlain[1].trim();
          } catch(_){}
          return null;
        }
        function filenameFromUrl(u){
          try{
            const url = new URL(u, window.location.origin);
            const path = url.pathname;
            const base = path.substring(path.lastIndexOf('/')+1);
            return decodeURIComponent(base || 'download');
          }catch(_) { return 'download'; }
        }

        try {
          const response = await fetch(this.href);
          const blob = await response.blob();
          
          // Check for snapshot headers (skip for single-file direct downloads)
          if (!isSingle) {
            handleSnapshotInfo(response);
          }
          
          // Trigger download
          const url = window.URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = url;
          const cd = response.headers.get('Content-Disposition') || '';
          const cdName = parseContentDispositionFilename(cd);
          a.download = cdName || filenameFromUrl(hrefUrl);
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          window.URL.revokeObjectURL(url);
          
        } catch (error) {
          console.error('Download failed:', error);
          // Fallback to normal download
          window.location.href = this.href;
        }
      });
    });
  }
  
  // Initialize snapshot functionality
  if (typeof window !== 'undefined') {
    document.addEventListener('DOMContentLoaded', interceptDownloads);
    // Re-run when new content is loaded dynamically
    document.addEventListener('contentLoaded', interceptDownloads);
  }

})();

// Removed mobile tags shortening - labels are hidden on mobile now
