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
    const container = document.querySelector('.container');
    const items = $$('.tile');
    items.sort((a,b)=>{
      const av = (key==='quality')?Number(a.dataset.quality||0):
                 (key==='votes')?Number(a.dataset.votes):
                 (key==='projects')?Number(a.dataset.projects):
                 (key==='budget')?Number(a.dataset.budget||0):
                 (key==='year')?Number(a.dataset.year||0):
                 0; // quality placeholder
      const bv = (key==='quality')?Number(b.dataset.quality||0):
                 (key==='votes')?Number(b.dataset.votes):
                 (key==='projects')?Number(b.dataset.projects):
                 (key==='budget')?Number(b.dataset.budget||0):
                 (key==='year')?Number(b.dataset.year||0):
                 0;
      return (av-bv)*dir;
    });
    items.forEach(it=>container.appendChild(it));
  }

  function visibleRowChecks(){
    return $$('.tile').filter(t => !t.hidden && t.style.display !== 'none').map(t => t.querySelector('.row-check'));
  }

  function updateChecks(){
    const checks = visibleRowChecks();
    const anyChecked = checks.some(ch => ch.checked);
    const allChecked = checks.length > 0 && checks.every(ch => ch.checked);
    downloadBtn.disabled = !anyChecked;
    selectAll.indeterminate = anyChecked && !allChecked;
    if(checks.length){ selectAll.disabled = false; } else { selectAll.disabled = true; selectAll.checked = false; selectAll.indeterminate = false; }
  }

  selectAll.addEventListener('change', () => {
    const checks = visibleRowChecks();
    checks.forEach(ch => ch.checked = selectAll.checked);
    updateChecks();
  });
  document.addEventListener('change', (e) => {
    if(e.target && e.target.classList.contains('row-check')){
      updateChecks();
    }
  });

  form.addEventListener('submit', (e) => {
    // add hidden inputs for selected files
    const prev = form.querySelectorAll('input[name="files"]');
    prev.forEach(p => p.remove());
    const selected = $$('.row-check:checked');
    if(!selected.length){ e.preventDefault(); return; }
    selected.forEach(ch => {
      const inp = document.createElement('input');
      inp.type = 'hidden';
      inp.name = 'files';
      inp.value = ch.dataset.file;
      form.appendChild(inp);
    });
  });

  // listeners for controls
  // simple debounce for input-heavy changes
  let tHandle;
  function debounced(){
    clearTimeout(tHandle); tHandle = setTimeout(filter, 100);
  }

  [input, filterCountry, filterCity, filterYear, votesMin, votesMax, projectsMin, projectsMax, lenMin, lenMax, filterType, excludeFully, excludeExperimental]
    .forEach(el => el.addEventListener('input', debounced));
  orderBy.addEventListener('change', sortTiles);
  orderDir.addEventListener('click', ()=>{
    orderDir.dataset.dir = (orderDir.dataset.dir === 'desc') ? 'asc' : 'desc';
    orderDir.textContent = (orderDir.dataset.dir === 'desc') ? '↓' : '↑';
    sortTiles();
  });

  // initial
  orderDir.dataset.dir = 'asc';
  orderDir.textContent = '↑';
  initOptions();
  updateChecks();
  sortTiles();
  // pagination
  function revealNext(){
    const eligible = tiles.filter(t => !t.hidden);
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
})();
