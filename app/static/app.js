(function(){
  const $ = (s)=>document.querySelector(s);
  const $$ = (s)=>Array.from(document.querySelectorAll(s));
  
  // State
  let currentOffset = 0;
  let limit = 50;
  let isLoading = false;
  let hasMore = true;
  let totalCount = 0;
  let selectedFiles = new Set();
  let updateURLTimeout = null;
  let combinations = [];
  let isSelectAllActive = false;
  
  // Elements
  const container = document.getElementById('downloadForm');
  const sentinel = document.getElementById('sentinel');
  const countEl = $('#count');
  
  // Initialize totalCount from DOM if available
  if (countEl && countEl.textContent) {
      const parsed = parseInt(countEl.textContent.replace(/\D/g, ''), 10);
      if (!isNaN(parsed)) {
          totalCount = parsed;
      }
  }
  const selectAll = $('#selectAll');
  const downloadBtn = $('#downloadBtn');
  const filtersPanel = document.getElementById('filtersPanel');
  const openFiltersBtn = document.getElementById('openFilters');
  const closeFiltersBtn = document.getElementById('closeFilters');
  let filtersBackdrop = null;
  
  // Filters
  const filters = {
    search: $('#search'),
    country: $('#filterCountry'),
    city: $('#filterCity'),
    year: $('#filterYear'),
    votesMin: $('#votesMin'),
    votesMax: $('#votesMax'),
    projectsMin: $('#projectsMin'),
    projectsMax: $('#projectsMax'),
    lenMin: $('#lenMin'),
    lenMax: $('#lenMax'),
    type: $('#filterType'),
    excludeFully: $('#excludeFully'),
    excludeExperimental: $('#excludeExperimental'),
    requireGeo: $('#requireGeo'),
    requireTarget: $('#requireTarget'),
    requireCategory: $('#requireCategory'),
    orderBy: $('#orderBy'),
    orderDir: $('#orderDir')
  };

  // Helper to get filter values
  function getFilterValues() {
    return {
      search: filters.search ? filters.search.value.trim() : '',
      country: filters.country ? filters.country.value : '',
      city: filters.city ? filters.city.value : '',
      year: filters.year ? filters.year.value : '',
      votes_min: filters.votesMin ? filters.votesMin.value : '',
      votes_max: filters.votesMax ? filters.votesMax.value : '',
      projects_min: filters.projectsMin ? filters.projectsMin.value : '',
      projects_max: filters.projectsMax ? filters.projectsMax.value : '',
      len_min: filters.lenMin ? filters.lenMin.value : '',
      len_max: filters.lenMax ? filters.lenMax.value : '',
      type: filters.type ? filters.type.value : '',
      exclude_fully: filters.excludeFully ? filters.excludeFully.checked : false,
      exclude_experimental: filters.excludeExperimental ? filters.excludeExperimental.checked : false,
      require_geo: filters.requireGeo ? filters.requireGeo.checked : false,
      require_target: filters.requireTarget ? filters.requireTarget.checked : false,
      require_category: filters.requireCategory ? filters.requireCategory.checked : false,
      order_by: filters.orderBy ? filters.orderBy.value : 'quality',
      order_dir: filters.orderDir ? (filters.orderDir.dataset.dir || 'desc') : 'desc'
    };
  }

  function isAnyFilterActive() {
    const v = getFilterValues();
    if (v.search) return true;
    if (v.country) return true;
    if (v.city) return true;
    if (v.year) return true;
    if (v.votes_min || v.votes_max) return true;
    if (v.projects_min || v.projects_max) return true;
    if (v.len_min || v.len_max) return true;
    if (v.type) return true;
    if (v.exclude_fully) return true;
    if (v.exclude_experimental) return true;
    if (v.require_geo) return true;
    if (v.require_target) return true;
    if (v.require_category) return true;
    return false;
  }

  function escapeHtml(text) {
    if (text == null) return '';
    return String(text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  // Render a single tile
  function renderTile(t) {
    // Helper for tags
    let tags = '';
    // Removed country/city/year tags to match server-side rendering
    if (t.has_geo) tags += `<span class="tag geo" title="Dataset includes project coordinates">Geo</span>`;
    if (t.has_category) tags += `<span class="tag category" title="Dataset includes project categories">Cat</span>`;
    if (t.has_target) tags += `<span class="tag target" title="Dataset includes project targets">Tgt</span>`;

    // Helper for vote type
    let voteTypeHtml = escapeHtml(t.vote_type || '');
    const vt = (t.vote_type || '').toLowerCase();
    if (vt === 'approval') {
        voteTypeHtml = `app.`;
        if (t.approval_knapsack) voteTypeHtml += `, <code>knapsack</code>`;
        else if (t.approval_k_label) voteTypeHtml += `, <code>${escapeHtml(t.approval_k_label)}</code>`;
    } else if (vt === 'ordinal') {
        voteTypeHtml = `ord.`;
        if (t.ordinal_k_label) voteTypeHtml += `, <code>${escapeHtml(t.ordinal_k_label)}</code>`;
    } else if (vt === 'cumulative') {
        voteTypeHtml = `cml.`;
        if (t.cumulative_points_label) voteTypeHtml += `, <code>${escapeHtml(t.cumulative_points_label)}</code>`;
    }

    // Helper for budget
    let budgetHtml = '';
    if (t.budget) {
        if (t.currency) {
            budgetHtml = escapeHtml(t.budget.replace(t.currency, '').replace('€','').replace('$','').trim());
        } else {
            budgetHtml = escapeHtml(t.budget.replace('€','').replace('$','').trim());
        }
    }
    // Note: currency is in header in original template, but here we put it in grid too? 
    // Original: <div>Budget{% if t.currency %} {{ t.currency }}{% endif %}</div>
    // Original grid value: {{ t.budget|replace... }}
    
    const isSelected = selectedFiles.has(t.file_name);

    const dataAttrs = `
        data-title="${escapeHtml(t.title)}"
        data-webpage="${escapeHtml(t.webpage_name)}"
        data-desc="${escapeHtml(t.description)}"
        data-file="${escapeHtml(t.file_name)}"
        data-comments="${escapeHtml((t.comments || []).join(' '))}"
        data-country="${escapeHtml(t.country)}"
        data-city="${escapeHtml(t.city)}"
        data-year="${escapeHtml(t.year)}"
        data-votes="${t.num_votes_raw != null ? t.num_votes_raw : ''}"
        data-projects="${t.num_projects_raw != null ? t.num_projects_raw : ''}"
        data-budget="${t.budget_raw != null ? t.budget_raw : ''}"
        data-currency="${escapeHtml(t.currency)}"
        data-type="${escapeHtml(t.vote_type)}"
        data-aklabel="${escapeHtml(t.approval_k_label || '')}"
        data-aktype="${escapeHtml(t.approval_k_type || '')}"
        data-aknapsack="${t.approval_knapsack ? '1' : '0'}"
        data-cpts="${escapeHtml(t.cumulative_points_label || '')}"
        data-vlen="${t.vote_length_raw != null ? t.vote_length_raw.toFixed(3) : ''}"
        data-fully="${t.fully_funded ? '1' : '0'}"
        data-experimental="${t.experimental ? '1' : '0'}"
        data-quality="${t.quality.toFixed(6)}"
        data-geo="${t.has_geo ? '1' : '0'}"
        data-target="${t.has_target ? '1' : '0'}"
        data-category="${t.has_category ? '1' : '0'}"
        ${t.num_selected_projects_raw != null ? `data-selected="${t.num_selected_projects_raw}"` : ''}
        data-rule="${escapeHtml(t.rule_raw)}"
        data-edition="${escapeHtml(t.edition)}"
        data-language="${escapeHtml(t.language)}"
    `;

    return `
      <section class="tile" ${dataAttrs}>
        <div class="tile-header">
          <input class="row-check" type="checkbox" data-file="${escapeHtml(t.file_name)}" ${isSelected ? 'checked' : ''} />
          <div class="title-row">
            <h2>${escapeHtml(t.title).replace(/_/g, ' ')}</h2>
            <div class="labels">
                ${t.experimental ? '<span class="tag exp" title="Dataset marked as experimental">Experimental</span>' : ''}
                ${t.fully_funded ? '<span class="tag funded" title="All selected projects are funded">Fully funded</span>' : ''}
                ${tags}
            </div>
          </div>
          <span class="qs" title="QS = (avg vote length)³ × (projects)² × (votes). Higher is better.">QS ${t.quality_short}</span>
          <a class="doc" href="/preview/${t.file_name}" title="Preview" data-track="file_preview" data-track-category="engagement"></a>
          <a class="visualization" href="/visualize/${t.file_name}" title="Visualize" data-track="file_visualize" data-track-category="engagement"></a>
          <a class="download" href="/download/${t.file_name}" title="Download" 
             data-track="file_download" data-track-category="download" 
             data-filename="${t.file_name}" data-download-type="single"></a>
        </div>
        <!-- Mobile compact meta row (hidden on desktop) -->
        <div class="mobile-meta" aria-hidden="true">
          <div>
            <span># votes</span>
            <strong>${t.num_votes}</strong>
          </div>
          <div>
            <span># projects</span>
            <strong>${t.num_projects}</strong>
          </div>
          <div>
            <span>Vote length</span>
            <strong>${t.vote_length}</strong>
          </div>
        </div>
        <div class="grid head">
          <div>Description</div>
          <div>Votes</div>
          <div>Projects</div>
          <div>Budget${t.currency ? ' ' + escapeHtml(t.currency) : ''}</div>
          <div>Vote type</div>
          <div>Vote length</div>
        </div>
        <div class="grid">
          <div class="muted">${escapeHtml(t.description)}</div>
          <div>${t.num_votes}</div>
          <div>${t.num_projects}</div>
          <div>${budgetHtml}</div>
          <div>${voteTypeHtml}</div>
          <div>${t.vote_length}</div>
        </div>
        ${(t.comments && t.comments.length > 0) ? `
        <div style="padding: 0 16px 8px 16px;">
          <details class="mt-1 comments-block">
            <summary class="inline-flex items-center gap-1 text-[11px] px-2 py-0.5 rounded bg-slate-100 text-slate-700 border border-slate-200 cursor-pointer comments-toggle" onclick="event.stopPropagation()" title="Parsed from META 'comment' using #n: markers">${t.comments.length} comment${t.comments.length !== 1 ? 's' : ''}</summary>
            <ul class="mt-1.5 text-[11px] text-slate-700 space-y-0.5">
              ${t.comments.slice(0, 3).map(c => `<li class="flex items-start gap-1"><span class="text-slate-400">•</span><span>${escapeHtml(c)}</span></li>`).join('')}
            </ul>
            ${t.comments.length > 3 ? `
            <details class="mt-1">
              <summary class="text-[11px] text-indigo-600 cursor-pointer" onclick="event.stopPropagation()">Show ${t.comments.length - 3} more</summary>
              <ul class="mt-1 text-[11px] text-slate-700 space-y-1">
                ${t.comments.slice(3).map(c => `<li class="flex items-start gap-1"><span class="text-slate-400">•</span><span>${escapeHtml(c)}</span></li>`).join('')}
              </ul>
            </details>
            ` : ''}
          </details>
        </div>
        ` : ''}
      </section>
    `;
  }

  function updateURL(params) {
    if (updateURLTimeout) clearTimeout(updateURLTimeout);
    updateURLTimeout = setTimeout(() => {
      const cleanParams = new URLSearchParams();
      for (const [key, value] of params.entries()) {
        if (value === '' || value === null || value === undefined) continue;
        if (value === 'false') continue;
        if (key === 'order_by' && value === 'quality') continue;
        if (key === 'order_dir' && value === 'desc') continue;
        if (key === 'limit') continue;
        if (key === 'offset') continue;
        cleanParams.set(key, value);
      }
      const newURL = window.location.pathname + (cleanParams.toString() ? '?' + cleanParams.toString() : '');
      history.replaceState({}, '', newURL);
    }, 300);
  }

  function restoreFilters() {
    const params = new URLSearchParams(window.location.search);
    let hasFilters = false;

    const set = (el, paramName) => {
        if (!el) return;
        const val = params.get(paramName);
        if (val) {
            el.value = val;
            hasFilters = true;
        }
    };
    
    const setCheck = (el, paramName) => {
        if (!el) return;
        const val = params.get(paramName);
        if (val === 'true') {
            el.checked = true;
            hasFilters = true;
        }
    };

    set(filters.search, 'search');
    set(filters.country, 'country');
    set(filters.city, 'city');
    set(filters.year, 'year');
    set(filters.votesMin, 'votes_min');
    set(filters.votesMax, 'votes_max');
    set(filters.projectsMin, 'projects_min');
    set(filters.projectsMax, 'projects_max');
    set(filters.lenMin, 'len_min');
    set(filters.lenMax, 'len_max');
    set(filters.type, 'type');
    
    setCheck(filters.excludeFully, 'exclude_fully');
    setCheck(filters.excludeExperimental, 'exclude_experimental');
    setCheck(filters.requireGeo, 'require_geo');
    setCheck(filters.requireTarget, 'require_target');
    setCheck(filters.requireCategory, 'require_category');
    
    set(filters.orderBy, 'order_by');
    
    const dir = params.get('order_dir');
    if (dir && filters.orderDir) {
        filters.orderDir.dataset.dir = dir;
        filters.orderDir.textContent = dir === 'asc' ? '↑' : '↓';
        if (dir !== 'desc') hasFilters = true;
    }
    
    return hasFilters;
  }

  async function fetchOptions() {
    try {
        const res = await fetch('/api/options');
        const data = await res.json();
        
        combinations = data.combinations || [];
        
        if (filters.country) {
            data.countries.forEach(c => {
                const o = document.createElement('option');
                o.value = c;
                o.textContent = c;
                filters.country.appendChild(o);
            });
        }
        
        if (filters.city) {
            data.cities.forEach(c => {
                const o = document.createElement('option');
                o.value = c;
                o.textContent = c;
                filters.city.appendChild(o);
            });
        }
        
        if (filters.year) {
            data.years.forEach(y => {
                const o = document.createElement('option');
                o.value = y;
                o.textContent = y;
                filters.year.appendChild(o);
            });
        }
        
    } catch (e) {
        console.error("Failed to fetch options", e);
    }
  }

  async function fetchTiles(reset = false) {
    if (isLoading) return;
    if (reset) {
        currentOffset = 0;
        hasMore = true;
        $$('.tile').forEach(el => el.remove());
        
        // Clear selection on filter change
        selectedFiles.clear();
        isSelectAllActive = false;
        if (selectAll) selectAll.checked = false;
        updateSelectionUI();
    }
    if (!hasMore) return;

    isLoading = true;
    if (container) container.classList.add('loading');

    const params = new URLSearchParams(getFilterValues());
    params.set('offset', currentOffset);
    params.set('limit', limit);

    try {
        const res = await fetch(`/api/search?${params.toString()}`);
        const data = await res.json();
        
        totalCount = data.total;
        if (countEl) countEl.textContent = totalCount;

        if (data.tiles.length < limit) {
            hasMore = false;
        }

        const html = data.tiles.map(renderTile).join('');
        if (sentinel) sentinel.insertAdjacentHTML('beforebegin', html);
        
        currentOffset += data.tiles.length;
        
        // Re-attach event listeners for new elements (checkboxes, etc)
        interceptDownloads();
        
        if (isSelectAllActive) {
            $$('.row-check').forEach(cb => {
                if (!cb.checked) {
                    cb.checked = true;
                    selectedFiles.add(cb.dataset.file);
                }
            });
            updateSelectionUI();
        }
        
        updateURL(params);

    } catch (e) {
        console.error("Failed to fetch tiles", e);
    } finally {
        isLoading = false;
        if (container) container.classList.remove('loading');
    }
  }

  function updateFilterAvailability() {
    if (!combinations || combinations.length === 0) return;

    const selCountry = filters.country ? filters.country.value : '';
    const selCity = filters.city ? filters.city.value : '';
    const selYear = filters.year ? filters.year.value : '';

    const isValid = (c, u, y, ignoreField) => {
        if (ignoreField !== 'country' && selCountry && c !== selCountry) return false;
        if (ignoreField !== 'city' && selCity && u !== selCity) return false;
        if (ignoreField !== 'year' && selYear && y !== selYear) return false;
        return true;
    };

    if (filters.country) {
        Array.from(filters.country.options).forEach(opt => {
            if (!opt.value) return;
            const exists = combinations.some(comb => 
                comb.c === opt.value && isValid(comb.c, comb.u, comb.y, 'country')
            );
            opt.disabled = !exists;
            opt.style.color = exists ? '' : '#ccc';
        });
    }

    if (filters.city) {
        Array.from(filters.city.options).forEach(opt => {
            if (!opt.value) return;
            const exists = combinations.some(comb => 
                comb.u === opt.value && isValid(comb.c, comb.u, comb.y, 'city')
            );
            opt.disabled = !exists;
            opt.style.color = exists ? '' : '#ccc';
        });
    }

    if (filters.year) {
        Array.from(filters.year.options).forEach(opt => {
            if (!opt.value) return;
            const exists = combinations.some(comb => 
                comb.y === opt.value && isValid(comb.c, comb.u, comb.y, 'year')
            );
            opt.disabled = !exists;
            opt.style.color = exists ? '' : '#ccc';
        });
    }
  }

  function updateFilters() {
    updateFilterAvailability();
    fetchTiles(true);
  }

  // Event Listeners
  Object.values(filters).forEach(el => {
    if (!el) return;
    if (el.tagName === 'SELECT' || el.type === 'checkbox') {
        el.addEventListener('change', updateFilters);
    } else {
        // Debounce text inputs
        let timeout;
        el.addEventListener('input', () => {
            clearTimeout(timeout);
            const inputMode = (el.getAttribute('inputmode') || '').toLowerCase();
            const delay = (inputMode === 'numeric' || inputMode === 'decimal') ? 700 : 500;
            timeout = setTimeout(updateFilters, delay);
        });
    }
  });

  if (filters.orderDir) {
      filters.orderDir.addEventListener('click', () => {
          const current = filters.orderDir.dataset.dir || 'desc';
          const next = current === 'desc' ? 'asc' : 'desc';
          filters.orderDir.dataset.dir = next;
          filters.orderDir.textContent = next === 'desc' ? '↓' : '↑'; // Or whatever icon
          updateFilters();
      });
  }

  if (document.getElementById('filtersClear')) {
      document.getElementById('filtersClear').addEventListener('click', () => {
          // Reset all filters
          if (filters.search) filters.search.value = '';
          if (filters.country) filters.country.value = '';
          // ... reset others ...
          // For brevity, just reload page or reset manually
          window.location.href = '/';
      });
  }

  // Infinite Scroll
  if (sentinel) {
      const observer = new IntersectionObserver((entries) => {
          if (entries[0].isIntersecting && hasMore && !isLoading) {
              fetchTiles(false);
          }
      });
      observer.observe(sentinel);
  }

  // Selection Logic
  if (container) {
      container.addEventListener('click', (e) => {
          // Handle tile click for selection (ignoring interactive elements)
          const tile = e.target.closest('.tile');
          if (tile) {
              // Ignore clicks on links, buttons, inputs, details/summary
              if (e.target.closest('a') || 
                  e.target.closest('button') || 
                  e.target.closest('input') || 
                  e.target.closest('summary') ||
                  e.target.closest('details')) {
                  return;
              }
              
              const checkbox = tile.querySelector('.row-check');
              if (checkbox) {
                  checkbox.checked = !checkbox.checked;
                  const file = checkbox.dataset.file;
                  if (checkbox.checked) {
                      selectedFiles.add(file);
                  } else {
                      selectedFiles.delete(file);
                      if (isSelectAllActive) {
                          isSelectAllActive = false;
                          if (selectAll) selectAll.checked = false;
                      }
                  }
                  updateSelectionUI();
              }
          }
      });

      container.addEventListener('change', (e) => {
          if (e.target.classList.contains('row-check')) {
              const file = e.target.dataset.file;
              if (e.target.checked) {
                  selectedFiles.add(file);
              } else {
                  selectedFiles.delete(file);
                  if (isSelectAllActive) {
                      isSelectAllActive = false;
                      if (selectAll) selectAll.checked = false;
                  }
              }
              updateSelectionUI();
          }
      });
  }

  if (selectAll) {
      selectAll.addEventListener('change', (e) => {
          const checked = e.target.checked;
          isSelectAllActive = checked;
          
          // If global select all is active, we don't need to manually add every file to the set
          // But we should visually check visible boxes
          $$('.row-check').forEach(cb => {
              cb.checked = checked;
              const file = cb.dataset.file;
              if (checked) selectedFiles.add(file); else selectedFiles.delete(file);
          });
          updateSelectionUI();
      });
  }

  function updateSelectionUI() {
      const count = selectedFiles.size;
      const isGlobal = isSelectAllActive;

      if (downloadBtn) {
          if (isGlobal) {
             downloadBtn.textContent = `Download ${totalCount} files`;
             downloadBtn.disabled = false;
          } else {
             downloadBtn.textContent = count > 0 ? `Download ${count} selected file${count === 1 ? '' : 's'}` : 'Download selected';
             downloadBtn.disabled = count === 0;
          }
      }
      
      // Sync hidden inputs for form submission
      // Remove existing hidden inputs
      $$('input[type="hidden"][name="files"]').forEach(el => el.remove());
      
      // Add new hidden inputs for selected files
      const form = document.getElementById('downloadForm');
      if (form) {
          // If global select all, we don't need individual files
          if (!isGlobal) {
              selectedFiles.forEach(file => {
                  const input = document.createElement('input');
                  input.type = 'hidden';
                  input.name = 'files';
                  input.value = file;
                  form.appendChild(input);
              });
          }
      }
  }

  // Initial load handling
  // If tiles are already present (from server), set offset
  const initialTiles = $$('.tile');
  if (initialTiles.length > 0) {
      currentOffset = initialTiles.length;
      // Also populate selectedFiles from checked boxes
      $$('.row-check:checked').forEach(cb => selectedFiles.add(cb.dataset.file));
      updateSelectionUI();
  } else {
      // If no tiles, fetch first batch
      fetchTiles(false);
  }

  // Mobile Drawer
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
    document.addEventListener('keydown', (e)=>{
      if(e.key === 'Escape') closeDrawer();
    });
    window.addEventListener('resize', ()=>{
      if(window.innerWidth > 768) closeDrawer();
    });
  }

  // Snapshot & Download Logic
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
  
  function handleSnapshotInfo(response) {
    const snapshotId = response.headers.get('X-Download-Snapshot-ID');
    const snapshotUrl = response.headers.get('X-Download-Snapshot-URL');
    
    if (snapshotId && snapshotUrl) {
      showSnapshotNotification(snapshotId, snapshotUrl);
    }
  }
  
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

  function interceptDownloads() {
    // Intercept single file downloads
    $$('a.download').forEach(link => {
      if (link.href.includes('/download/snapshot/')) return; // Skip snapshot links
      if (link.dataset.intercepted) return; // Skip already intercepted
      link.dataset.intercepted = 'true';
      
      link.addEventListener('click', async function(e) {
        e.preventDefault();
        const isSingle = this.dataset && this.dataset.downloadType === 'single';
        const hrefUrl = this.href;

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
  
  // Mini Popover Logic
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

  // Download Form Handler (Progress Bar & Polling)
  const downloadForm = document.getElementById('downloadForm');
  if (downloadForm) {
      downloadForm.addEventListener('submit', async (e) => {
          e.preventDefault();
          
          if (selectedFiles.size === 0 && !isSelectAllActive) return;

          // Build form data
          const fd = new FormData(downloadForm);
          
          // Check if we should use "select_all" optimization
          if (isSelectAllActive) {
             fd.append('select_all', 'true');
             // If global select all, we don't need individual files
             fd.delete('files');
             
             // Append current filter values to FormData
             const filters = getFilterValues();
             for (const [key, value] of Object.entries(filters)) {
                 if (value !== '' && value !== null && value !== undefined && value !== false) {
                     fd.append(key, value);
                 }
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
          try {
            let url = '/download-selected/start';
            if (fd.get('select_all') === 'true') {
                url += (url.includes('?') ? '&' : '?') + 'select_all=true';
            }
            startResp = await fetch(url, { method: 'POST', body: fd });
          } catch(err) {
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
          
          // Poll progress
          let tries = 0;
          const maxTries = 600; // 10 minutes
          
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
            
            if(phase){ 
                phase.textContent = (d.status === 'ready') ? 'Ready' : 'Zipping'; 
                phase.className = (d.status === 'ready') ? 'px-2 py-1 rounded-full text-xs font-medium bg-emerald-100 text-emerald-700' : 'px-2 py-1 rounded-full text-xs font-medium bg-indigo-100 text-indigo-700'; 
            }
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
                 const dlUrl = startData.file_url || `/download-selected/file/${token}`;
                 const resp = await fetch(dlUrl);
                 if(resp.ok) {
                     handleSnapshotInfo(resp);
                     
                     // Try to get filename from Content-Disposition
                     const cd = resp.headers.get('Content-Disposition') || '';
                     const cdName = parseContentDispositionFilename(cd);
                     const finalName = cdName || d.download_name || 'pb_selected.zip';

                     const blob = await resp.blob();
                     const url = window.URL.createObjectURL(blob);
                     const a = document.createElement('a');
                     a.href = url;
                     a.download = finalName;
                     document.body.appendChild(a);
                     a.click();
                     document.body.removeChild(a);
                     window.URL.revokeObjectURL(url);
                 } else {
                     window.location.href = dlUrl;
                 }
              } catch(e) {
                  console.error(e);
                  window.location.href = startData.file_url || `/download-selected/file/${token}`;
              }
              
              setTimeout(() => {
                  if(box) box.classList.add('hidden');
              }, 3000);
              
              return;
            }
            
            await new Promise(res=>setTimeout(res, 500));
          }
          
          alert('Download timed out');
          if(box) box.classList.add('hidden');
      });
  }

  // Initialize
  if (typeof window !== 'undefined') {
    document.addEventListener('DOMContentLoaded', async () => {
        interceptDownloads();
        await fetchOptions();
        if (restoreFilters()) {
            fetchTiles(true);
        }
        updateFilterAvailability();
    });
    // Re-run when new content is loaded dynamically
    document.addEventListener('contentLoaded', interceptDownloads);
  }

})();
