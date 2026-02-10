(function() {
    function initEditorPage(pageData) {
        pageData = pageData || {};
        const editorRefreshUrl = pageData.editorRefreshUrl || '/editor/refresh';
        const editorDataUrl = pageData.editorDataUrl || '/editor_data';

    // Toast Notification Functions
    function showNotification(message, type = 'success', duration = 3000) {
        const toastType = type === 'danger' ? 'error' : type;
        if (typeof showToast === 'function') {
            showToast(message, toastType, duration);
        }
    }

    function escapeHtml(value) {
        return String(value || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function handleLogoLoadError(img) {
        if (!img) return;
        try {
            const alreadyRetried = img.dataset.logoFallbackTried === '1';
            if (alreadyRetried) {
                img.style.display = 'none';
                return;
            }

            const url = new URL(img.src, window.location.href);
            const isHttps = url.protocol === 'https:';
            const isIpv4 = /^\d{1,3}(\.\d{1,3}){3}$/.test(url.hostname);
            if (isHttps && isIpv4) {
                img.dataset.logoFallbackTried = '1';
                img.src = img.src.replace(/^https:\/\//i, 'http://');
                return;
            }
        } catch (e) {
            // ignore parse errors, just hide below
        }
        img.style.display = 'none';
    }

    // Confirmation Dialog Function
    function showConfirmDialog(options) {
        return new Promise((resolve) => {
            const modal = document.getElementById('confirmModal');
            const titleEl = document.getElementById('confirmModalTitle');
            const messageEl = document.getElementById('confirmModalMessage');
            const iconEl = document.getElementById('confirmModalIcon');
            const okBtn = document.getElementById('confirmModalOk');
            const okTextEl = document.getElementById('confirmModalOkText');

            // Set content
            titleEl.textContent = options.title || 'Confirm';
            messageEl.textContent = options.message || 'Are you sure?';
            okTextEl.textContent = options.okText || 'OK';

            // Set icon and button style based on type
            const type = options.type || 'warning';
            if (type === 'danger') {
                iconEl.innerHTML = '<i class="fas fa-exclamation-triangle fa-2x text-danger"></i>';
                okBtn.className = 'btn btn-danger';
            } else if (type === 'info') {
                iconEl.innerHTML = '<i class="fas fa-info-circle fa-2x text-info"></i>';
                okBtn.className = 'btn btn-primary';
            } else {
                iconEl.innerHTML = '<i class="fas fa-question-circle fa-2x text-warning"></i>';
                okBtn.className = 'btn btn-warning';
            }

            // Create bootstrap modal instance
            const bsModal = new bootstrap.Modal(modal);

            // Handle OK button click
            const handleOk = () => {
                okBtn.removeEventListener('click', handleOk);
                modal.removeEventListener('hidden.bs.modal', handleCancel);
                bsModal.hide();
                resolve(true);
            };

            // Handle Cancel/Close
            const handleCancel = () => {
                okBtn.removeEventListener('click', handleOk);
                resolve(false);
            };

            okBtn.addEventListener('click', handleOk);
            modal.addEventListener('hidden.bs.modal', handleCancel, { once: true });

            // Show modal
            bsModal.show();
        });
    }

    var enabledEdits = [];
    var numberEdits = [];
    var nameEdits = [];
    var groupEdits = [];
    var epgEdits = [];
    var dataTable;
    var allChannelNamesCount = {}; // Track all channel name frequencies for autocomplete
    var enabledChannelNamesCount = {}; // Track enabled channel name frequencies for duplicate detection

    function getPortalAttr(ele) {
        return (ele && (ele.dataset.portal || ele.getAttribute('data-portal'))) || '';
    }

    function getChannelIdAttr(ele) {
        if (!ele) return '';
        return ele.dataset.channelId
            || ele.getAttribute('data-channelid')
            || ele.getAttribute('data-channel-id')
            || ele.getAttribute('data-channelId')
            || '';
    }

    function getGroupChildRowFromElement(ele) {
        const tr = ele ? ele.closest('tr') : null;
        if (!tr) return null;
        // If called from an element inside the child row itself
        if (tr.querySelector('td[colspan]')) return tr;

        // If called from the group parent row, the child details row is usually the next sibling
        const nextTr = tr.nextElementSibling;
        if (nextTr && nextTr.querySelector && nextTr.querySelector('td[colspan]')) {
            return nextTr;
        }
        return null;
    }

    function getGroupSwitchForChildRow(childTr) {
        if (!childTr) return null;
        const parentTr = childTr.previousElementSibling;
        if (!parentTr) return null;
        return parentTr.querySelector('input.form-check-input[data-group-items]');
    }

    function getMemberSwitchesInChildRow(childTr) {
        if (!childTr) return [];
        return Array.from(
            childTr.querySelectorAll('td[colspan] input.form-check-input[data-portal], .group-cell-toggle input.form-check-input[data-portal]')
        );
    }

    function parseGroupItems(input) {
        if (!input) return [];
        const raw = input.dataset.groupItems || input.getAttribute('data-group-items') || '[]';
        if (!raw) return [];
        try {
            const parsed = JSON.parse(raw);
            return Array.isArray(parsed) ? parsed : [];
        } catch (e) {
            try {
                const decoded = raw.replace(/&quot;/g, '"');
                const parsed = JSON.parse(decoded);
                return Array.isArray(parsed) ? parsed : [];
            } catch (e2) {
                return [];
            }
        }
    }

    function syncGroupSwitchVisualStateFromChildRow(childTr) {
        const groupSwitch = getGroupSwitchForChildRow(childTr);
        const memberSwitches = getMemberSwitchesInChildRow(childTr);
        if (!groupSwitch || !memberSwitches.length) return;

        const checkedCount = memberSwitches.filter(sw => !!sw.checked).length;
        const allChecked = checkedCount === memberSwitches.length;
        const allUnchecked = checkedCount === 0;
        const mixed = !allChecked && !allUnchecked;

        groupSwitch.indeterminate = mixed;
        groupSwitch.checked = allChecked;
        groupSwitch.classList.toggle('is-mixed', mixed);
        if (mixed) {
            groupSwitch.setAttribute('data-mixed', '1');
        } else {
            groupSwitch.removeAttribute('data-mixed');
        }
    }

    function editAll(ele) {
        var checkboxes = document.getElementsByClassName('checkbox');
        var enable = ele.checked;
        for (var i = 0, n = checkboxes.length; i < n; i++) {
            if (i != 0) {
                checkboxes[i].checked = enable;
                checkboxes[i].onchange();
            }
        }
    }

    function editEnabled(ele) {
        var p = getPortalAttr(ele);
        var i = getChannelIdAttr(ele);
        var c = ele.checked;
        if (!p || !i) return;
        upsertEnabledEdit(p, i, c);

        const childTr = getGroupChildRowFromElement(ele);
        if (childTr) {
            syncGroupSwitchVisualStateFromChildRow(childTr);
        }
    }

    function upsertEnabledEdit(portal, channelId, enabled) {
        if (!portal || !channelId) return;
        const idx = enabledEdits.findIndex(
            e => e["portal"] === portal && e["channel id"] === channelId
        );
        const payload = { "portal": portal, "channel id": channelId, "enabled": !!enabled };
        if (idx >= 0) {
            enabledEdits[idx] = payload;
        } else {
            enabledEdits.push(payload);
        }
    }

    function editCustomNumber(ele) {
        var p = ele.getAttribute('data-portal');
        var i = ele.getAttribute('data-channelId');
        var c = ele.value;
        var j = { "portal": p, "channel id": i, "custom number": c };
        numberEdits.push(j);
    }

    function editCustomName(ele) {
        var p = ele.getAttribute('data-portal');
        var i = ele.getAttribute('data-channelId');
        var c = ele.value;
        var j = { "portal": p, "channel id": i, "custom name": c };
        nameEdits.push(j);
    }

    function editCustomGroup(ele) {
        var p = ele.getAttribute('data-portal');
        var i = ele.getAttribute('data-channelId');
        var c = ele.value;
        var j = { "portal": p, "channel id": i, "custom genre": c };
        groupEdits.push(j);
    }

    function editCustomEpgId(ele) {
        var p = ele.getAttribute('data-portal');
        var i = ele.getAttribute('data-channelId');
        var c = ele.value;
        var j = { "portal": p, "channel id": i, "custom epg id": c };
        epgEdits.push(j);
        updateEpgSourceHintForInput(ele);
    }

    let epgSuggestTimer = null;
    const epgSuggestCache = new Map();

    function renderEpgSuggestions(items) {
        const list = document.getElementById('epg-suggestions');
        if (!list) return;
        list.innerHTML = '';
        items.forEach(function(item) {
            const opt = document.createElement('option');
            opt.value = item.id;
            const labelParts = [];
            if (item.name) labelParts.push(item.name);
            if (item.source) labelParts.push(item.source);
            if (labelParts.length) {
                opt.label = labelParts.join(' — ');
            }
            list.appendChild(opt);
        });
    }

    function fetchEpgSuggestions(query, input) {
        if (!query) return;
        if (epgSuggestCache.has(query)) {
            renderEpgSuggestions(epgSuggestCache.get(query));
            return;
        }

        fetch(`/api/editor/epg/suggestions?q=${encodeURIComponent(query)}&limit=20`)
            .then(resp => resp.json())
            .then(data => {
                if (!data || !data.ok) return;
                const items = data.items || [];
                epgSuggestCache.set(query, items);
                renderEpgSuggestions(items);
            })
            .catch(() => {});
    }

    function fetchEpgSuggestionsByQueries(queries, input) {
        const cleaned = [];
        const seenQueries = new Set();
        (queries || []).forEach(q => {
            const value = (q || '').toString().trim();
            if (!value || seenQueries.has(value)) return;
            seenQueries.add(value);
            cleaned.push(value);
        });
        if (!cleaned.length) return;
        const seen = new Set();
        const merged = [];
        const fetches = cleaned.map(query => {
            if (epgSuggestCache.has(query)) {
                epgSuggestCache.get(query).forEach(item => {
                    if (!seen.has(item.id)) {
                        seen.add(item.id);
                        merged.push(item);
                    }
                });
                return Promise.resolve();
            }
            return fetch(`/api/editor/epg/suggestions?q=${encodeURIComponent(query)}&limit=20`)
                .then(resp => resp.json())
                .then(data => {
                    if (!data || !data.ok) return;
                    const items = data.items || [];
                    epgSuggestCache.set(query, items);
                    items.forEach(item => {
                        if (!seen.has(item.id)) {
                            seen.add(item.id);
                            merged.push(item);
                        }
                    });
                })
                .catch(() => {});
        });
        Promise.all(fetches).then(() => {
            if (merged.length) {
                renderEpgSuggestions(merged);
            }
        });
    }

    document.addEventListener('focusin', function(e) {
        if (!e.target.classList.contains('epg-suggest-input')) return;
        const stationId = e.target.dataset.stationId || '';
        const callSign = e.target.dataset.callSign || '';
        const channelId = e.target.dataset.channelId || '';
        const channelName = e.target.dataset.channelName || '';
        const query = e.target.value.trim();
        const queries = query
            ? [query, stationId, callSign, channelName, channelId]
            : [stationId, callSign, channelName, channelId];
        fetchEpgSuggestionsByQueries(queries, e.target);
        updateEpgSourceHintForInput(e.target);
    });

    document.addEventListener('input', function(e) {
        if (!e.target.classList.contains('epg-suggest-input')) return;
        const query = e.target.value.trim();
        const stationId = e.target.dataset.stationId || '';
        const callSign = e.target.dataset.callSign || '';
        const channelId = e.target.dataset.channelId || '';
        const channelName = e.target.dataset.channelName || '';
        clearTimeout(epgSuggestTimer);
        epgSuggestTimer = setTimeout(() => {
            const queries = query
                ? [query, stationId, callSign, channelName, channelId]
                : [stationId, callSign, channelName, channelId];
            fetchEpgSuggestionsByQueries(queries, e.target);
        }, 200);
    });

    function updateEpgSourceHintForInput(input) {
        if (!input) return;
        const wrapper = input.closest('.subline-epg');
        if (!wrapper) return;
        const hint = wrapper.querySelector('.epg-source-hint');
        if (!hint) return;
        const epgId = (input.value || '').trim();
        if (!epgId) {
            hint.textContent = 'Quelle: -';
            return;
        }
        fetch(`/api/editor/epg/source?id=${encodeURIComponent(epgId)}`)
            .then(resp => resp.json())
            .then(data => {
                if (!data || !data.ok) {
                    hint.textContent = 'Quelle: -';
                    return;
                }
                const sourceText = data.source ? `Quelle: ${data.source}` : 'Quelle: -';
                hint.textContent = sourceText;
            })
            .catch(() => {
                hint.textContent = 'Quelle: -';
            });
    }

    function updateEpgSourceHintForRow(row) {
        if (!row) return;
        if (typeof row.child !== 'function') return;
        const child = row.child();
        if (!child) return;
        let node = null;
        if (child instanceof HTMLElement) {
            node = child;
        } else if (child.get && child.get(0)) {
            node = child.get(0);
        } else if (child[0]) {
            node = child[0];
        }
        if (!node) return;
        const inputs = node.querySelectorAll('.epg-suggest-input');
        if (inputs && inputs.length) {
            inputs.forEach(input => updateEpgSourceHintForInput(input));
        }
    }

    function refreshEpgForIds(epgIds, opts = {}) {
        if (!Array.isArray(epgIds) || !epgIds.length) return;
        const payload = { epg_ids: epgIds };
        return fetch('/api/editor/epg/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(resp => resp.json())
        .then(data => {
            if (data && data.ok) {
                if (opts.successMessage) {
                    if (typeof showToast === 'function') {
                        showToast(opts.successMessage, 'success');
                    } else {
                        showNotification(opts.successMessage, 'success');
                    }
                }
            } else if (opts.errorMessage) {
                if (typeof showToast === 'function') {
                    showToast(opts.errorMessage + ': ' + (data.error || 'Unknown error'), 'error');
                } else {
                    showNotification(opts.errorMessage + ': ' + (data.error || 'Unknown error'), 'error', 5000);
                }
            }
        })
        .catch(err => {
            if (opts.errorMessage) {
                if (typeof showToast === 'function') {
                    showToast(opts.errorMessage + ': ' + err, 'error');
                } else {
                    showNotification(opts.errorMessage + ': ' + err, 'error', 5000);
                }
            }
        });
    }

    function refreshEpgForChannel(button) {
        const wrapper = button.closest('.subline-epg');
        if (!wrapper) return;
        const input = wrapper.querySelector('.epg-suggest-input');
        if (!input) return;
        const epgId = (input.value || '').trim();
        if (!epgId) {
            if (typeof showToast === 'function') {
                showToast('Bitte zuerst eine EPG-ID setzen.', 'warning');
            } else {
                showNotification('Bitte zuerst eine EPG-ID setzen.', 'warning', 4000);
            }
            return;
        }
        const original = button.innerHTML;
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
        refreshEpgForIds([epgId], {
            successMessage: 'EPG wurde aktualisiert.',
            errorMessage: 'EPG-Refresh fehlgeschlagen'
        }).finally(() => {
            button.disabled = false;
            button.innerHTML = original;
        });
    }


    function save() {
        const formData = new FormData();
        formData.append('enabledEdits', JSON.stringify(enabledEdits));
        formData.append('numberEdits', JSON.stringify(numberEdits));
        formData.append('nameEdits', JSON.stringify(nameEdits));
        formData.append('groupEdits', JSON.stringify(groupEdits));
        formData.append('epgEdits', JSON.stringify(epgEdits));

        // Show loading indicator
        var saveBtn = $('.dt-button:contains("Save")').first();
        var originalText = saveBtn.html();
        saveBtn.html('<i class="fas fa-spinner fa-spin"></i> Saving...').prop('disabled', true);

        fetch('/editor/save', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                if (typeof showToast === 'function') {
                    showToast(data.message || 'Playlist config saved!', 'success');
                } else {
                    showNotification(data.message || 'Playlist config saved!', 'success');
                }
                const pendingEpgEdits = epgEdits.slice();
                const responseEpgIds = Array.isArray(data.epg_ids) ? data.epg_ids : [];
                // Clear edit arrays
                enabledEdits = [];
                numberEdits = [];
                nameEdits = [];
                groupEdits = [];
                epgEdits = [];
                if (pendingEpgEdits.length) {
                    const epgIds = responseEpgIds.length
                        ? responseEpgIds
                        : [...new Set(pendingEpgEdits.map(e => (e["custom epg id"] || "").trim()).filter(Boolean))];
                    if (epgIds.length) {
                        refreshEpgForIds(epgIds, {
                            successMessage: 'EPG wurde aktualisiert.',
                            errorMessage: 'EPG-Refresh fehlgeschlagen'
                        });
                    }
                }
                // Reload table data without losing filters
                reloadEditorTable();
            } else {
                if (typeof showToast === 'function') {
                    showToast('Error: ' + (data.error || 'Unknown error'), 'error');
                } else {
                    showNotification('Error: ' + (data.error || 'Unknown error'), 'error', 5000);
                }
            }
        })
        .catch(error => {
            if (typeof showToast === 'function') {
                showToast('Error saving changes: ' + error, 'error');
            } else {
                showNotification('Error saving changes: ' + error, 'error', 5000);
            }
        })
        .finally(() => {
            saveBtn.html(originalText).prop('disabled', false);
        });
    }

    var player = document.getElementById("player")
    var title = document.getElementById("channelLabel")
    player.volume = 0.25
    function selectChannel(ele) {
        link = ele.getAttribute('data-link');
        player.src = link;
        channel = ele.getAttribute('data-customChannelName');
        if (channel == "") {
            channel = ele.getAttribute('data-autoChannelName') || ele.getAttribute('data-channelName');
        }
        title.innerHTML = channel
    }

    $('#videoModal').on('hidden.bs.modal', function () {
        player.src = "";
    })

    /* Create an array with the values of all the checkboxes in a column */
    $.fn.dataTable.ext.order['dom-checkbox'] = function (settings, col) {
        return this.api().column(col, { order: 'index' }).nodes().map(function (td, i) {
            return $('input', td).prop('checked') ? '1' : '0';
        });
    };

    /* Create an array with the values of all the input boxes in a column, parsed as numbers */
    $.fn.dataTable.ext.order['dom-text-numeric'] = function (settings, col) {
        return this.api().column(col, { order: 'index' }).nodes().map(function (td, i) {
            var val = $('input', td).val();
            return val === '' ? $('input', td).attr('placeholder') : val * 1;
        });
    };

    /* Create an array with the values of all the text boxes in a column */
    $.fn.dataTable.ext.order['dom-text'] = function (settings, col) {
        return this.api().column(col, { order: 'index' }).nodes().map(function (td, i) {
            var val = $('input', td).val();
            return val === '' ? $('input', td).attr('placeholder') : val;
        });
    };

    // Tom Select instances
    let portalSelect, groupSelect, countrySelect, eventTagSelect;
    // Store all groups data for dynamic filtering
    let allGroupsData = [];
    const FILTER_STORAGE_KEY = 'editorFilters';

    function loadStoredFilters() {
        try {
            const raw = localStorage.getItem(FILTER_STORAGE_KEY);
            return raw ? JSON.parse(raw) : {};
        } catch (e) {
            return {};
        }
    }

    function saveStoredFilters(data) {
        localStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify(data));
    }

    function applyToggleState(button, state) {
        button.dataset.state = state;
        button.classList.remove('btn-outline-secondary', 'btn-success', 'btn-danger', 'state-include', 'state-exclude', 'state-off');
        if (state === 'include') {
            button.classList.add('btn-success');
            button.classList.add('state-include');
            button.title = 'Include only';
        } else if (state === 'exclude') {
            button.classList.add('btn-danger');
            button.classList.add('state-exclude');
            button.title = 'Exclude';
        } else {
            button.classList.add('btn-outline-secondary');
            button.classList.add('state-off');
            button.title = 'Off';
        }
    }

    function bindTriToggle(button) {
        button.addEventListener('mousemove', (e) => {
            const rect = button.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const ratio = x / rect.width;
            button.classList.remove('hover-left', 'hover-right');
            button.classList.remove('hover-left', 'hover-right', 'hover-middle', 'hover-override');
            if (ratio < 0.45) {
                button.classList.add('hover-left');
            } else if (ratio > 0.55) {
                button.classList.add('hover-right');
            } else {
                button.classList.add('hover-middle');
            }
            const current = button.dataset.state || 'off';
            if ((current === 'include' && ratio > 0.55) || (current === 'exclude' && ratio < 0.45)) {
                button.classList.add('hover-override');
            }
        });
        button.addEventListener('mouseleave', () => {
            button.classList.remove('hover-left', 'hover-right', 'hover-middle');
        });
        button.addEventListener('click', (e) => {
            e.preventDefault();
            const rect = button.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const ratio = x / rect.width;
            const current = button.dataset.state || 'off';
            let next = 'off';
            if (ratio < 0.45) {
                next = current === 'include' ? 'off' : 'include';
            } else if (ratio > 0.55) {
                next = current === 'exclude' ? 'off' : 'exclude';
            }
            applyToggleState(button, next);
            persistFilters();
            if (dataTable) dataTable.ajax.reload();
        });
    }

    // Initialize Tom Select dropdowns
    function initializeFilters() {
        // Re-init safe: destroy previous instances if this page init runs again
        [portalSelect, groupSelect, countrySelect, eventTagSelect].forEach(instance => {
            if (instance && typeof instance.destroy === 'function') {
                instance.destroy();
            }
        });
        portalSelect = null;
        groupSelect = null;
        countrySelect = null;
        eventTagSelect = null;

        ['#portalFilter', '#groupFilter', '#countryFilter', '#eventTagFilter'].forEach(selector => {
            const el = document.querySelector(selector);
            if (el && el.tomselect && typeof el.tomselect.destroy === 'function') {
                el.tomselect.destroy();
            }
        });

        // Portal filter with Tom Select
        portalSelect = new TomSelect('#portalFilter', {
            plugins: ['remove_button', 'clear_button'],
            placeholder: 'All Portals...',
            allowEmptyOption: true,
            closeAfterSelect: false,
            hidePlaceholder: true,
            maxOptions: null,
            onChange: function() {
                updateGroupFilter();
                persistFilters();
                if (dataTable) dataTable.ajax.reload();
            }
        });

        // Group filter with Tom Select (with optgroup support)
        groupSelect = new TomSelect('#groupFilter', {
            plugins: ['remove_button', 'clear_button'],
            placeholder: 'All Groups...',
            allowEmptyOption: true,
            closeAfterSelect: false,
            hidePlaceholder: true,
            optgroupField: 'portal',
            optgroupLabelField: 'portal',
            optgroupValueField: 'portal',
            lockOptgroupOrder: true,
            maxOptions: null,
            render: {
                optgroup_header: function(data, escape) {
                    return '<div class="optgroup-header">' + escape(data.portal) + '</div>';
                }
            },
            onChange: function() {
                persistFilters();
                if (dataTable) dataTable.ajax.reload();
            }
        });

        countrySelect = new TomSelect('#countryFilter', {
            plugins: ['remove_button', 'clear_button'],
            placeholder: 'All Countries...',
            allowEmptyOption: true,
            closeAfterSelect: false,
            hidePlaceholder: true,
            maxOptions: null,
            onChange: function() {
                persistFilters();
                if (dataTable) dataTable.ajax.reload();
            }
        });

        eventTagSelect = new TomSelect('#eventTagFilter', {
            plugins: ['remove_button', 'clear_button'],
            placeholder: 'All Event Tags...',
            allowEmptyOption: true,
            closeAfterSelect: false,
            hidePlaceholder: true,
            maxOptions: null,
            onChange: function() {
                persistFilters();
                if (dataTable) dataTable.ajax.reload();
            }
        });

        loadTagFilterValues();

        document.querySelectorAll('.tri-toggle').forEach(button => {
            bindTriToggle(button);
        });

        // Custom search input - connect to DataTable
        let searchTimeout;
        const searchInput = document.getElementById('searchFilter');
        searchInput.oninput = function(e) {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(function() {
                if (dataTable) dataTable.search(e.target.value).draw();
                persistFilters();
            }, 300);
        };

        // Populate the dropdowns
        populateFilters();
    }

    // Update group filter based on selected portals
    function updateGroupFilter() {
        const selectedPortals = portalSelect.getValue();

        // Clear current options
        groupSelect.clear();
        groupSelect.clearOptions();

        // Filter groups based on selected portals
        let filteredGroups = allGroupsData;
        if (selectedPortals && selectedPortals.length > 0) {
            filteredGroups = allGroupsData.filter(group => selectedPortals.includes(group.portal));
        }

        // Re-add filtered groups
        filteredGroups.forEach(group => {
            groupSelect.addOptionGroup(group.portal, { portal: group.portal });
            group.genres.forEach(genre => {
                groupSelect.addOption({ value: genre, text: genre, portal: group.portal });
            });
        });
    }

    // Populate filter dropdowns
    function populateFilters() {
        // Populate portals dropdown
        fetch('/editor/portals')
            .then(res => res.json())
            .then(data => {
                data.portals.forEach(portal => {
                    portalSelect.addOption({ value: portal, text: portal });
                });
                applyStoredSelects();
            })
            .catch(err => console.error('Error loading portals:', err));

        // Populate groups dropdown (grouped by portal)
        fetch('/editor/genres-grouped')
            .then(res => res.json())
            .then(data => {
                // Store all groups data for dynamic filtering
                allGroupsData = data.genres_by_portal;

                // Add optgroups for each portal
                data.genres_by_portal.forEach(group => {
                    groupSelect.addOptionGroup(group.portal, { portal: group.portal });
                    group.genres.forEach(genre => {
                        groupSelect.addOption({ value: genre, text: genre, portal: group.portal });
                    });
                });
                applyStoredSelects();
            })
            .catch(err => console.error('Error loading groups:', err));
    }

    function renderToggleButtons(containerId, values, prefix) {
        const container = document.getElementById(containerId);
        if (!container) return;
        container.innerHTML = '';
        values.forEach(value => {
            const key = `${prefix}:${value}`;
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'btn btn-outline-secondary btn-sm tri-toggle';
            button.dataset.key = key;
            button.dataset.state = 'off';
            button.textContent = value;
            bindTriToggle(button);
            container.appendChild(button);
        });
    }

    function loadTagFilterValues() {
        fetch('/api/editor/tag-values')
            .then(res => res.json())
            .then(data => {
                renderToggleButtons('resolutionButtons', data.resolutions || [], 'resolution');
                (data.countries || []).forEach(value => {
                    countrySelect.addOption({ value: value, text: value });
                });
                (data.event_tags || []).forEach(value => {
                    eventTagSelect.addOption({ value: value, text: value });
                });
                renderToggleButtons('miscButtons', data.misc_tags || [], 'misc');
                applyStoredSelects();
            })
            .catch(err => console.error('Error loading tag values:', err));
    }

    function applyStoredSelects() {
        const stored = loadStoredFilters();
        if (stored.portal && portalSelect) portalSelect.setValue(stored.portal, true);
        if (stored.group && groupSelect) groupSelect.setValue(stored.group, true);
        if (stored.country && countrySelect) countrySelect.setValue(stored.country, true);
        if (stored.eventTags && eventTagSelect) eventTagSelect.setValue(stored.eventTags, true);
        if (stored.toggles) {
            document.querySelectorAll('.tri-toggle').forEach(button => {
                const key = button.dataset.key;
                applyToggleState(button, stored.toggles[key] || 'off');
            });
        }
        if (stored.search !== undefined) {
            const input = document.getElementById('searchFilter');
            if (input) input.value = stored.search;
        }
        if (dataTable) {
            dataTable.ajax.reload();
        }
    }

    function persistFilters() {
        const existing = loadStoredFilters();
        const toggles = { ...(existing.toggles || {}) };
        document.querySelectorAll('.tri-toggle').forEach(button => {
            toggles[button.dataset.key] = button.dataset.state || 'off';
        });
        saveStoredFilters({
            portal: portalSelect ? portalSelect.getValue() : [],
            group: groupSelect ? groupSelect.getValue() : [],
            country: countrySelect ? countrySelect.getValue() : [],
            eventTags: eventTagSelect ? eventTagSelect.getValue() : [],
            search: document.getElementById('searchFilter')?.value || '',
            toggles: toggles,
        });
    }

    // Helper to get selected values as comma-separated string
    function getFilterValues(selectInstance) {
        const values = selectInstance.getValue();
        if (Array.isArray(values)) {
            return values.join(',');
        }
        return values || '';
    }

    function hashCode(value) {
        let hash = 0;
        for (let i = 0; i < value.length; i++) {
            hash = ((hash << 5) - hash) + value.charCodeAt(i);
            hash |= 0;
        }
        return Math.abs(hash);
    }

    function applyCountryColors() {
        const theme = document.documentElement.getAttribute('data-bs-theme') || 'dark';
        const lightness = theme === 'dark' ? 32 : 72;
        const saturation = 45;
        const textColor = theme === 'dark' ? '#f8f9fa' : '#1f2328';
        document.querySelectorAll('.name-country').forEach(el => {
            const code = (el.dataset.country || el.textContent || '').trim();
            if (!code) return;
            const hue = hashCode(code) % 360;
            el.style.backgroundColor = `hsl(${hue}, ${saturation}%, ${lightness}%)`;
            el.style.color = textColor;
        });
    }

    function applyGroupSwitchStates() {
        document.querySelectorAll('input.form-check-input[data-group-items]').forEach(sw => {
            const mixed = sw.getAttribute('data-mixed') === '1' || sw.classList.contains('is-mixed');
            sw.indeterminate = mixed;
            sw.classList.toggle('is-mixed', mixed);
        });
    }

    $(document).ready(function () {
        // Prevent "Cannot reinitialise DataTable" when this page init runs multiple times
        if ($.fn.DataTable.isDataTable('#table')) {
            const existingTable = $('#table').DataTable();
            existingTable.off('draw.dt');
            existingTable.destroy();
        }
        $('#table tbody').off('click', 'button.row-toggle');

        // Initialize Tom Select filter dropdowns
        initializeFilters();

        dataTable = $('#table').DataTable({
            dom: "<'row m-1 align-items-center'<'col-auto'B><'col-auto ms-auto'l><'col-auto ms-2'p>>" +
                "<'row'<'col-12'tr>>" +
                "<'row mb-1 mb-lg-0'<'col-auto text-light'i><'col-auto ms-auto'p>>",
            serverSide: true,
            processing: true,
            order: [[0, 'desc'], [3, 'asc']],
            pageLength: 250,
            lengthMenu: [[25, 50, 100, 250, 500, 1000], [25, 50, 100, 250, 500, 1000]],
            columnDefs: [
                { targets: [0, 1], width: "0%" },
                { targets: 0, className: "align-middle", orderable: true, searchable: false, orderDataType: 'dom-checkbox' },
                { targets: 1, className: "align-middle", orderable: false, searchable: false },
                { targets: 2, className: "align-middle", width: "56px" },
                { targets: 3, className: "align-middle", type: 'string', width: "45%" },
                { targets: 4, className: "align-middle", type: 'string' },
                { targets: 5, className: "align-middle", width: "140px" },
                { targets: 6, className: "align-middle text-end", orderable: false, searchable: false, width: "40px" }
            ],
            language: {
                search: "",
                searchPlaceholder: 'Filter',
                lengthMenu: "_MENU_",
                processing: "Loading channels..."
            },
            buttons: {
                buttons: [
                    {
                        text: '<i class="fas fa-save"></i> Save',
                        titleAttr: 'Save',
                        className: "btn btn-success",
                        action: function () {
                            save();
                        }
                    },
                    {
                        text: '<i class="fas fa-sync"></i> Refresh Channels',
                        titleAttr: 'Refresh channel list from portal',
                        className: "btn btn-primary",
                        action: function () {
                            refreshChannels();
                        }
                    },
                    {
                        text: '<i class="fas fa-undo"></i> Reset',
                        titleAttr: 'Reset',
                        className: "btn btn-danger",
                        action: async function () {
                            const confirmed = await showConfirmDialog({
                                title: 'Confirm Reset',
                                message: 'This will clear all edits! Are you sure?',
                                type: 'danger',
                                okText: 'Reset'
                            });
                            if (confirmed) {
                                document.getElementById('reset').submit();
                            }
                        }
                    },
                ],
            },
            ajax: {
                "url": editorDataUrl,
                "dataType": "json",
                "contentType": "application/json",
                "data": function(d) {
                    // Add custom filter parameters (comma-separated for multi-select)
                    d.portal = getFilterValues(portalSelect);
                    d.group = getFilterValues(groupSelect);
                    d.country = getFilterValues(countrySelect);
                    d.event_tags = getFilterValues(eventTagSelect);
                    const toggles = {};
                    const resolutionInclude = [];
                    const resolutionExclude = [];
                    const miscInclude = [];
                    const miscExclude = [];
                    let hevcState = 'off';
                    document.querySelectorAll('.tri-toggle').forEach(button => {
                        const key = button.dataset.key;
                        const state = button.dataset.state || 'off';
                        toggles[key] = state;
                        if (key === 'hevc') {
                            hevcState = state;
                        } else if (key && key.startsWith('resolution:')) {
                            const value = key.split(':')[1];
                            if (state === 'include') resolutionInclude.push(value);
                            if (state === 'exclude') resolutionExclude.push(value);
                        } else if (key && key.startsWith('misc:')) {
                            const value = key.split(':')[1];
                            if (state === 'include') miscInclude.push(value);
                            if (state === 'exclude') miscExclude.push(value);
                        }
                    });
                    d.raw = toggles.raw === 'off' ? '' : toggles.raw;
                    d.event = toggles.event === 'off' ? '' : toggles.event;
                    d.header = toggles.header === 'off' ? '' : toggles.header;
                    d.match = toggles.match === 'off' ? '' : toggles.match;
                    d.epg = toggles.epg === 'off' ? '' : toggles.epg;
                    d.codec = hevcState === 'off' ? '' : hevcState;
                    d.resolution_include = resolutionInclude.join(',');
                    d.resolution_exclude = resolutionExclude.join(',');
                    d.misc_include = miscInclude.join(',');
                    d.misc_exclude = miscExclude.join(',');
                }
            },
            columns: [
                {
                    data: "enabled",
                    render: function (data, type, row, meta) {
                        let r = '<div class="editor-toggle-wrap">\
                                <button type="button" class="btn btn-link row-toggle" title="Details">\
                                    <i class="fas fa-chevron-right expand-icon"></i>\
                                </button>';
                        if (row.isGroup) {
                            const payload = JSON.stringify(
                                (row.groupItems || []).map(item => ({ portal: item.portal, channelId: item.channelId }))
                            ).replace(/'/g, "\\'").replace(/"/g, '&quot;');
                            const groupItems = row.groupItems || [];
                            const enabledCount = groupItems.filter(item => !!item.enabled).length;
                            const allEnabled = groupItems.length > 0 && enabledCount === groupItems.length;
                            const mixedEnabled = groupItems.length > 0 && enabledCount > 0 && enabledCount < groupItems.length;
                            const switchClass = mixedEnabled ? 'checkbox form-check-input is-mixed' : 'checkbox form-check-input';
                            r += '<div class="form-check form-switch editor-switch">\
                                <input \
                                type="checkbox" \
                                class="' + switchClass + '" \
                                onchange="applyGroupEnabled(this)" \
                                data-group-items="' + payload + '"\
                                ' + (allEnabled ? ' checked' : '') + '\
                                ' + (mixedEnabled ? ' data-mixed=\"1\"' : '') + '\
                                >\
                                </div>';
                        } else {
                            r += '<div class="form-check form-switch editor-switch">\
                                <input \
                                type="checkbox" \
                                class="checkbox form-check-input" \
                                onchange="editEnabled(this)" \
                                data-portal="' + row.portal + '" \
                                data-channelId="' + row.channelId + '"';
                            if (data == true) {
                                r = r + ' checked';
                            }
                            r = r + '></div>';
                        }
                        r += '</div>';
                        return r
                    }
                },
                {
                    data: "link",
                    render: function (data, type, row, meta) {
                        if (row.isGroup) {
                            return '';
                        }
                        return '<button \
                            class="btn btn-success btn-block editor-play-btn" \
                            title="Play" \
                            data-bs-toggle="modal" \
                            data-bs-target="#videoModal" \
                            onclick="selectChannel(this)" \
                            data-channelName="' + row.channelName + '" \
                            data-customChannelName="' + row.customChannelName + '" \
                            data-autoChannelName="' + row.autoChannelName + '" \
                            data-link="' + row.link + '" \
                            data-portal="' + row.portal + '" \
                            data-channelId="' + row.channelId + '" \
                            data-videoCodec="' + (row.videoCodec || '') + '">\
                            <i class="fas fa-play"></i>\
                        </button>'
                    }
                },
                {
                    data: "channelNumber",
                    render: function (data, type, row, meta) {
                        if (row.isGroup) {
                            return '';
                        }
                        return '<input \
                                type="text" \
                                class="form-control table-input" \
                                onchange="editCustomNumber(this)" \
                                data-portal="' + row.portal + '" \
                                data-channelId="' + row.channelId + '" \
                                placeholder="' + row.channelNumber + '" \
                                title="' + row.channelNumber + '" \
                                value="' + row.customChannelNumber + '">'
                    },
                },
                {
                    data: "channelName",
                    render: function (data, type, row, meta) {
                        var displayName = row.effectiveDisplayName || row.autoChannelName || row.channelName;
                        var tags = '';
                        var country = row.country || '--';
                        if (row.isGroup && row.groupCount && row.groupCount > 1) {
                            tags += '<span class="name-tag name-tag-dup">' + row.groupCount + 'x</span>';
                        } else if (row.duplicateCount && row.duplicateCount > 1) {
                            tags += '<span class="name-tag name-tag-dup">' + row.duplicateCount + 'x</span>';
                        }
                        if (row.miscTags) {
                            row.miscTags.split(',').forEach(tag => {
                                const trimmed = tag.trim();
                                if (!trimmed) return;
                                tags += '<span class="name-tag name-tag-misc">' + trimmed + '</span>';
                            });
                        }
                        // Right-to-left order (right edge -> left): EPG, Header, Codec, Quality, Raw.
                        const hasMatch = row.isGroup
                            ? (row.groupItems || []).some(i => !!(i.matchedName || i.matchedStationId || i.matchedCallSign))
                            : !!(row.matchedName || row.matchedStationId || row.matchedCallSign);
                        if (row.isRaw) {
                            tags += '<span class="name-tag name-tag-raw">RAW</span>';
                        }
                        if (row.resolution) {
                            tags += '<span class="name-tag name-tag-quality name-tag-quality-' + row.resolution.toLowerCase() + '">' + row.resolution + '</span>';
                        } else {
                            tags += '<span class="name-tag name-tag-quality name-tag-empty">QUAL</span>';
                        }
                        const hasHevc = row.videoCodec === "HEVC";
                        tags += '<span class="name-tag name-tag-codec ' + (hasHevc ? 'name-tag-ok' : 'name-tag-empty') + '">HEVC</span>';
                        tags += '<span class="name-tag name-tag-event-flag ' + (row.isEvent ? 'name-tag-ok' : 'name-tag-empty') + '">EVENT</span>';
                        tags += '<span class="name-tag name-tag-match ' + (hasMatch ? 'name-tag-ok' : 'name-tag-empty') + '">MATCH</span>';
                        tags += '<span class="name-tag name-tag-header-flag ' + (row.isHeader ? 'name-tag-ok' : 'name-tag-empty') + '">HEADER</span>';
                        const hasCustomEpg = row.isGroup
                            ? (row.groupItems || []).some(i => !!(i.customEpgId && String(i.customEpgId).trim()))
                            : !!(row.customEpgId && String(row.customEpgId).trim());
                        let epgClass = 'name-tag-bad';
                        if (row.hasEpg) {
                            epgClass = hasCustomEpg ? 'name-tag-ok' : 'name-tag-epg-portal';
                        }
                        tags += '<span class="name-tag name-tag-epg ' + epgClass + '">EPG</span>';
                        if (row.isGroup) {
                            return '<div class="name-field">' +
                                   '<div class="name-country" title="Country" data-country="' + country + '">' + country + '</div>' +
                                   '<div class="name-static" title="' + displayName + '">' + displayName + '</div>' +
                                   '<div class="name-tags">' + tags + '</div>' +
                                   '</div>';
                        }
                        return '<div class="name-field">' +
                               '<div class="name-country" title="Country" data-country="' + country + '">' + country + '</div>' +
                               '<input \
                                type="text" \
                                class="form-control table-input name-input" \
                                onchange="editCustomName(this)" \
                                data-portal="' + row.portal + '" \
                                data-channelId="' + row.channelId + '" \
                                placeholder="' + displayName + '" \
                                title="' + displayName + '" \
                                value="' + row.customChannelName + '">' +
                               '<div class="name-tags">' + tags + '</div>' +
                               '</div>';
                    },
                },
                {
                    data: "genre",
                    render: function (data, type, row, meta) {
                        if (row.isGroup) {
                            const items = row.groupItems || [];
                            const payload = JSON.stringify(
                                items.map(item => ({ portal: item.portal, channelId: item.channelId }))
                            ).replace(/'/g, "\\'").replace(/"/g, '&quot;');
                            const values = items.map(item => (item.customGenre || '').trim());
                            const unique = [...new Set(values)];
                            const sharedCustomGroup = unique.length === 1 ? unique[0] : '';
                            return '<input \
                                type="text" \
                                class="form-control table-input" \
                                onchange="applyGroupCustomGroup(this)" \
                                data-group-items="' + payload + '" \
                                placeholder="' + (row.groupGenre || '-') + '" \
                                title="' + (row.groupGenre || '-') + '" \
                                value="' + sharedCustomGroup + '">';
                        }
                        return '<input \
                                type="text" \
                                class="form-control table-input" \
                                onchange="editCustomGroup(this)" \
                                data-portal="' + row.portal + '" \
                                data-channelId="' + row.channelId + '" \
                                placeholder="' + row.genre + '" \
                                title="' + row.genre + '" \
                                value="' + row.customGenre + '">'
                    },
                },
                {
                    data: "portalName",
                    render: function (data, type, row, meta) {
                        if (row.isGroup) {
                            const count = row.groupCount || 0;
                            return '<span class="portal-pill portal-pill-count">' + count + ' Portale</span>';
                        }
                        // Store the full row data in a data attribute for the modal
                        const rowDataJson = JSON.stringify(row).replace(/'/g, "\\'").replace(/"/g, '&quot;');
                        return '<span class="portal-pill" role="button" ' +
                               'onclick="showChannelInfo(this)" ' +
                               'data-row="' + rowDataJson + '">' +
                               (data || 'Unknown') + '</span>';
                    }
                },
                {
                    data: "channelId",
                    render: function (data, type, row, meta) {
                        if (row.isGroup) {
                            return '';
                        }
                        const rowDataJson = JSON.stringify(row).replace(/'/g, "\\'").replace(/"/g, '&quot;');
                        return `
                            <div class="dropdown editor-row-menu">
                                <button class="btn btn-sm btn-outline-secondary editor-row-menu-btn" type="button" data-bs-toggle="dropdown" aria-expanded="false" title="Actions">
                                    <i class="fas fa-ellipsis-v"></i>
                                </button>
                                <ul class="dropdown-menu dropdown-menu-end">
                                    <li>
                                        <button class="dropdown-item" type="button" onclick="openManualMatch(this)" data-row="${rowDataJson}">
                                            <i class="fas fa-magic me-2"></i> Manual match
                                        </button>
                                    </li>
                                    <li>
                                        <button class="dropdown-item text-danger" type="button" onclick="resetManualMatch(this)" data-row="${rowDataJson}">
                                            <i class="fas fa-eraser me-2"></i> Reset match
                                        </button>
                                    </li>
                                    <li><hr class="dropdown-divider"></li>
                                    <li>
                                        <button class="dropdown-item text-danger" type="button" onclick="deleteChannelFromEditor(this)" data-row="${rowDataJson}">
                                            <i class="fas fa-trash me-2"></i> Delete channel
                                        </button>
                                    </li>
                                </ul>
                            </div>
                        `;
                    }
                },
            ],
        });

        dataTable.on('draw.dt', function() {
            applyCountryColors();
            applyGroupSwitchStates();
        });

        $('#table tbody').on('click', 'button.row-toggle', function (e) {
            e.preventDefault();
            const tr = $(this).closest('tr');
            const row = dataTable.row(tr);
            const rowData = row && typeof row.data === 'function' ? row.data() : null;
            if (!rowData) {
                return;
            }
            if (row.child.isShown()) {
                row.child.hide();
                tr.removeClass('shown');
                $(this).find('i').css('transform', 'rotate(0deg)');
            } else {
                row.child(renderChannelDetails(rowData)).show();
                tr.addClass('shown');
                $(this).find('i').css('transform', 'rotate(90deg)');
                updateEpgSourceHintForRow(row);
            }
        });

        const stored = loadStoredFilters();
        if (stored.search) {
            dataTable.search(stored.search).draw();
        }
        applyStoredSelects();
        applyCountryColors();
        applyGroupSwitchStates();
    });

    function renderChannelDetails(row) {
            if (!row) {
                return '<div class="channel-subline"><div class="subline-item">Keine Details vorhanden.</div></div>';
            }
            if (row.isGroup) {
                return renderGroupDetails(row);
            }
            return renderChannelDetailsContent(row);
    }

    function renderGroupDetails(row) {
            const items = row.groupItems || [];
            if (!items.length) {
                return '<div class="channel-subline"><div class="subline-item">Keine Portale gefunden.</div></div>';
            }
            const representative = items.find(item => item && (item.logo || item.matchedLogo || item.matchedName)) || items[0];
            const sharedCustomEpg = (() => {
                const values = items.map(item => (item.customEpgId || '').trim());
                const unique = [...new Set(values)];
                return unique.length === 1 ? unique[0] : '';
            })();
            const groupItemsPayload = JSON.stringify(
                items.map(item => ({ portal: item.portal, channelId: item.channelId }))
            ).replace(/'/g, "\\'").replace(/"/g, '&quot;');
            return `
            <div class="group-details">
                <div class="group-portal-block group-portal-block--details">
                    <div class="group-details-list">
                        <div class="group-details-row">
                            ${renderChannelDetailsContent({
                                ...representative,
                                customEpgId: sharedCustomEpg,
                                portal: representative.portal,
                                channelId: representative.channelId
                            }).replace('onchange="editCustomEpgId(this)"', `data-group-items="${groupItemsPayload}" onchange="applyGroupEpgId(this)"`)}
                        </div>
                    </div>
                </div>
                ${items.map(item => `
                    <div class="group-portal-block">
                        <div class="group-portal-row">
                            <div class="group-cell group-cell-toggle">
                                <div class="form-check form-switch editor-switch">
                                    <input type="checkbox" class="checkbox form-check-input"
                                           onchange="editEnabled(this)"
                                           data-portal="${item.portal}"
                                           data-channelId="${item.channelId}"
                                           ${item.enabled ? 'checked' : ''}>
                                </div>
                            </div>
                            <div class="group-cell group-cell-play">
                                <button class="btn btn-success btn-block editor-play-btn"
                                        title="Play"
                                        data-bs-toggle="modal"
                                        data-bs-target="#videoModal"
                                        onclick="selectChannel(this)"
                                        data-channelName="${item.channelName}"
                                        data-customChannelName="${item.customChannelName}"
                                        data-autoChannelName="${item.autoChannelName}"
                                        data-link="${item.link}"
                                        data-portal="${item.portal}"
                                        data-channelId="${item.channelId}"
                                        data-videoCodec="${(item.videoCodec || '')}">
                                    <i class="fas fa-play"></i>
                                </button>
                            </div>
                            <div class="group-cell group-cell-number">
                                <input type="text" class="form-control table-input"
                                       onchange="editCustomNumber(this)"
                                       data-portal="${item.portal}"
                                       data-channelId="${item.channelId}"
                                       placeholder="${item.channelNumber || ''}"
                                       title="${item.channelNumber || ''}"
                                       value="${item.customChannelNumber || ''}">
                            </div>
                            <div class="group-cell group-cell-name">
                                <input type="text" class="form-control table-input name-input"
                                       onchange="editCustomName(this)"
                                       data-portal="${item.portal}"
                                       data-channelId="${item.channelId}"
                                       placeholder="${item.effectiveDisplayName || item.autoChannelName || item.channelName || ''}"
                                       title="${item.effectiveDisplayName || item.autoChannelName || item.channelName || ''}"
                                       value="${item.customChannelName || ''}">
                            </div>
                            <div class="group-cell group-cell-group">
                                <input type="text" class="form-control table-input"
                                       onchange="editCustomGroup(this)"
                                       data-portal="${item.portal}"
                                       data-channelId="${item.channelId}"
                                       placeholder="${item.genre || ''}"
                                       title="${item.genre || ''}"
                                       value="${item.customGenre || ''}">
                            </div>
                            <div class="group-cell group-cell-epg">
                                <input type="text"
                                       class="form-control table-input epg-suggest-input"
                                       onchange="editCustomEpgId(this)"
                                       data-portal="${item.portal}"
                                       data-channelId="${item.channelId}"
                                       data-stationId="${(item.matchedStationId || '').replace(/"/g, '&quot;')}"
                                       data-callSign="${(item.matchedCallSign || '').replace(/"/g, '&quot;')}"
                                       data-channelName="${(item.effectiveDisplayName || item.channelName || item.name || '').replace(/"/g, '&quot;')}"
                                       list="epg-suggestions"
                                       placeholder="${item.portal}${item.channelId}"
                                       title="${item.portal}${item.channelId}"
                                       value="${item.customEpgId || ''}">
                            </div>
                            <div class="group-cell group-cell-portal">
                                <span class="portal-pill" role="button"
                                      onclick="showChannelInfo(this)"
                                      data-row="${JSON.stringify(item).replace(/'/g, "\\'").replace(/"/g, '&quot;')}">
                                      ${item.portalName || 'Unknown'}
                                </span>
                            </div>
                        </div>
                    </div>
                `).join('')}
            </div>
            `;
    }

    function applyGroupCustomGroup(input) {
        const items = parseGroupItems(input);
        if (!items.length) return;
        const value = input.value;
        items.forEach(item => {
            groupEdits.push({ "portal": item.portal, "channel id": item.channelId, "custom genre": value });
        });
        const container = input.closest('.group-details');
        const root = container || document;
        items.forEach(item => {
            const selector = `input[data-portal="${item.portal}"][data-channelId="${item.channelId}"]`;
            root.querySelectorAll(selector).forEach(field => {
                if (field.onchange === editCustomGroup) {
                    field.value = value;
                }
            });
        });
    }

    function applyGroupEpgId(input) {
        const items = parseGroupItems(input);
        if (!items.length) return;
        const value = input.value;
        items.forEach(item => {
            epgEdits.push({ "portal": item.portal, "channel id": item.channelId, "custom epg id": value });
        });
        const container = input.closest('.group-details');
        if (container) {
            items.forEach(item => {
                const selector = `.epg-suggest-input[data-portal="${item.portal}"][data-channelId="${item.channelId}"]`;
                container.querySelectorAll(selector).forEach(field => {
                    field.value = value;
                    updateEpgSourceHintForInput(field);
                });
            });
        }
    }

    function applyGroupEnabled(input) {
        const enabled = !!input.checked;
        input.indeterminate = false;
        input.classList.remove('is-mixed');
        input.removeAttribute('data-mixed');

        const childTr = getGroupChildRowFromElement(input);
        if (childTr) {
            const memberSwitches = getMemberSwitchesInChildRow(childTr);
            if (memberSwitches.length) {
                memberSwitches.forEach(field => {
                    const portal = getPortalAttr(field);
                    const channelId = getChannelIdAttr(field);
                    if (!portal || !channelId) return;
                    field.checked = enabled;
                    upsertEnabledEdit(portal, channelId, enabled);
                });
                syncGroupSwitchVisualStateFromChildRow(childTr);
                return;
            }
        }

        // Fallback: use data-group-items mapping when child row switches are not present.
        const items = parseGroupItems(input);
        if (!items.length) return;
        items.forEach(item => {
            upsertEnabledEdit(item.portal, item.channelId, enabled);
        });

        const groupKeySet = new Set(items.map(item => `${item.portal}::${item.channelId}`));
        document.querySelectorAll('input.form-check-input[data-portal]').forEach(field => {
            const portal = getPortalAttr(field);
            const channelId = getChannelIdAttr(field);
            if (groupKeySet.has(`${portal}::${channelId}`)) {
                field.checked = enabled;
            }
        });
    }

    function renderChannelDetailsContent(row) {
            const matchedScore = row.matchedScore ? Number(row.matchedScore).toFixed(2) : '-';
            const matchedName = row.matchedName || '-';
            const matchedStationId = row.matchedStationId || '-';
            const matchedCallSign = row.matchedCallSign || '-';
            const logo = row.logo || (row.matchedName ? (row.matchedLogo || '') : '');
            const epgValue = row.customEpgId || '';
            const epgPlaceholder = row.portal + row.channelId;
            const epgEffective = row.customEpgId || row.effectiveEpgId || '';
            return `
            <div class="channel-subline channel-subline--compact">
                <div class="subline-item subline-logo">
                    <span class="subline-label">Logo</span>
                    ${logo ? `<img class="channel-logo" loading="lazy" src="${logo}" alt="logo" onerror="handleLogoLoadError(this)">` : '<span class="subline-empty">—</span>'}
                </div>
                <div class="subline-item"><span class="subline-label">Station ID</span><span class="subline-value">${matchedStationId}</span></div>
                <div class="subline-item"><span class="subline-label">Matched Name</span><span class="subline-value">${matchedName}</span></div>
                <div class="subline-item"><span class="subline-label">Call Sign</span><span class="subline-value">${matchedCallSign}</span></div>
                <div class="subline-item"><span class="subline-label">Score</span><span class="subline-value">${matchedScore}</span></div>
                <div class="subline-item subline-epg">
                    <span class="subline-label">EPG ID</span>
                    <div class="epg-input-row">
                        <input
                            type="text"
                            class="form-control table-input subline-input epg-suggest-input"
                            onchange="editCustomEpgId(this)"
                            data-portal="${row.portal}"
                            data-channelId="${row.channelId}"
                            data-stationId="${(row.matchedStationId || '').replace(/"/g, '&quot;')}"
                            data-callSign="${(row.matchedCallSign || '').replace(/"/g, '&quot;')}"
                            data-channelName="${(row.effectiveDisplayName || row.channelName || row.name || '').replace(/"/g, '&quot;')}"
                            list="epg-suggestions"
                            placeholder="${epgPlaceholder}"
                            title="${epgPlaceholder}"
                            value="${epgValue}">
                        <button type="button" class="btn btn-outline-secondary btn-sm epg-refresh-btn" onclick="refreshEpgForChannel(this)" title="EPG aktualisieren">
                            <i class="fas fa-sync"></i>
                        </button>
                    </div>
                    <div class="epg-source-hint" data-epg-id="${epgEffective}">Quelle: -</div>
                </div>
            </div>
            `;
    }
    
    // Function to refresh channels from portal
    async function refreshChannels() {
        const confirmed = await showConfirmDialog({
            title: 'Refresh Channels',
            message: 'This will fetch the latest channel list from your portals. This may take a few minutes. Continue?',
            type: 'info',
            okText: 'Refresh'
        });
        if (!confirmed) return;

        // Show loading indicator
        var btn = $('.btn-primary').first();
        var originalText = btn.html();
        btn.html('<i class="fas fa-spinner fa-spin"></i> Refreshing...').prop('disabled', true);

        fetch(editorRefreshUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                if (typeof showToast === 'function') {
                    showToast('Successfully refreshed ' + data.total + ' channels!', 'success');
                } else {
                    showNotification('Successfully refreshed ' + data.total + ' channels!', 'success');
                }
                dataTable.ajax.reload();
            } else {
                if (typeof showToast === 'function') {
                    showToast('Error refreshing channels: ' + (data.message || 'Unknown error'), 'error');
                } else {
                    showNotification('Error refreshing channels: ' + (data.message || 'Unknown error'), 'error', 5000);
                }
            }
        })
        .catch(error => {
            if (typeof showToast === 'function') {
                showToast('Error refreshing channels: ' + error, 'error');
            } else {
                showNotification('Error refreshing channels: ' + error, 'error', 5000);
            }
        })
        .finally(() => {
            btn.html(originalText).prop('disabled', false);
        });
    }

    // Show Channel Info Modal
    function showChannelInfo(element) {
        try {
            const rowDataStr = element.getAttribute('data-row').replace(/&quot;/g, '"');
            const row = JSON.parse(rowDataStr);

            // Set title
            document.getElementById('channelInfoTitle').textContent = row.customChannelName || row.effectiveDisplayName || row.autoChannelName || row.channelName || 'Channel Details';

            // Logo
            const logoContainer = document.getElementById('channelInfoLogoContainer');
            const logoImg = document.getElementById('channelInfoLogo');
            if (row.logo) {
                logoImg.dataset.logoFallbackTried = '0';
                logoImg.onerror = function() { handleLogoLoadError(this); };
                logoImg.src = row.logo;
                logoContainer.style.display = 'block';
            } else {
                logoContainer.style.display = 'none';
            }

            // Basic Info
            document.getElementById('infoChannelId').textContent = row.channelId || '-';
            document.getElementById('infoName').textContent = row.channelName || '-';
            document.getElementById('infoAutoName').textContent = row.autoChannelName || '-';
            document.getElementById('infoCustomName').textContent = row.customChannelName || '-';
            document.getElementById('infoNumber').textContent = row.channelNumber || '-';
            document.getElementById('infoCustomNumber').textContent = row.customChannelNumber || '-';
            document.getElementById('infoEnabled').innerHTML = row.enabled
                ? '<span class="badge bg-success">Yes</span>'
                : '<span class="badge bg-secondary">No</span>';

            // Group Info
            document.getElementById('infoGenre').textContent = row.genre || '-';
            document.getElementById('infoGenreId').textContent = row.genreId || '-';
            document.getElementById('infoCustomGenre').textContent = row.customGenre || '-';
            document.getElementById('infoPortalName').textContent = row.portalName || '-';
            document.getElementById('infoPortalId').textContent = row.portal || '-';

            // EPG & Fallback
            document.getElementById('infoHasEpg').innerHTML = row.hasEpg
                ? '<span class="badge bg-success">Yes</span>'
                : '<span class="badge bg-secondary">No</span>';
            document.getElementById('infoCustomEpgId').textContent = row.customEpgId
                ? row.customEpgId
                : (row.effectiveEpgId ? row.effectiveEpgId + ' (auto)' : '-');
            document.getElementById('infoDuplicates').innerHTML = row.duplicateCount > 1
                ? '<span class="badge bg-warning text-dark">' + row.duplicateCount + 'x enabled</span>'
                : '<span class="badge bg-secondary">None</span>';

            // Tags
            document.getElementById('infoResolution').textContent = row.resolution || '-';
            document.getElementById('infoVideoCodec').textContent = row.videoCodec || '-';
            document.getElementById('infoEventTags').textContent = row.eventTags || '-';
            document.getElementById('infoMiscTags').textContent = row.miscTags || '-';
            document.getElementById('infoCountry').textContent = row.country || '-';
            document.getElementById('infoMatchedName').textContent = row.matchedName || '-';
            document.getElementById('infoMatchedSource').textContent = row.matchedSource || '-';
            document.getElementById('infoIsRaw').innerHTML = row.isRaw
                ? '<span class="badge bg-warning text-dark">Yes</span>'
                : '<span class="badge bg-secondary">No</span>';
            document.getElementById('infoIsEvent').innerHTML = row.isEvent
                ? '<span class="badge bg-primary">Yes</span>'
                : '<span class="badge bg-secondary">No</span>';
            document.getElementById('infoIsHeader').innerHTML = row.isHeader
                ? '<span class="badge bg-light text-dark">Yes</span>'
                : '<span class="badge bg-secondary">No</span>';

            // Stream URL
            document.getElementById('infoLink').textContent = row.link || '-';

            // Available MACs - Parse and number them
            const macsContainer = document.getElementById('infoMacsContainer');
            if (row.availableMacs && row.availableMacs.trim()) {
                const macs = row.availableMacs.split(',').map(m => m.trim()).filter(m => m);
                if (macs.length > 0) {
                    let macsHtml = '<div class="table-responsive"><table class="table table-sm table-bordered">';
                    macsHtml += '<thead><tr><th style="width: 60px;">#</th><th>MAC Address</th></tr></thead><tbody>';
                    macs.forEach((mac, index) => {
                        macsHtml += '<tr><td><span class="badge bg-primary">' + (index + 1) + '</span></td>';
                        macsHtml += '<td><code>' + mac + '</code></td></tr>';
                    });
                    macsHtml += '</tbody></table></div>';
                    macsContainer.innerHTML = macsHtml;
                } else {
                    macsContainer.innerHTML = '<p class="text-muted mb-0">No MACs available</p>';
                }
            } else {
                macsContainer.innerHTML = '<p class="text-muted mb-0">No MACs available</p>';
            }

            // Alternate IDs
            const alternateIdsContainer = document.getElementById('infoAlternateIds');
            if (row.alternateIds && row.alternateIds.trim()) {
                const altIds = row.alternateIds.split(',').map(id => id.trim()).filter(id => id);
                if (altIds.length > 0) {
                    let altHtml = '<div class="d-flex flex-wrap gap-1">';
                    altIds.forEach(id => {
                        altHtml += '<code class="badge bg-info text-dark">' + id + '</code>';
                    });
                    altHtml += '</div>';
                    alternateIdsContainer.innerHTML = altHtml;
                } else {
                    alternateIdsContainer.innerHTML = '<p class="text-muted mb-0">No alternate IDs</p>';
                }
            } else {
                alternateIdsContainer.innerHTML = '<p class="text-muted mb-0">No alternate IDs</p>';
            }

            // Store current channel info for merge
            window.currentChannelInfo = {
                portal: row.portal,
                channelId: row.channelId,
                channelName: row.customChannelName || row.autoChannelName || row.channelName
            };

            // Reset merge search
            document.getElementById('mergeChannelSearch').value = '';
            document.getElementById('mergeTargetPortal').value = '';
            document.getElementById('mergeTargetChannelId').value = '';
            document.getElementById('mergeChannelBtn').disabled = true;
            document.getElementById('mergeSearchResults').style.display = 'none';

            // Show modal
            const modal = new bootstrap.Modal(document.getElementById('channelInfoModal'));
            modal.show();
        } catch (error) {
            console.error('Error showing channel info:', error);
            showNotification('Error loading channel details', 'error');
        }
    }

    // Merge channel search functionality
    let mergeSearchTimeout = null;
    document.getElementById('mergeChannelSearch').addEventListener('input', function(e) {
        const query = e.target.value.trim();
        const resultsContainer = document.getElementById('mergeSearchResults');

        if (query.length < 2) {
            resultsContainer.style.display = 'none';
            return;
        }

        clearTimeout(mergeSearchTimeout);
        mergeSearchTimeout = setTimeout(async () => {
            // Search via API (since DataTable uses server-side pagination)
            resultsContainer.innerHTML = '<div class="list-group-item text-muted"><i class="fas fa-spinner fa-spin"></i> Searching...</div>';
            resultsContainer.style.display = 'block';

            try {
                const response = await fetch('/api/editor/search-for-merge', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        portal: window.currentChannelInfo.portal,
                        excludeChannelId: window.currentChannelInfo.channelId,
                        query: query
                    })
                });

                const data = await response.json();

                if (data.success && data.channels.length > 0) {
                    let html = '';
                    data.channels.forEach(ch => {
                        const displayName = ch.customName || ch.name;
                        html += '<a href="#" class="list-group-item list-group-item-action merge-result-item" ' +
                                'data-portal="' + window.currentChannelInfo.portal + '" data-channel-id="' + ch.channelId + '">' +
                                '<small class="text-muted">ID: ' + ch.channelId + '</small> - ' + displayName +
                                (ch.genre ? ' <span class="badge bg-secondary">' + ch.genre + '</span>' : '') +
                                '</a>';
                    });
                    resultsContainer.innerHTML = html;
                } else {
                    resultsContainer.innerHTML = '<div class="list-group-item text-muted">No matching channels found</div>';
                }
            } catch (error) {
                console.error('Error searching channels:', error);
                resultsContainer.innerHTML = '<div class="list-group-item text-danger">Error searching channels</div>';
            }
        }, 300);
    });

    // Handle merge result selection
    document.getElementById('mergeSearchResults').addEventListener('click', function(e) {
        e.preventDefault();
        const item = e.target.closest('.merge-result-item');
        if (item) {
            const portal = item.dataset.portal;
            const channelId = item.dataset.channelId;
            const displayText = item.textContent;

            document.getElementById('mergeChannelSearch').value = displayText;
            document.getElementById('mergeTargetPortal').value = portal;
            document.getElementById('mergeTargetChannelId').value = channelId;
            document.getElementById('mergeChannelBtn').disabled = false;
            document.getElementById('mergeSearchResults').style.display = 'none';
        }
    });

    // Merge button click handler
    document.getElementById('mergeChannelBtn').addEventListener('click', async function() {
        const secondaryPortal = document.getElementById('mergeTargetPortal').value;
        const secondaryChannelId = document.getElementById('mergeTargetChannelId').value;

        if (!secondaryPortal || !secondaryChannelId) {
            showNotification('Please select a channel to merge', 'warning');
            return;
        }

        if (!window.currentChannelInfo) {
            showNotification('Error: Current channel info not found', 'error');
            return;
        }

        const primaryPortal = window.currentChannelInfo.portal;
        const primaryChannelId = window.currentChannelInfo.channelId;

        if (primaryChannelId === secondaryChannelId) {
            showNotification('Cannot merge a channel with itself', 'warning');
            return;
        }

        // Confirm merge
        const confirmed = await showConfirmDialog({
            title: 'Merge channel',
            message: 'Merge channel ID ' + secondaryChannelId + ' into ' + primaryChannelId + '?\n\n' +
                     'The secondary channel will be deleted and its ID will become an alternate for the primary channel.',
            type: 'warning',
            okText: 'Merge'
        });
        if (!confirmed) return;

        const btn = this;
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Merging...';

        fetch('/api/editor/merge', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                primaryPortal: primaryPortal,
                primaryChannelId: primaryChannelId,
                secondaryPortal: secondaryPortal,
                secondaryChannelId: secondaryChannelId
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showNotification(data.message, 'success', 5000);
                // Close modal and refresh table
                bootstrap.Modal.getInstance(document.getElementById('channelInfoModal')).hide();
                reloadEditorTable();
            } else {
                showNotification('Merge failed: ' + data.error, 'error', 5000);
            }
        })
        .catch(error => {
            showNotification('Error merging channels: ' + error, 'error', 5000);
        })
        .finally(() => {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-object-group"></i> Merge';
        });
    });

    // Manual match helpers
    let manualMatchContext = null;
    let manualMatchSearchTimeout = null;

    function parseRowDataFromElement(element) {
        const rowDataStr = element.getAttribute('data-row').replace(/&quot;/g, '"');
        return JSON.parse(rowDataStr);
    }

    function reloadEditorTable() {
        if (dataTable && dataTable.ajax) {
            dataTable.ajax.reload(null, false);
            return;
        }
        if ($.fn.DataTable.isDataTable('#table')) {
            $('#table').DataTable().ajax.reload(null, false);
        }
    }

    function openManualMatch(element) {
        const row = parseRowDataFromElement(element);
        manualMatchContext = {
            portal: row.portal,
            channelId: row.channelId,
            name: row.customChannelName || row.effectiveDisplayName || row.autoChannelName || row.channelName || ''
        };
        document.getElementById('manualMatchChannel').textContent = manualMatchContext.name || '-';
        const searchInput = document.getElementById('manualMatchSearch');
        searchInput.value = '';
        loadManualMatchSuggestions('');

        const modal = new bootstrap.Modal(document.getElementById('manualMatchModal'));
        modal.show();
    }

    function resetManualMatch(element) {
        const row = parseRowDataFromElement(element);
        showConfirmDialog({
            title: 'Reset match',
            message: 'Remove the current match for this channel?',
            type: 'warning',
            okText: 'Reset'
        }).then((confirmed) => {
            if (!confirmed) return;
            fetch('/api/editor/match/reset', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ portal: row.portal, channelId: row.channelId })
            })
            .then(response => response.json())
            .then(data => {
                if (data.ok) {
                    showNotification('Match reset.', 'success');
                    reloadEditorTable();
                } else {
                    showNotification(data.error || 'Reset failed', 'error');
                }
            })
            .catch(error => showNotification('Reset failed: ' + error, 'error'));
        });
    }

    function deleteChannelFromEditor(element) {
        const row = parseRowDataFromElement(element);
        const channelName = row.customChannelName || row.effectiveDisplayName || row.autoChannelName || row.channelName || row.channelId || 'this channel';
        showConfirmDialog({
            title: 'Delete channel',
            message: `Delete "${channelName}" (${row.channelId})? This will remove it from the portal cache.`,
            type: 'danger',
            okText: 'Delete'
        }).then((confirmed) => {
            if (!confirmed) return;
            fetch('/api/editor/delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ portal: row.portal, channelId: row.channelId })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showNotification('Channel deleted.', 'success');
                    const $tr = $(element).closest('tr');
                    if (dataTable && dataTable.row($tr).length) {
                        dataTable.row($tr).remove().draw(false);
                    }
                    reloadEditorTable();
                } else {
                    showNotification(data.error || 'Delete failed', 'error');
                }
            })
            .catch(error => showNotification('Delete failed: ' + error, 'error'));
        });
    }

    function loadManualMatchSuggestions(query) {
        if (!manualMatchContext) return;
        const results = document.getElementById('manualMatchResults');
        results.innerHTML = '<div class="text-muted"><i class="fas fa-spinner fa-spin me-2"></i>Loading matches...</div>';

        const params = new URLSearchParams({
            portal: manualMatchContext.portal,
            channelId: manualMatchContext.channelId,
            query: query || ''
        });

        fetch('/api/editor/match/suggestions?' + params.toString())
            .then(response => response.json())
            .then(data => {
                if (!data.ok) {
                    results.innerHTML = '<div class="text-danger">No suggestions available.</div>';
                    return;
                }
                const matches = data.results || [];
                if (matches.length === 0) {
                    results.innerHTML = '<div class="text-muted">No matches found.</div>';
                    return;
                }
                let html = '<div class="list-group">';
                matches.forEach(match => {
                    const score = match.score !== undefined ? Number(match.score).toFixed(2) : '-';
                    const callSign = match.call_sign ? `<span class="badge bg-secondary ms-2">${match.call_sign}</span>` : '';
                    const stationId = match.station_id ? `<small class="text-muted">ID ${match.station_id}</small>` : '';
                    const payload = JSON.stringify(match).replace(/'/g, "\\'").replace(/"/g, '&quot;');
                    html += `
                        <div class="list-group-item d-flex align-items-center justify-content-between gap-2">
                            <div>
                                <div class="fw-semibold">${escapeHtml(match.name || '')}${callSign}</div>
                                <div class="small text-muted">Score ${score} ${stationId}</div>
                            </div>
                            <button type="button" class="btn btn-sm btn-success" onclick="applyManualMatch(this)" data-match="${payload}">
                                Use
                            </button>
                        </div>
                    `;
                });
                html += '</div>';
                results.innerHTML = html;
            })
            .catch(error => {
                results.innerHTML = '<div class="text-danger">Error loading suggestions.</div>';
                console.error('Manual match error:', error);
            });
    }

    function applyManualMatch(element) {
        if (!manualMatchContext) return;
        const matchStr = element.getAttribute('data-match').replace(/&quot;/g, '"');
        const match = JSON.parse(matchStr);
        fetch('/api/editor/match/set', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                portal: manualMatchContext.portal,
                channelId: manualMatchContext.channelId,
                match: {
                    name: match.name || '',
                    station_id: match.station_id || '',
                    call_sign: match.call_sign || '',
                    logo_uri: match.logo_uri || '',
                    score: match.score || '',
                    source: 'channelsdvr'
                }
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.ok) {
                showNotification('Match updated.', 'success');
                bootstrap.Modal.getInstance(document.getElementById('manualMatchModal')).hide();
                reloadEditorTable();
            } else {
                showNotification(data.error || 'Match update failed', 'error');
            }
        })
        .catch(error => showNotification('Match update failed: ' + error, 'error'));
    }

    document.getElementById('manualMatchSearch').addEventListener('input', function(e) {
        const query = e.target.value.trim();
        clearTimeout(manualMatchSearchTimeout);
        manualMatchSearchTimeout = setTimeout(() => {
            loadManualMatchSuggestions(query);
        }, 300);
    });


        // expose functions used in inline handlers
        window.editAll = editAll;
        window.editEnabled = editEnabled;
        window.editCustomNumber = editCustomNumber;
        window.editCustomName = editCustomName;
        window.editCustomGroup = editCustomGroup;
        window.editCustomEpgId = editCustomEpgId;
        window.refreshEpgForChannel = refreshEpgForChannel;
        window.refreshChannels = refreshChannels;
        window.showChannelInfo = showChannelInfo;
        window.openManualMatch = openManualMatch;
        window.resetManualMatch = resetManualMatch;
        window.deleteChannelFromEditor = deleteChannelFromEditor;
        window.applyManualMatch = applyManualMatch;
        window.selectChannel = selectChannel;
        window.save = save;
        window.handleLogoLoadError = handleLogoLoadError;
        window.applyGroupCustomGroup = applyGroupCustomGroup;
        window.applyGroupEpgId = applyGroupEpgId;
        window.applyGroupEnabled = applyGroupEnabled;
    }
    window.App && window.App.register('editor', initEditorPage);
})();
