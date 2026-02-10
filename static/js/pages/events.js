(function() {
    function initEventsPage() {
        const listEl = document.getElementById('filtersList');
        const addBtn = document.getElementById('addFilterBtn');
        const saveBtn = document.getElementById('saveFilterBtn');
        const findEventsModalBtn = document.getElementById('findEventsModalBtn');
        const refreshSportsDbBtn = document.getElementById('refreshSportsDbBtn');
        const addGroupsBtn = document.getElementById('addGroupsBtn');
        const clearGroupsBtn = document.getElementById('clearGroupsBtn');
        const modalEl = document.getElementById('filterModal');
        const groupsModalEl = document.getElementById('groupsModal');
        const groupsTableBody = document.getElementById('groupsTableBody');
        const groupsSearch = document.getElementById('groupsSearch');
        const applyGroupsBtn = document.getElementById('applyGroupsBtn');
        const selectedGroupsSummary = document.getElementById('selectedGroupsSummary');
        if (!listEl || !addBtn || !saveBtn || !findEventsModalBtn || !modalEl || !refreshSportsDbBtn || !addGroupsBtn || !clearGroupsBtn || !groupsModalEl || !groupsTableBody || !groupsSearch || !applyGroupsBtn || !selectedGroupsSummary) return;

        const modalTitle = document.getElementById('filterModalTitle');
        const fields = {
            id: document.getElementById('filterId'),
            name: document.getElementById('filterName'),
            provider: document.getElementById('filterProvider'),
            sport: document.getElementById('filterSport'),
            leagues: document.getElementById('filterLeagues'),
            espnEvents: document.getElementById('filterEspnEvents'),
            espnWindow: document.getElementById('filterEspnWindow'),
            channelRegex: document.getElementById('filterChannelRegex'),
            epgPattern: document.getElementById('filterEpgPattern'),
            extractRegex: document.getElementById('filterExtractRegex'),
            outputTemplate: document.getElementById('filterOutputTemplate'),
            outputGroup: document.getElementById('filterOutputGroup'),
            channelNumberStart: document.getElementById('filterChannelNumberStart'),
            priority: document.getElementById('filterPriority'),
            enabled: document.getElementById('filterEnabled'),
        };

        let rules = [];
        let bsModal = null;
        let groupsModal = null;
        let matchedStreamsModal = null;
        let sportsCache = [];
        let leaguesCache = [];
        let availableGroups = [];
        let selectedGroupTokens = [];
        let espnMatchMap = {};
        let espnReplayMap = {};
        let espnEventMeta = {};
        let lastMatchedRows = [];
        let lastEventConfig = {};
        let previewSample = {
            home: 'Team A',
            away: 'Team B',
            home_abbr: 'TA',
            away_abbr: 'TB',
            sport: 'Sport',
            league: 'League',
            date: '2026-02-08',
            time: '15:30'
        };
        let previewTimer = null;
        let previewKey = '';
        const providerEndpoints = {
            espn: {
                refresh: '/api/events/espn/refresh',
                sports: '/api/events/espn/sports',
                leagues: '/api/events/espn/leagues',
                teams: '/api/events/espn/teams',
            }
        };

        function currentProvider() {
            return 'espn';
        }

        function splitCsv(value) {
            return String(value || '').split(',').map(function(v) { return v.trim(); }).filter(Boolean);
        }

        function selectedValues(selectEl) {
            return Array.from(selectEl.options || [])
                .filter(function(option) { return option.selected; })
                .map(function(option) { return option.value; });
        }

        function setSelectedValues(selectEl, values) {
            const set = new Set(values || []);
            Array.from(selectEl.options || []).forEach(function(option) {
                option.selected = set.has(option.value);
            });
        }

        function clearSelect(selectEl) {
            selectEl.innerHTML = '';
        }

        function populateSports(selectedSport) {
            clearSelect(fields.sport);
            const placeholder = document.createElement('option');
            placeholder.value = '';
            placeholder.textContent = 'Select sport...';
            fields.sport.appendChild(placeholder);
            sportsCache.forEach(function(sport) {
                const opt = document.createElement('option');
                const sportId = sport.id || sport.name;
                opt.value = sportId;
                opt.textContent = sport.name;
                if (selectedSport && selectedSport === sportId) {
                    opt.selected = true;
                }
                fields.sport.appendChild(opt);
            });
            updateOutputPreview();
        }

        function resolveSportValue(value) {
            const raw = String(value || '').trim();
            if (!raw) return '';
            const byId = sportsCache.find(function(s) { return String(s.id || s.name) === raw; });
            if (byId) return String(byId.id || byId.name);
            const byName = sportsCache.find(function(s) { return String(s.name || '').toLowerCase() === raw.toLowerCase(); });
            if (byName) return String(byName.id || byName.name);
            return raw;
        }

        function populateLeagues(selectedSport, selectedLeagueNames) {
            clearSelect(fields.leagues);
            leaguesCache
                .filter(function(league) {
                    return !selectedSport || league.sport === selectedSport;
                })
                .forEach(function(league) {
                    const opt = document.createElement('option');
                    opt.value = league.id || league.name;
                    opt.textContent = league.name;
                    opt.dataset.leagueId = league.id;
                    fields.leagues.appendChild(opt);
                });
            const selected = (selectedLeagueNames || []).map(function(value) {
                const raw = String(value || '').trim();
                if (!raw) return '';
                const byId = leaguesCache.find(function(l) { return String(l.id) === raw; });
                if (byId) return String(byId.id);
                const byName = leaguesCache.find(function(l) { return String(l.name || '').toLowerCase() === raw.toLowerCase(); });
                if (byName) return String(byName.id || byName.name);
                return raw;
            }).filter(Boolean);
            setSelectedValues(fields.leagues, selected);
            updateOutputPreview();
        }

        function updateOutputPreview() {
            const previewEl = document.getElementById('filterOutputPreview');
            if (!previewEl) return;
            const sportLabel = (fields.sport && fields.sport.options[fields.sport.selectedIndex])
                ? fields.sport.options[fields.sport.selectedIndex].textContent
                : '';
            const leagueLabel = Array.from(fields.leagues.options || [])
                .filter(function(option) { return option.selected; })
                .map(function(option) { return option.textContent; })[0] || '';
            previewSample.sport = sportLabel || previewSample.sport || 'Sport';
            previewSample.league = leagueLabel || previewSample.league || 'League';
            const template = (fields.outputTemplate.value || '').trim();
            const groupTemplate = (fields.outputGroup.value || '').trim();
            const namePreview = template
                .replaceAll('{home}', previewSample.home)
                .replaceAll('{away}', previewSample.away)
                .replaceAll('{home_abbr}', previewSample.home_abbr)
                .replaceAll('{away_abbr}', previewSample.away_abbr)
                .replaceAll('{home_short}', previewSample.home_abbr)
                .replaceAll('{away_short}', previewSample.away_abbr)
                .replaceAll('{sport}', previewSample.sport)
                .replaceAll('{league}', previewSample.league)
                .replaceAll('{date}', previewSample.date)
                .replaceAll('{time}', previewSample.time)
                .trim() || '-';
            const groupPreview = groupTemplate
                .replaceAll('{sport}', previewSample.sport)
                .replaceAll('{league}', previewSample.league)
                .trim() || '-';
            previewEl.textContent = `Preview: ${namePreview} | Group: ${groupPreview}`;
        }

        function scheduleEspnPreview() {
            if (!fields.espnEvents || !fields.espnEvents.checked) return;
            const sportValue = fields.sport.value.trim();
            const leagueValue = selectedValues(fields.leagues)[0] || '';
            if (!sportValue || !leagueValue) return;
            const key = `${sportValue}::${leagueValue}::${fields.espnWindow.value || 72}`;
            if (key === previewKey && previewSample._real) {
                updateOutputPreview();
                return;
            }
            previewKey = key;
            if (previewTimer) window.clearTimeout(previewTimer);
            previewTimer = window.setTimeout(function() {
                const previewEl = document.getElementById('filterOutputPreview');
                if (previewEl) previewEl.textContent = 'Preview: lade ESPN...';
                fetch('/api/events/preview/espn_event', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        sport: sportValue,
                        league_filters: [leagueValue],
                        espn_event_window_hours: Number(fields.espnWindow.value || 72)
                    })
                })
                    .then(function(resp) { return resp.json(); })
                    .then(function(data) {
                        if (data && data.ok && data.event) {
                            const start = data.event.start ? new Date(data.event.start) : null;
                            previewSample = {
                                home: data.event.home || 'Team A',
                                away: data.event.away || 'Team B',
                                home_abbr: data.event.home_abbr || data.event.home || 'TA',
                                away_abbr: data.event.away_abbr || data.event.away || 'TB',
                                sport: data.event.sport || previewSample.sport,
                                league: data.event.league || previewSample.league,
                                date: start ? start.toLocaleDateString('sv-SE') : previewSample.date,
                                time: start ? start.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' }) : previewSample.time,
                                _real: true
                            };
                        } else {
                            previewSample = {
                                home: previewSample.home || 'Team A',
                                away: previewSample.away || 'Team B',
                                home_abbr: previewSample.home_abbr || 'TA',
                                away_abbr: previewSample.away_abbr || 'TB',
                                sport: previewSample.sport || 'Sport',
                                league: previewSample.league || 'League',
                                date: previewSample.date || '2026-02-08',
                                time: previewSample.time || '15:30'
                            };
                        }
                        updateOutputPreview();
                    })
                    .catch(function() {
                        updateOutputPreview();
                    });
            }, 300);
        }

        function clearModal() {
            fields.id.value = '';
            fields.name.value = '';
            fields.provider.value = 'espn';
            fields.priority.value = '100';
            fields.espnEvents.checked = true;
            fields.espnWindow.value = '72';
            fields.channelRegex.value = '';
            fields.epgPattern.value = '';
            fields.extractRegex.value = '';
            fields.outputTemplate.value = '{home} vs {away} | {date} {time}';
            fields.outputGroup.value = 'EVENTS';
            fields.channelNumberStart.value = '10000';
            fields.enabled.checked = true;
            selectedGroupTokens = [];
            renderSelectedGroupsSummary();
            populateSports('');
            populateLeagues('', []);
            updateOutputPreview();
            modalTitle.textContent = 'Add Filter';
        }

        function payloadFromModal() {
            return {
                rule: {
                    name: fields.name.value.trim(),
                    enabled: fields.enabled.checked,
                    provider: currentProvider(),
                    use_espn_events: !!fields.espnEvents.checked,
                    espn_event_window_hours: Number(fields.espnWindow.value || 72),
                    sport: fields.sport.value.trim(),
                    league_filters: selectedValues(fields.leagues),
                    team_filters: [],
                    channel_groups: selectedGroupTokens.slice(),
                    channel_regex: fields.channelRegex.value.trim(),
                    epg_pattern: fields.epgPattern.value.trim(),
                    extract_regex: fields.extractRegex.value.trim(),
                    output_template: fields.outputTemplate.value.trim(),
                    output_group_name: fields.outputGroup.value.trim(),
                    channel_number_start: Number(fields.channelNumberStart.value || 10000),
                    priority: Number(fields.priority.value || 100)
                }
            };
        }

        function ruleToPreviewPayload(rule) {
            return {
                rule_id: rule.id || null,
                provider: 'espn',
                use_espn_events: !!rule.use_espn_events,
                espn_event_window_hours: Number(rule.espn_event_window_hours || 72),
                sport: rule.sport || '',
                groups: (rule.channel_groups || []).slice(),
                channel_regex: rule.channel_regex || '',
                epg_pattern: rule.epg_pattern || '',
                extract_regex: rule.extract_regex || '',
                league_filters: (rule.league_filters || []).slice(),
                team_filters: [],
                output_group_name: rule.output_group_name || 'EVENTS',
                channel_number_start: Number(rule.channel_number_start || 10000),
                output_template: rule.output_template || '{home} vs {away} | {date} {time}'
            };
        }

        function tokenToLabel(token) {
            const raw = String(token || '');
            if (!raw) return '';
            if (!raw.includes('::')) return raw;
            const parts = raw.split('::');
            const portalId = parts.shift();
            const groupName = parts.join('::');
            const group = availableGroups.find(function(item) { return item.token === raw; });
            const portal = group ? (group.portal_name || portalId) : portalId;
            return `${groupName} (${portal})`;
        }

        function renderSelectedGroupsSummary() {
            if (!selectedGroupTokens.length) {
                selectedGroupsSummary.innerHTML = '<span class="text-muted">No groups selected (all groups).</span>';
                return;
            }
            selectedGroupsSummary.innerHTML = selectedGroupTokens.map(function(token) {
                return `<span class="badge bg-dark me-1 mb-1">${tokenToLabel(token)}</span>`;
            }).join('');
        }

        function loadAvailableGroups() {
            return fetch('/api/events/groups/detailed')
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        availableGroups = [];
                        return;
                    }
                    availableGroups = data.groups || [];
                })
                .catch(function() {
                    availableGroups = [];
                });
        }

        function renderGroupsTable(filterText) {
            const term = String(filterText || '').trim().toLowerCase();
            const selectedSet = new Set(selectedGroupTokens);
            const rows = availableGroups.filter(function(group) {
                if (!term) return true;
                return (
                    String(group.group_name || '').toLowerCase().includes(term) ||
                    String(group.portal_name || '').toLowerCase().includes(term) ||
                    String(group.portal_id || '').toLowerCase().includes(term)
                );
            });
            if (!rows.length) {
                groupsTableBody.innerHTML = '<tr><td colspan="4" class="text-muted">No groups found.</td></tr>';
                return;
            }
            groupsTableBody.innerHTML = '';
            rows.forEach(function(group) {
                const isSelected = selectedSet.has(group.token) || selectedSet.has(group.group_name);
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td><input type="checkbox" class="form-check-input group-select" value="${group.token}" ${isSelected ? 'checked' : ''}></td>
                    <td>${group.group_name}</td>
                    <td>${group.portal_name || group.portal_id}</td>
                    <td>${group.channel_count || 0}</td>
                `;
                groupsTableBody.appendChild(tr);
            });
        }

        function openGroupsModal() {
            if (!groupsModal && window.bootstrap) {
                groupsModal = bootstrap.Modal.getOrCreateInstance(groupsModalEl);
            }
            loadAvailableGroups().then(function() {
                renderGroupsTable(groupsSearch.value);
                if (groupsModal) groupsModal.show();
            });
        }

        function applyGroupsSelection() {
            const checks = Array.from(groupsTableBody.querySelectorAll('.group-select:checked'));
            selectedGroupTokens = checks.map(function(cb) { return cb.value; });
            renderSelectedGroupsSummary();
            if (groupsModal) groupsModal.hide();
        }

        function loadSportsAndLeagues(provider) {
            const selectedProvider = provider || currentProvider();
            const endpoints = providerEndpoints[selectedProvider];
            return Promise.all([
                fetch(endpoints.sports).then(function(resp) { return resp.json(); }),
                fetch(endpoints.leagues).then(function(resp) { return resp.json(); })
            ]).then(function(results) {
                const sportsData = results[0];
                const leaguesData = results[1];
                sportsCache = (sportsData && sportsData.ok && Array.isArray(sportsData.sports)) ? sportsData.sports : [];
                leaguesCache = (leaguesData && leaguesData.ok && Array.isArray(leaguesData.leagues)) ? leaguesData.leagues : [];
            }).catch(function() {
                sportsCache = [];
                leaguesCache = [];
            });
        }

        function loadTeamsForSelectedLeagues(selectedTeamNames, forceRefresh) {
            return Promise.resolve();
        }

        function openModal(rule) {
            if (!bsModal && window.bootstrap) {
                bsModal = bootstrap.Modal.getOrCreateInstance(modalEl);
            }

            fields.provider.value = 'espn';
            loadSportsAndLeagues(fields.provider.value).then(function() {
                if (rule) {
                    fields.id.value = String(rule.id || '');
                    fields.name.value = rule.name || '';
                    fields.provider.value = 'espn';
                    fields.priority.value = String(rule.priority || 100);
                    fields.espnEvents.checked = !!rule.use_espn_events;
                    fields.espnWindow.value = String(rule.espn_event_window_hours || 72);
                    fields.channelRegex.value = rule.channel_regex || '';
                    fields.epgPattern.value = rule.epg_pattern || '';
                    fields.extractRegex.value = rule.extract_regex || '';
                    fields.outputTemplate.value = rule.output_template || '{home} vs {away} | {date} {time}';
                    fields.outputGroup.value = rule.output_group_name || 'EVENTS';
                    fields.channelNumberStart.value = String(rule.channel_number_start || 10000);
                    fields.enabled.checked = !!rule.enabled;
                    selectedGroupTokens = (rule.channel_groups || []).map(function(value) {
                        const raw = String(value || '').trim();
                        if (!raw) return '';
                        if (raw.includes('::')) return raw;
                        return raw;
                    }).filter(Boolean);
                    renderSelectedGroupsSummary();
                    const resolvedSport = resolveSportValue(rule.sport || '');
                    populateSports(resolvedSport);
                    populateLeagues(resolvedSport, rule.league_filters || []);
                    updateOutputPreview();
                    scheduleEspnPreview();
                    modalTitle.textContent = 'Edit Filter';
                } else {
                    clearModal();
                }
                if (bsModal) bsModal.show();
            });
        }

        function renderTeams(rule) {
            return '<span class="text-muted">All teams (ESPN league events).</span>';
        }

        function resolveLeagueLabel(value) {
            const raw = String(value || '').trim();
            if (!raw) return '';
            const byId = leaguesCache.find(function(l) { return String(l.id) === raw; });
            if (byId) return byId.name;
            const byName = leaguesCache.find(function(l) { return String(l.name || '').toLowerCase() === raw.toLowerCase(); });
            if (byName) return byName.name;
            return raw;
        }

        function renderGroups(rule) {
            const groups = Array.isArray(rule.channel_groups) ? rule.channel_groups : [];
            if (!groups.length) return '<span class="text-muted">All groups</span>';
            return groups.map(function(group) {
                return `<span class="badge bg-dark me-1 mb-1">${tokenToLabel(group)}</span>`;
            }).join('');
        }

        function renderRules() {
            if (!rules.length) {
                listEl.innerHTML = `
                    <div class="card">
                        <div class="card-body text-muted">No filters defined yet.</div>
                    </div>
                `;
                return;
            }

            listEl.innerHTML = '';
            rules.forEach(function(rule) {
                const item = document.createElement('div');
                item.className = 'event-filter-item card mb-2';
                item.dataset.ruleId = String(rule.id);
                item.innerHTML = `
                    <div class="event-filter-header card-header">
                        <button class="event-filter-toggle btn btn-link p-0 text-start" type="button" data-action="toggle">
                            <i class="fas fa-chevron-right me-2 event-filter-chevron"></i>
                            <strong>${rule.name || '(Unnamed filter)'}</strong>
                            <span class="ms-2 text-muted">ESPN · ${rule.sport || 'no sport'}</span>
                            <span class="ms-2 badge ${rule.enabled ? 'bg-success' : 'bg-secondary'}">${rule.enabled ? 'enabled' : 'disabled'}</span>
                        </button>
                        <div class="event-filter-actions">
                            <button class="btn btn-sm btn-outline-info" data-action="preview">Preview</button>
                            <button class="btn btn-sm btn-outline-primary" data-action="edit">Edit</button>
                            <button class="btn btn-sm btn-outline-danger" data-action="delete">Delete</button>
                        </div>
                    </div>
                    <div class="event-filter-body card-body d-none">
                        <div class="mb-2"><strong>Teams:</strong> <div class="mt-1">${renderTeams(rule)}</div></div>
                        <div class="mb-2"><strong>Leagues:</strong> <div class="mt-1">${(rule.league_filters || []).length ? (rule.league_filters || []).map(function(l){return `<span class="badge bg-info text-dark me-1 mb-1">${resolveLeagueLabel(l)}</span>`;}).join('') : '<span class="text-muted">No league filter set.</span>'}</div></div>
                        <div class="mb-2"><strong>Groups:</strong> <div class="mt-1">${renderGroups(rule)}</div></div>
                        <div class="mb-2"><strong>Generated Group:</strong> <code>${rule.output_group_name || 'EVENTS'}</code></div>
                        <div class="mb-2"><strong>Number Start:</strong> <code>${rule.channel_number_start || 10000}</code></div>
                        <div class="mb-2"><strong>Channel Regex:</strong> <code>${rule.channel_regex || '-'}</code></div>
                        <div class="mb-2"><strong>EPG Pattern:</strong> <code>${rule.epg_pattern || '-'}</code></div>
                        <div class="mb-3"><strong>Extract Regex:</strong> <code>${rule.extract_regex || '-'}</code></div>
                        <div class="event-filter-preview" data-role="preview">
                            <span class="text-muted">No preview run yet.</span>
                        </div>
                    </div>
                `;

                item.querySelector('[data-action="toggle"]').addEventListener('click', function() {
                    const body = item.querySelector('.event-filter-body');
                    const icon = item.querySelector('.event-filter-chevron');
                    const open = body.classList.contains('d-none');
                    body.classList.toggle('d-none', !open);
                    icon.classList.toggle('fa-chevron-right', !open);
                    icon.classList.toggle('fa-chevron-down', open);
                    if (open) {
                        const previewTarget = item.querySelector('[data-role="preview"]');
                        if (previewTarget && previewTarget.dataset.loaded !== 'true') {
                            runEventPreview(ruleToPreviewPayload(rule), previewTarget);
                        }
                    }
                });
                item.querySelector('[data-action="edit"]').addEventListener('click', function() { openModal(rule); });
                item.querySelector('[data-action="delete"]').addEventListener('click', function() { deleteRule(rule.id); });
                item.querySelector('[data-action="preview"]').addEventListener('click', function() {
                    runChannelPreview(ruleToPreviewPayload(rule), item.querySelector('[data-role="preview"]'));
                });
                const findBtn = document.createElement('button');
                findBtn.className = 'btn btn-sm btn-outline-success';
                findBtn.textContent = 'Find Events';
                findBtn.addEventListener('click', function() {
                    runEventPreview(ruleToPreviewPayload(rule), item.querySelector('[data-role="preview"]'));
                });
                item.querySelector('.event-filter-actions').prepend(findBtn);
                listEl.appendChild(item);
            });
        }

        function runChannelPreview(payload, target) {
            fetch('/api/events/preview/channels', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            })
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        target.innerHTML = `<span class="text-danger">${(data && data.error) || 'Preview failed.'}</span>`;
                        return;
                    }
                    const rows = data.channels || [];
                    if (!rows.length) {
                        target.innerHTML = '<span class="text-muted">No channels matched.</span>';
                        return;
                    }
                    const items = rows.slice(0, 20).map(function(row) {
                        return `<li><strong>${row.channel_name}</strong> <span class="text-muted">(${row.portal_name} / ${row.group_name})</span></li>`;
                    }).join('');
                    const more = data.matched_channels > 20 ? `<div class="text-muted small mt-1">+ ${data.matched_channels - 20} more</div>` : '';
                    target.innerHTML = `
                        <div class="small text-muted mb-1">Matched ${data.matched_channels} of ${data.total_channels} channels</div>
                        <ul class="mb-0">${items}</ul>
                        ${more}
                    `;
                })
                .catch(function() {
                    target.innerHTML = '<span class="text-danger">Preview failed.</span>';
                });
        }

        function runEventPreview(payload, target) {
            lastEventConfig = {
                rule_id: payload.rule_id || null,
                output_group_name: payload.output_group_name || 'EVENTS',
                channel_number_start: Number(payload.channel_number_start || 10000),
                output_template: payload.output_template || '{home} vs {away} | {date} {time}'
            };
            fetch('/api/events/preview/programmes', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            })
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        target.innerHTML = `<span class="text-danger">${(data && data.error) || 'Event preview failed.'}</span>`;
                        return;
                    }
                    const espnEvents = Array.isArray(data.espn_events) ? data.espn_events : [];
                    if (espnEvents.length) {
                        target.dataset.loaded = 'true';
                        espnMatchMap = {};
                        espnReplayMap = {};
                        espnEventMeta = {};
                        espnEvents.forEach(function(ev) {
                            if (ev.event_id) {
                                espnMatchMap[ev.event_id] = ev.matched_channels || [];
                                espnReplayMap[ev.event_id] = ev.replay_channels || [];
                                espnEventMeta[ev.event_id] = {
                                    home: ev.home || '',
                                    away: ev.away || '',
                                    start: ev.start || '',
                                    sport: ev.sport || '',
                                    league: ev.league || ''
                                };
                            }
                        });
                        const rows = espnEvents.slice(0, 25).map(function(ev) {
                            const when = new Date(ev.start).toLocaleString();
                            return `
                                <tr>
                                    <td>${when}</td>
                                    <td>${ev.home || '?'}</td>
                                    <td>${ev.away || '?'}</td>
                                    <td>
                                        <button class="btn btn-sm btn-outline-info" data-action="show-streams" data-event-id="${ev.event_id || ''}">
                                            ${ev.matched_streams || 0}
                                        </button>
                                        ${(ev.replay_streams || 0) ? `<span class="badge bg-secondary ms-1">${ev.replay_streams} replay</span>` : ''}
                                    </td>
                                </tr>
                            `;
                        }).join('');
                        const more = espnEvents.length > 25 ? `<div class="text-muted small mt-1">+ ${espnEvents.length - 25} more</div>` : '';
                        target.innerHTML = `
                            <div class="small text-muted mb-2">Upcoming ESPN fixtures</div>
                            <div class="table-responsive">
                                <table class="table table-sm table-dark mb-0">
                                    <thead>
                                        <tr>
                                            <th>Date/Time</th>
                                            <th>Home</th>
                                            <th>Away</th>
                                            <th>Matched Streams</th>
                                        </tr>
                                    </thead>
                                    <tbody>${rows}</tbody>
                                </table>
                            </div>
                            ${more}
                        `;
                        const streamBtns = target.querySelectorAll('[data-action="show-streams"]');
                        streamBtns.forEach(function(btn) {
                            btn.addEventListener('click', function() {
                                const eventId = btn.getAttribute('data-event-id');
                                showMatchedStreams(eventId);
                            });
                        });
                        return;
                    }
                    if (payload && payload.use_espn_events) {
                        target.dataset.loaded = 'true';
                        target.innerHTML = '<span class="text-muted">Keine ESPN-Spiele im gewählten Zeitraum gefunden.</span>';
                        return;
                    }
                    const rows = data.events || [];
                    if (!rows.length) {
                        target.innerHTML = '<span class="text-muted">No events matched.</span>';
                        return;
                    }
                    target.dataset.loaded = 'true';
                    const items = rows.slice(0, 25).map(function(row) {
                        const match = (row.home || row.away) ? `${row.home || '?'} vs ${row.away || '?'}` : row.title;
                        return `<li><strong>${match}</strong> <span class="text-muted">(${row.channel_name} | ${new Date(row.start).toLocaleString()})</span></li>`;
                    }).join('');
                    const more = data.matched_events > 25 ? `<div class="text-muted small mt-1">+ ${data.matched_events - 25} more</div>` : '';
                    target.innerHTML = `
                        <div class="small text-muted mb-1">Matched ${data.matched_events} events</div>
                        <ul class="mb-0">${items}</ul>
                        ${more}
                    `;
                })
                .catch(function() {
                    target.innerHTML = '<span class="text-danger">Event preview failed.</span>';
                });
        }

        function loadRules() {
            fetch('/api/events/rules')
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        showToast('Could not load filters.', 'error');
                        return;
                    }
                    rules = data.rules || [];
                    renderRules();
                })
                .catch(function() {
                    showToast('Could not load filters.', 'error');
                });
        }

        function saveRule() {
            const payload = payloadFromModal();
            if (!payload.rule.name) {
                showToast('Filter name is required.', 'warning');
                return;
            }
            const id = (fields.id.value || '').trim();
            const method = id ? 'PUT' : 'POST';
            const url = id ? `/api/events/rules/${encodeURIComponent(id)}` : '/api/events/rules';
            fetch(url, {
                method: method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            })
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        showToast((data && data.error) || 'Could not save filter.', 'error');
                        return;
                    }
                    if (bsModal) bsModal.hide();
                    showToast('Filter saved.', 'success');
                    loadRules();
                })
                .catch(function() {
                    showToast('Could not save filter.', 'error');
                });
        }

        function deleteRule(id) {
            fetch(`/api/events/rules/${encodeURIComponent(id)}`, { method: 'DELETE' })
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        showToast((data && data.error) || 'Could not delete filter.', 'error');
                        return;
                    }
                    showToast('Filter deleted.', 'success');
                    loadRules();
                })
                .catch(function() {
                    showToast('Could not delete filter.', 'error');
                });
        }

        function showMatchedStreams(eventId) {
            const listEl = document.getElementById('matchedStreamsList');
            const modalEl = document.getElementById('matchedStreamsModal');
            if (!listEl || !modalEl) return;
            const rows = Array.isArray(espnMatchMap[eventId]) ? espnMatchMap[eventId] : [];
            const replayRows = Array.isArray(espnReplayMap[eventId]) ? espnReplayMap[eventId] : [];
            lastMatchedRows = rows;
            if (!rows.length) {
                listEl.innerHTML = '<span class="text-muted">No matched streams.</span>';
            } else {
                const items = rows.map(function(row, idx) {
                    const group = row.group_name ? ` / ${row.group_name}` : '';
                    const prog = row.program_title ? `${row.program_title}` : '';
                    const sub = row.program_sub_title ? `${row.program_sub_title}` : '';
                    const desc = row.program_description ? `${row.program_description}` : '';
                    const when = row.program_start ? ` · ${new Date(row.program_start).toLocaleString()}` : '';
                    const details = [];
                    if (prog || when) details.push(`<div class="text-muted small">${prog}${when}</div>`);
                    if (sub) details.push(`<div class="text-muted small">${sub}</div>`);
                    if (desc) details.push(`<div class="text-muted small">${desc}</div>`);
                    const extra = details.join('');
                    const eventMeta = espnEventMeta[eventId] || {};
                    const members = Array.isArray(row.group_members) ? row.group_members : null;
                    const createdCount = members ? members.filter(m => m.created_event_channel_id).length : (row.created_event_channel_id ? 1 : 0);
                    const totalCount = members ? members.length : 1;
                    const isCreated = createdCount === totalCount && totalCount > 0;
                    const isPartial = createdCount > 0 && createdCount < totalCount;
                    const badge = isPartial ? `<span class="badge bg-warning text-dark ms-2">Partial</span>` : '';
                    const createButton = `
                        <button type="button" class="btn btn-sm btn-outline-success"
                            data-action="create-event-channel"
                            data-event-id="${eventId}"
                            data-row-index="${idx}">
                            Create Channel
                        </button>
                    `;
                    const deleteButton = `
                        <button type="button" class="btn btn-sm btn-outline-danger"
                            data-action="delete-event-channel"
                            data-event-id="${eventId}"
                            data-row-index="${idx}">
                            Delete Channel
                        </button>
                    `;
                    return `
                        <li class="d-flex justify-content-between align-items-start">
                            <div>
                                <strong>${row.channel_name}</strong>
                                <span class="text-muted">(${row.portal_name}${group})</span>
                                ${badge}
                                ${extra}
                            </div>
                            ${isCreated ? deleteButton : createButton}
                        </li>
                    `;
                }).join('');
                listEl.innerHTML = `<ul class="mb-0">${items}</ul>`;
                const buttons = listEl.querySelectorAll('[data-action="create-event-channel"]');
                buttons.forEach(function(btn) {
                    btn.addEventListener('click', function() {
                        const rowIndex = Number(btn.getAttribute('data-row-index') || 0);
                        const row = rows[rowIndex];
                        const members = Array.isArray(row.group_members) ? row.group_members : [row];
                        members.forEach(function(member) {
                            if (member.created_event_channel_id) return;
                            const payload = {
                                rule_id: lastEventConfig.rule_id,
                                event_id: btn.getAttribute('data-event-id'),
                                portal_id: member.portal_id,
                                channel_id: member.channel_id,
                                home: (espnEventMeta[eventId] || {}).home || '',
                                away: (espnEventMeta[eventId] || {}).away || '',
                                start: (espnEventMeta[eventId] || {}).start || '',
                                sport: (espnEventMeta[eventId] || {}).sport || '',
                                league: (espnEventMeta[eventId] || {}).league || '',
                                output_group_name: lastEventConfig.output_group_name,
                                channel_number_start: lastEventConfig.channel_number_start,
                                output_template: lastEventConfig.output_template
                            };
                            createEventChannel(payload);
                        });
                    });
                });
                const deleteButtons = listEl.querySelectorAll('[data-action="delete-event-channel"]');
                deleteButtons.forEach(function(btn) {
                    btn.addEventListener('click', async function() {
                        const rowIndex = Number(btn.getAttribute('data-row-index') || 0);
                        const row = rows[rowIndex];
                        const members = Array.isArray(row.group_members) ? row.group_members : [row];
                        const confirmed = await confirmAction({
                            title: 'Delete Event Channel',
                            message: 'Diesen Event-Channel wirklich entfernen?',
                            okText: 'Delete',
                            type: 'danger'
                        });
                        if (!confirmed) return;
                        members.forEach(function(member) {
                            if (!member.created_event_channel_id) return;
                            deleteEventChannel(eventId, {
                                portal_id: member.created_event_portal_id,
                                channel_id: member.created_event_channel_id,
                                source_portal_id: member.portal_id,
                                source_channel_id: member.channel_id
                            });
                        });
                    });
                });
            }
            if (replayRows.length) {
                const replayItems = replayRows.map(function(row) {
                    const group = row.group_name ? ` / ${row.group_name}` : '';
                    const prog = row.program_title ? `${row.program_title}` : '';
                    const sub = row.program_sub_title ? `${row.program_sub_title}` : '';
                    const desc = row.program_description ? `${row.program_description}` : '';
                    const when = row.program_start ? ` · ${new Date(row.program_start).toLocaleString()}` : '';
                    const details = [];
                    if (prog || when) details.push(`<div class="text-muted small">${prog}${when}</div>`);
                    if (sub) details.push(`<div class="text-muted small">${sub}</div>`);
                    if (desc) details.push(`<div class="text-muted small">${desc}</div>`);
                    const extra = details.join('');
                    return `
                        <li class="d-flex justify-content-between align-items-start">
                            <div>
                                <strong>${row.channel_name}</strong>
                                <span class="text-muted">(${row.portal_name}${group})</span>
                                ${extra}
                            </div>
                            <span class="badge bg-secondary">Replay</span>
                        </li>
                    `;
                }).join('');
                const replayBlock = `
                    <div class="mt-3">
                        <div class="text-muted small mb-2">Replays</div>
                        <ul class="mb-0">${replayItems}</ul>
                    </div>
                `;
                listEl.innerHTML = `${listEl.innerHTML}${replayBlock}`;
            }
            if (!matchedStreamsModal && window.bootstrap) {
                matchedStreamsModal = bootstrap.Modal.getOrCreateInstance(modalEl);
            }
            if (matchedStreamsModal) matchedStreamsModal.show();
        }

        function bindMatchedStreamsActions() {
            const listEl = document.getElementById('matchedStreamsList');
            if (!listEl) return;
            listEl.addEventListener('click', async function(evt) {
                const btn = evt.target.closest('[data-action]');
                if (!btn) return;
                const action = btn.getAttribute('data-action');
                if (action === 'create-event-channel') {
                    const eventId = btn.getAttribute('data-event-id');
                    const rowIndex = Number(btn.getAttribute('data-row-index') || 0);
                    const row = lastMatchedRows[rowIndex];
                    if (!row) return;
                    const members = Array.isArray(row.group_members) ? row.group_members : [row];
                    members.forEach(function(member) {
                        if (member.created_event_channel_id) return;
                        const payload = {
                            rule_id: lastEventConfig.rule_id,
                            event_id: eventId,
                            portal_id: member.portal_id,
                            channel_id: member.channel_id,
                            home: (espnEventMeta[eventId] || {}).home || '',
                            away: (espnEventMeta[eventId] || {}).away || '',
                            start: (espnEventMeta[eventId] || {}).start || '',
                            sport: (espnEventMeta[eventId] || {}).sport || '',
                            league: (espnEventMeta[eventId] || {}).league || '',
                            output_group_name: lastEventConfig.output_group_name,
                            channel_number_start: lastEventConfig.channel_number_start,
                            output_template: lastEventConfig.output_template
                        };
                        createEventChannel(payload);
                    });
                }
                if (action === 'delete-event-channel') {
                    const eventId = btn.getAttribute('data-event-id');
                    const rowIndex = Number(btn.getAttribute('data-row-index') || 0);
                    const row = lastMatchedRows[rowIndex];
                    if (!row) return;
                    const members = Array.isArray(row.group_members) ? row.group_members : [row];
                    const confirmed = await confirmAction({
                        title: 'Delete Event Channel',
                        message: 'Diesen Event-Channel wirklich entfernen?',
                        okText: 'Delete',
                        type: 'danger'
                    });
                    if (!confirmed) return;
                    members.forEach(function(member) {
                        if (!member.created_event_channel_id) return;
                        deleteEventChannel(eventId, {
                            portal_id: member.created_event_portal_id,
                            channel_id: member.created_event_channel_id,
                            source_portal_id: member.portal_id,
                            source_channel_id: member.channel_id
                        });
                    });
                }
            });
        }

        function createEventChannel(payload) {
            fetch('/api/events/create_channel', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            })
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        if (typeof showToast === 'function') {
                            showToast((data && data.error) || 'Could not create channel.', 'error');
                        }
                        return;
                    }
                    if (typeof showToast === 'function') {
                        showToast('Event channel created.', 'success');
                    }
                    if (payload && payload.event_id && payload.portal_id && payload.channel_id && data.channel_id && espnMatchMap[payload.event_id]) {
                        espnMatchMap[payload.event_id] = espnMatchMap[payload.event_id].map(function(row) {
                            if (row.portal_id === payload.portal_id && row.channel_id === payload.channel_id) {
                                return Object.assign({}, row, {
                                    created_event_channel_id: data.channel_id,
                                    created_event_portal_id: payload.portal_id
                                });
                            }
                            return row;
                        });
                    }
                    if (payload && payload.event_id) {
                        showMatchedStreams(payload.event_id);
                    }
                })
                .catch(function() {
                    if (typeof showToast === 'function') {
                        showToast('Could not create channel.', 'error');
                    }
                });
        }

        function deleteEventChannel(eventId, payload) {
            fetch('/api/events/delete_channel', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            })
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        if (typeof showToast === 'function') {
                            showToast((data && data.error) || 'Could not delete channel.', 'error');
                        }
                        return;
                    }
                    if (eventId && payload && payload.source_portal_id && payload.source_channel_id && espnMatchMap[eventId]) {
                        espnMatchMap[eventId] = espnMatchMap[eventId].map(function(row) {
                            if (row.portal_id === payload.source_portal_id && row.channel_id === payload.source_channel_id) {
                                return Object.assign({}, row, {
                                    created_event_channel_id: '',
                                    created_event_portal_id: ''
                                });
                            }
                            return row;
                        });
                    }
                    if (typeof showToast === 'function') {
                        showToast('Event channel deleted.', 'success');
                    }
                    if (eventId) showMatchedStreams(eventId);
                })
                .catch(function() {
                    if (typeof showToast === 'function') {
                        showToast('Could not delete channel.', 'error');
                    }
                });
        }

        function showConfirmDialog(options) {
            return new Promise((resolve) => {
                const modal = document.getElementById('confirmModal');
                const titleEl = document.getElementById('confirmModalTitle');
                const messageEl = document.getElementById('confirmModalMessage');
                const iconEl = document.getElementById('confirmModalIcon');
                const okBtn = document.getElementById('confirmModalOk');
                const okTextEl = document.getElementById('confirmModalOkText');

                titleEl.textContent = options.title || 'Confirm';
                messageEl.textContent = options.message || 'Are you sure?';
                okTextEl.textContent = options.okText || 'OK';

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

                const bsModal = new bootstrap.Modal(modal);

                const handleOk = () => {
                    okBtn.removeEventListener('click', handleOk);
                    modal.removeEventListener('hidden.bs.modal', handleCancel);
                    bsModal.hide();
                    resolve(true);
                };

                const handleCancel = () => {
                    okBtn.removeEventListener('click', handleOk);
                    resolve(false);
                };

                okBtn.addEventListener('click', handleOk);
                modal.addEventListener('hidden.bs.modal', handleCancel, { once: true });
                bsModal.show();
            });
        }

        function confirmAction(options) {
            return showConfirmDialog(options || {});
        }

        fields.sport.addEventListener('change', function() {
            populateLeagues(fields.sport.value, []);
        });

        fields.leagues.addEventListener('change', function() {});

        addGroupsBtn.addEventListener('click', openGroupsModal);
        clearGroupsBtn.addEventListener('click', function() {
            selectedGroupTokens = [];
            renderSelectedGroupsSummary();
        });
        applyGroupsBtn.addEventListener('click', applyGroupsSelection);
        groupsSearch.addEventListener('input', function() {
            renderGroupsTable(groupsSearch.value);
        });

        ['filterOutputTemplate', 'filterOutputGroup'].forEach(function(id) {
            const el = document.getElementById(id);
            if (el) {
                el.addEventListener('input', updateOutputPreview);
            }
        });
        if (fields.sport) {
            fields.sport.addEventListener('change', function() {
                populateLeagues(resolveSportValue(fields.sport.value), selectedValues(fields.leagues));
                updateOutputPreview();
                scheduleEspnPreview();
            });
        }
        if (fields.leagues) {
            fields.leagues.addEventListener('change', function() {
                updateOutputPreview();
                scheduleEspnPreview();
            });
        }
        if (fields.espnEvents) {
            fields.espnEvents.addEventListener('change', function() {
                scheduleEspnPreview();
            });
        }

        refreshSportsDbBtn.addEventListener('click', function() {
            const provider = currentProvider();
            const endpoints = providerEndpoints[provider];
            fetch(endpoints.refresh, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ force: true })
            })
                .then(function(resp) { return resp.json(); })
                .then(function(data) {
                    if (!data || !data.ok) {
                        showToast((data && data.error) || `${provider.toUpperCase()} refresh failed.`, 'error');
                        return;
                    }
                    showToast(`${provider.toUpperCase()} cache refreshed.`, 'success');
                    loadSportsAndLeagues(provider);
                })
                .catch(function() {
                    showToast(`${provider.toUpperCase()} refresh failed.`, 'error');
                });
        });

        addBtn.addEventListener('click', function() { openModal(null); });
        saveBtn.addEventListener('click', saveRule);
        findEventsModalBtn.addEventListener('click', function() {
            const payload = payloadFromModal().rule;
            const target = selectedGroupsSummary;
            runEventPreview(
                {
                    provider: 'espn',
                    use_espn_events: !!payload.use_espn_events,
                    espn_event_window_hours: Number(payload.espn_event_window_hours || 72),
                    sport: payload.sport || '',
                    groups: selectedGroupTokens.slice(),
                    channel_regex: payload.channel_regex || '',
                    epg_pattern: payload.epg_pattern || '',
                    extract_regex: payload.extract_regex || '',
                    league_filters: payload.league_filters || [],
                    team_filters: payload.team_filters || [],
                    output_group_name: payload.output_group_name || 'EVENTS',
                    channel_number_start: Number(payload.channel_number_start || 10000),
                    output_template: payload.output_template || '{home} vs {away} | {date} {time}'
                },
                target
            );
        });
        // Load rules independently so the page never stays on "Loading filters..."
        // when provider/group cache requests are slow or temporarily unavailable.
        loadRules();
        loadSportsAndLeagues(currentProvider());
        loadAvailableGroups();
        bindMatchedStreamsActions();
    }

    window.App && window.App.register('events', initEventsPage, function() {});
    if (window.App && typeof window.App.run === 'function') {
        window.App.run({ preserveScroll: true });
    }
})();
