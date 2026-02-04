(function() {
    function initPortalsPage(pageData) {
        pageData = pageData || {};

const portalsData = (pageData.portals || []);
portalsData.__settings__ = (pageData.settings || {});

// View toggle
function setView(view) {
    const cardView = document.getElementById('cardView');
    const listView = document.getElementById('listView');
    const cardBtn = document.getElementById('cardViewBtn');
    const listBtn = document.getElementById('listViewBtn');

    if (view === 'card') {
        cardView.classList.add('active');
        listView.classList.remove('active');
        cardBtn.classList.add('active');
        listBtn.classList.remove('active');
    } else {
        cardView.classList.remove('active');
        listView.classList.add('active');
        cardBtn.classList.remove('active');
        listBtn.classList.add('active');
    }

    localStorage.setItem('portalsView', view);
}

// Toggle portal expand/collapse (list view)
function togglePortal(portalId) {
    const item = document.querySelector(`.portal-list-item[data-portal-id="${portalId}"]`);
    if (item) {
        item.classList.toggle('expanded');
    }
}

// Edit portal
let currentEditPortalId = null;
let currentEditSelectedGenres = [];

function editPortal(portalId) {
    const portal = portalsData[portalId];
    if (!portal) return;

    currentEditPortalId = portalId;
    currentEditSelectedGenres = portal.selected_genres || [];

    const toBool = (value, fallback = false) => {
        if (value === undefined || value === null) return fallback;
        if (value === true || value === false) return value;
        if (typeof value === 'number') return value !== 0;
        return String(value).toLowerCase() === 'true';
    };

    document.getElementById('edit_portal_id').value = portalId;
    document.getElementById('edit_enabled').checked = toBool(portal.enabled, true);
    document.getElementById('edit_name').value = portal.name;
    document.getElementById('edit_url').value = portal.url;
    document.getElementById('edit_macs').value = Object.keys(portal.macs).join(',');
    document.getElementById('edit_streams_per_mac').value = portal['streams per mac'];
    document.getElementById('edit_epg_offset').value = portal['epg offset'];
    document.getElementById('edit_proxy').value = portal.proxy || '';
    document.getElementById('edit_fetch_epg').checked = toBool(portal['fetch epg'], true);
    document.getElementById('edit_auto_normalize').checked = toBool(portal['auto normalize names'], false);
    document.getElementById('edit_auto_match').checked = toBool(portal['auto match'], false);

    new bootstrap.Modal(document.getElementById('editPortalModal')).show();
}

// Preserve selected_genres when updating portal
document.getElementById('editPortalForm')?.addEventListener('submit', function(e) {
    // Remove any existing hidden genre inputs
    this.querySelectorAll('input[name="selected_genres"]').forEach(el => el.remove());

    // Keep the original genre selection (genres are managed via separate modal)
    currentEditSelectedGenres.forEach(g => {
        const input = document.createElement('input');
        input.type = 'hidden';
        input.name = 'selected_genres';
        input.value = g;
        this.appendChild(input);
    });
});

// Delete portal
function deletePortal(portalId, portalName) {
    document.getElementById('delete_portal_id').value = portalId;
    document.getElementById('delete_portal_name').textContent = portalName;
    new bootstrap.Modal(document.getElementById('deletePortalModal')).show();
}

// Delete individual MAC
let pendingMacDelete = { portalId: null, mac: null };

function deleteMac(portalId, mac) {
    pendingMacDelete = { portalId, mac };
    document.getElementById('delete_mac_address').textContent = mac;
    new bootstrap.Modal(document.getElementById('deleteMacModal')).show();
}

async function confirmMacDelete() {
    const { portalId, mac } = pendingMacDelete;
    if (!portalId || !mac) return;

    try {
        const response = await fetch('/api/portal/mac/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ portal_id: portalId, mac: mac })
        });

        const data = await response.json();

        if (data.success) {
            // Remove the row from the table
            const row = document.querySelector(`tr[data-mac="${mac}"]`);
            if (row) {
                row.remove();
            }

            // Update MAC count in the header
            const item = document.querySelector(`.portal-list-item[data-portal-id="${portalId}"]`);
            if (item) {
                const countSpan = item.querySelector('.portal-meta-item span');
                const currentCount = parseInt(countSpan.textContent) - 1;
                countSpan.textContent = `${currentCount} MACs`;

                // Update section title
                const sectionTitle = item.querySelector('.mac-section-title');
                if (sectionTitle) {
                    sectionTitle.innerHTML = `<i class="fas fa-list"></i> MAC Addresses (${currentCount})`;
                }

                // Show "no macs" message if all deleted
                if (currentCount === 0) {
                    const tableWrapper = item.querySelector('.mac-table');
                    if (tableWrapper) {
                        tableWrapper.outerHTML = `
                            <div class="no-macs">
                                <i class="fas fa-network-wired fa-2x mb-2"></i>
                                <p>No MAC addresses configured</p>
                            </div>`;
                    }
                }
            }

            // Update local data
            if (portalsData[portalId] && portalsData[portalId].macs) {
                delete portalsData[portalId].macs[mac];
            }

            // Reload the page to reflect changes
            location.reload();
        } else {
            showToast('Error: ' + (data.message || 'Failed to delete MAC'), 'error');
        }
    } catch (error) {
        console.error('Error deleting MAC:', error);
        showToast('Error deleting MAC address', 'error');
    }

    bootstrap.Modal.getInstance(document.getElementById('deleteMacModal')).hide();
}

// Refresh MAC data for a portal
async function refreshPortalMacs(portalId) {
    const btn = event.target.closest('button');
    const originalContent = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Refreshing...';

    try {
        const response = await fetch('/api/portal/macs/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ portal_id: portalId })
        });

        const data = await response.json();

        if (data.success) {
            // Update local data and refresh table
            if (data.macs) {
                portalsData[portalId].macs = data.macs;
                updateMacTable(portalId, data.macs);
            }
            showToast(data.message, 'success');
        } else {
            showToast('Error: ' + (data.message || 'Failed to refresh MACs'), 'error');
        }
    } catch (error) {
        console.error('Error refreshing MACs:', error);
        showToast('Error refreshing MAC data', 'error');
    }

    btn.disabled = false;
    btn.innerHTML = originalContent;
}

// Update MAC table with new data
function updateMacTable(portalId, macs) {
    console.log('updateMacTable called for portal:', portalId, 'with macs:', macs);

    const portalItem = document.querySelector(`.portal-list-item[data-portal-id="${portalId}"]`);
    if (!portalItem) {
        console.error('Portal item not found for ID:', portalId);
        return;
    }

    const tbody = portalItem.querySelector('.mac-table tbody');
    if (!tbody) {
        console.error('MAC table tbody not found');
        return;
    }

    for (const [mac, macData] of Object.entries(macs)) {
        console.log('Processing MAC:', mac, 'Data:', macData);

        const row = tbody.querySelector(`tr[data-mac="${mac}"]`);
        if (!row) {
            console.warn('Row not found for MAC:', mac);
            continue;
        }

        const expiry = typeof macData === 'object' ? (macData.expiry || 'Unknown') : (macData || 'Unknown');
        const watchdog = typeof macData === 'object' ? (macData.watchdog_timeout || 0) : 0;
        const playback = typeof macData === 'object' ? (macData.playback_limit || 0) : 0;

        console.log('Updating row - expiry:', expiry, 'watchdog:', watchdog, 'playback:', playback);

        // Update expiry cell - both text and data attribute
        const expiryCell = row.querySelector('.mac-expiry');
        if (expiryCell) {
            expiryCell.textContent = expiry;
            expiryCell.setAttribute('data-expiry', expiry);
        }

        // Update days left cell - set data attribute for calculation
        const daysCell = row.querySelector('.mac-days-left');
        if (daysCell) {
            daysCell.setAttribute('data-expiry', expiry);
        }

        // Update watchdog cell
        const watchdogCell = row.querySelector('.mac-watchdog');
        if (watchdogCell) {
            watchdogCell.setAttribute('data-watchdog', watchdog);
        }

        // Update playback limit cell
        const playbackCell = row.querySelector('.mac-playback-limit');
        if (playbackCell) {
            playbackCell.textContent = playback > 0 ? playback : '-';
        }
    }

    // Re-calculate days left and watchdog badges
    console.log('Recalculating expiry highlights and watchdog badges');
    highlightExpiringMacs();
    formatWatchdogBadges();
}

// Parse date string like "March 2, 2026, 12:00 am" to Date object
function parseExpiryDate(dateStr) {
    if (!dateStr) return null;
    try {
        return new Date(dateStr);
    } catch (e) {
        return null;
    }
}

// Sort MAC tables by expiration date (earliest first)
function sortMacTablesByExpiry() {
    document.querySelectorAll('.mac-table tbody').forEach(tbody => {
        const rows = Array.from(tbody.querySelectorAll('tr'));

        rows.sort((a, b) => {
            const expiryA = parseExpiryDate(a.querySelector('.mac-expiry')?.dataset.expiry);
            const expiryB = parseExpiryDate(b.querySelector('.mac-expiry')?.dataset.expiry);

            // Handle null dates (put them at the end)
            if (!expiryA && !expiryB) return 0;
            if (!expiryA) return 1;
            if (!expiryB) return -1;

            return expiryA - expiryB; // Earliest first
        });

        // Re-append sorted rows
        rows.forEach(row => tbody.appendChild(row));
    });
}

// Calculate remaining days and highlight MACs
function highlightExpiringMacs() {
    const now = new Date();
    const thirtyDaysFromNow = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);
    const oneDay = 24 * 60 * 60 * 1000;
    const statusClasses = ['status-expired', 'status-expiring-soon', 'status-valid'];

    document.querySelectorAll('.mac-expiry').forEach(cell => {
        const expiryStr = cell.dataset.expiry;
        // Clear old status classes
        cell.classList.remove(...statusClasses);

        if (!expiryStr || expiryStr === 'Unknown') return;

        try {
            const expiry = new Date(expiryStr);
            if (isNaN(expiry.getTime())) return; // Invalid date

            if (expiry < now) {
                cell.classList.add('status-expired');
            } else if (expiry < thirtyDaysFromNow) {
                cell.classList.add('status-expiring-soon');
            } else {
                cell.classList.add('status-valid');
            }
        } catch (e) {
            // Invalid date format, skip
        }
    });

    // Calculate and display remaining days
    document.querySelectorAll('.mac-days-left').forEach(cell => {
        const expiryStr = cell.dataset.expiry;
        // Clear old status classes
        cell.classList.remove(...statusClasses);

        if (!expiryStr || expiryStr === 'Unknown') {
            cell.textContent = '-';
            return;
        }

        try {
            const expiry = new Date(expiryStr);
            if (isNaN(expiry.getTime())) {
                cell.textContent = '-';
                return;
            }

            const diffTime = expiry - now;
            const diffDays = Math.ceil(diffTime / oneDay);

            if (diffDays < 0) {
                cell.textContent = 'Expired';
                cell.classList.add('status-expired');
            } else if (diffDays === 0) {
                cell.textContent = 'Today';
                cell.classList.add('status-expiring-soon');
            } else if (diffDays === 1) {
                cell.textContent = '1 day';
                cell.classList.add('status-expiring-soon');
            } else if (diffDays <= 30) {
                cell.textContent = `${diffDays} days`;
                cell.classList.add('status-expiring-soon');
            } else {
                cell.textContent = `${diffDays} days`;
                cell.classList.add('status-valid');
            }
        } catch (e) {
            cell.textContent = '-';
        }
    });
}

// Format watchdog timeout badges with color coding
function formatWatchdogBadges() {
    document.querySelectorAll('.mac-watchdog').forEach(cell => {
        const watchdog = parseInt(cell.dataset.watchdog) || 0;
        const badge = cell.querySelector('.watchdog-badge');
        if (!badge) return;

        let text, colorClass, title;
        if (watchdog === 0) {
            // Keine Daten verfügbar
            text = '-';
            colorClass = 'bg-secondary';
            title = 'Keine Watchdog-Daten verfügbar';
        } else if (watchdog < 60) {
            // Sehr aktiv - rot
            text = `${watchdog}s`;
            colorClass = 'bg-danger';
            title = 'Sehr aktiv (< 60s) - MAC wird gerade genutzt';
        } else if (watchdog < 300) {
            // Aktiv - gelb
            const mins = Math.floor(watchdog / 60);
            text = `${mins}m`;
            colorClass = 'bg-warning text-dark';
            title = 'Aktiv (1-5 min) - Kürzlich genutzt';
        } else if (watchdog < 1800) {
            // Moderat - blau
            const mins = Math.floor(watchdog / 60);
            text = `${mins}m`;
            colorClass = 'bg-info';
            title = 'Moderate Aktivität (5-30 min)';
        } else {
            // Idle - grün
            const mins = Math.floor(watchdog / 60);
            text = mins >= 60 ? `${Math.floor(mins / 60)}h` : `${mins}m`;
            colorClass = 'bg-success';
            title = 'Idle (> 30 min) - Sicher zu benutzen';
        }

        badge.className = `watchdog-badge badge ${colorClass}`;
        badge.textContent = text;
        badge.title = title;
    });
}

// Calculate and display LATEST MAC expiry for each portal in list view
function updatePortalExpiryIndicators() {
    const now = new Date();
    const oneDay = 24 * 60 * 60 * 1000;

    document.querySelectorAll('.portal-stat-expiry').forEach(indicator => {
        const portalId = indicator.dataset.portalId;
        const portalItem = indicator.closest('.portal-list-item');
        if (!portalItem) return;

        const expiryDates = [];
        portalItem.querySelectorAll('.mac-expiry').forEach(cell => {
            const expiryStr = cell.dataset.expiry;
            if (expiryStr) {
                try {
                    expiryDates.push(new Date(expiryStr));
                } catch (e) {}
            }
        });

        const valueSpan = indicator.querySelector('.portal-expiry-value');
        if (expiryDates.length === 0) {
            valueSpan.textContent = '-';
            return;
        }

        // Find LATEST (last) expiry - maximum date
        const latest = expiryDates.reduce((max, d) => d > max ? d : max);
        const diffDays = Math.ceil((latest - now) / oneDay);

        if (diffDays < 0) {
            valueSpan.textContent = 'Exp!';
            valueSpan.classList.add('status-expired');
        } else if (diffDays === 0) {
            valueSpan.textContent = 'Today';
            valueSpan.classList.add('status-expiring-soon');
        } else if (diffDays <= 30) {
            valueSpan.textContent = `${diffDays}d`;
            valueSpan.classList.add('status-expiring-soon');
        } else {
            valueSpan.textContent = `${diffDays}d`;
            valueSpan.classList.add('status-valid');
        }
    });
}

// Event delegation for portal actions
document.addEventListener('click', function(e) {
    const btn = e.target.closest('[data-action]');
    if (!btn) return;

    const action = btn.dataset.action;
    const portalId = btn.dataset.portalId;

    if (action === 'edit') {
        editPortal(portalId);
    } else if (action === 'delete') {
        deletePortal(portalId, btn.dataset.portalName);
    }
});

// ===== Genre Tile Helper =====
function toggleGenreTile(tile) {
    tile.classList.toggle('selected');
}

// ========== Channel Refresh Function ==========
async function refreshPortalChannels(portalId) {
    const portal = portalsData[portalId];
    const portalName = portal ? portal.name : portalId;

    // Find and disable the refresh button, show spinner
    const buttons = document.querySelectorAll(`button[onclick*="refreshPortalChannels('${portalId}')"]`);
    buttons.forEach(btn => {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    });

    showToast(`Refreshing channels for "${portalName}"...`, 'info');

    try {
        const response = await fetch('/api/portal/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ portal_id: portalId })
        });

        const data = await response.json();

        if (data.status === 'queued') {
            showToast('Refresh queued. It will run after the current one finishes.', 'info');
            pollRefreshStatus(portalId);
        } else if (data.status === 'running') {
            showToast('Refresh already running. Please wait...', 'warning');
            pollRefreshStatus(portalId);
        } else if (data.status === 'completed') {
            if (data.stats) {
                showToast(`${data.stats.channels} channels loaded for "${data.portal}"`, 'success');
                updatePortalStats(portalId, data.stats);
            } else {
                showToast(`Refresh completed for "${portalName}"`, 'success');
            }
        } else {
            showToast('Error: ' + (data.message || 'Unknown error'), 'error');
        }
    } catch (error) {
        showToast('Error refreshing channels: ' + error, 'error');
    } finally {
        // Re-enable buttons
        buttons.forEach(btn => {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-sync"></i>';
        });
    }
}

function pollRefreshStatus(portalId) {
    const startedAt = Date.now();
    const poll = () => {
        fetch('/api/portal/refresh/status', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ portal_id: portalId })
        })
            .then(res => res.json())
            .then(data => {
                if (data.status === 'completed') {
                    if (data.stats) {
                        showToast(`Refresh completed: ${data.stats.channels} channels`, 'success');
                        updatePortalStats(portalId, data.stats);
                    } else {
                        showToast('Refresh completed.', 'success');
                    }
                    return;
                }
                if (data.status === 'error') {
                    showToast(`Refresh failed: ${data.error || 'Unknown error'}`, 'error');
                    return;
                }
                if (Date.now() - startedAt < 300000) {
                    setTimeout(poll, 2000);
                }
            })
            .catch(() => {
                if (Date.now() - startedAt < 300000) {
                    setTimeout(poll, 3000);
                }
            });
    };
    setTimeout(poll, 1500);
}

function updatePortalStats(portalId, stats) {
    // Update card view stats
    const cardItem = document.querySelector(`.portal-card[data-portal-id="${portalId}"]`);
    if (cardItem) {
        // Update channels display
        const channelCol = cardItem.querySelector('.card-stat-channels');
        if (channelCol) {
            if (stats.total_channels > 0 && stats.total_channels !== stats.channels) {
                channelCol.innerHTML = `<strong><i class="fas fa-tv"></i> Channels:</strong>
                    <span class="text-primary">${stats.channels}</span> / ${stats.total_channels}`;
            } else {
                channelCol.innerHTML = `<strong><i class="fas fa-tv"></i> Channels:</strong> ${stats.channels}`;
            }
        }

        // Update groups display
        const groupCol = cardItem.querySelector('.card-stat-groups');
        if (groupCol) {
            if (stats.total_groups > 0 && stats.total_groups !== stats.groups) {
                groupCol.innerHTML = `<strong><i class="fas fa-folder"></i> Groups:</strong>
                    <span class="text-primary">${stats.groups}</span> / ${stats.total_groups}`;
            } else {
                groupCol.innerHTML = `<strong><i class="fas fa-folder"></i> Groups:</strong> ${stats.groups}`;
            }
        }
    }

    // Update list view stats
    const listItem = document.querySelector(`.portal-list-item[data-portal-id="${portalId}"]`);
    if (listItem) {
        // Update header meta (total channels/groups)
        const channelMeta = listItem.querySelector('.portal-stat-channels .portal-meta-value');
        if (channelMeta) channelMeta.textContent = stats.total_channels;

        const groupMeta = listItem.querySelector('.portal-stat-groups .portal-meta-value');
        if (groupMeta) groupMeta.textContent = stats.total_groups;

        // Update details tiles
        const channelTile = listItem.querySelector('.portal-info-tile:nth-child(1) .portal-info-value-large');
        if (channelTile) {
            if (stats.total_channels > 0 && stats.total_channels !== stats.channels) {
                channelTile.innerHTML = `<span class="text-primary">${stats.channels}</span><span class="text-muted"> / ${stats.total_channels}</span>`;
            } else {
                channelTile.innerHTML = `<span class="text-primary">${stats.channels}</span>`;
            }
        }

        const groupTile = listItem.querySelector('.portal-info-tile:nth-child(2) .portal-info-value-large');
        if (groupTile) {
            if (stats.total_groups > 0 && stats.total_groups !== stats.groups) {
                groupTile.innerHTML = `<span class="text-primary">${stats.groups}</span><span class="text-muted"> / ${stats.total_groups}</span>`;
            } else {
                groupTile.innerHTML = `<span class="text-primary">${stats.groups}</span>`;
            }
        }
    }
}

// ========== Standalone Genre Modal Functions ==========
let currentGenreModalPortalId = null;
let currentGenreModalMac = null;  // MAC used to fetch genres
let genreModalGenres = [];

async function openGenreModal(portalId) {
    currentGenreModalPortalId = portalId;
    const portal = portalsData[portalId];

    if (!portal) {
        showToast('Portal not found', 'error');
        return;
    }

    // Set modal title
    document.getElementById('genreModalPortalName').textContent = portal.name || 'Unnamed Portal';

    // Reset modal state
    document.getElementById('genreModalLoading').style.display = 'block';
    document.getElementById('genreModalContent').style.display = 'none';
    document.getElementById('genreModalError').style.display = 'none';
    document.getElementById('genreModalSearchInput').value = '';
    document.getElementById('genreModalTilesContainer').innerHTML = '';

    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('genreModal'));
    modal.show();

    // First try to load groups from database (fast)
    try {
        const dbResponse = await fetch('/api/portal/groups', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ portal_id: portalId })
        });

        const dbData = await dbResponse.json();

        if (dbData.success && dbData.groups && dbData.groups.length > 0) {
            // Groups found in database - use active flag for selection
            document.getElementById('genreModalLoading').style.display = 'none';
            genreModalGenres = dbData.groups.map(g => ({
                id: g.id,
                title: g.title,
                channel_count: g.channel_count
            }));
            if (!genreModalGenres.find(g => String(g.id) === 'UNGROUPED')) {
                genreModalGenres.unshift({ id: 'UNGROUPED', title: 'Ungrouped', channel_count: 0 });
            }
            const activeGenreIds = dbData.groups.filter(g => g.active).map(g => g.id);
            renderGenreModalTiles(genreModalGenres, activeGenreIds);
            updateGenreModalStats();
            document.getElementById('genreModalContent').style.display = 'block';
            return;
        }
    } catch (dbError) {
        console.log('Could not load groups from DB, falling back to portal API:', dbError);
    }

    // Fallback: Fetch genres from portal API (for new portals without channels yet)
    try {
        const firstMac = Object.keys(portal.macs)[0];
        if (!firstMac) {
            document.getElementById('genreModalLoading').style.display = 'none';
            document.getElementById('genreModalErrorMessage').textContent = 'Keine MACs für dieses Portal vorhanden.';
            document.getElementById('genreModalError').style.display = 'block';
            return;
        }
        currentGenreModalMac = firstMac;
        const response = await fetch('/api/portal/genres/list', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                url: portal.url,
                mac: firstMac,
                proxy: portal.proxy || ''
            })
        });

        const data = await response.json();
        document.getElementById('genreModalLoading').style.display = 'none';

        if (data.success && data.genres && data.genres.length > 0) {
            genreModalGenres = data.genres;
            if (!genreModalGenres.find(g => String(g.id) === 'UNGROUPED')) {
                genreModalGenres.unshift({ id: 'UNGROUPED', title: 'Ungrouped', channel_count: 0 });
            }
            renderGenreModalTiles(data.genres, portal.selected_genres || []);
            updateGenreModalStats();
            document.getElementById('genreModalContent').style.display = 'block';
        } else {
            document.getElementById('genreModalErrorMessage').textContent =
                data.message || 'No groups available from this portal';
            document.getElementById('genreModalError').style.display = 'block';
        }
    } catch (error) {
        document.getElementById('genreModalLoading').style.display = 'none';
        document.getElementById('genreModalErrorMessage').textContent = 'Error fetching groups: ' + error;
        document.getElementById('genreModalError').style.display = 'block';
    }
}

function renderGenreModalTiles(genres, selectedGenres) {
    const container = document.getElementById('genreModalTilesContainer');
    const totalChannels = genres.reduce((sum, g) => sum + (g.channel_count || 0), 0);

    const ensured = [...genres];
    if (!ensured.find(g => String(g.id) === 'UNGROUPED')) {
        ensured.unshift({ id: 'UNGROUPED', title: 'Ungrouped', channel_count: 0 });
    }
    container.innerHTML = ensured.map(g => {
        const isSelected = selectedGenres.length === 0 || selectedGenres.includes(g.id);
        return `
            <div class="genre-tile ${isSelected ? 'selected' : ''}"
                 data-genre-id="${g.id}" data-genre-title="${g.title}"
                 onclick="toggleGenreTile(this); updateGenreModalStats();">
                <div class="genre-tile-title">${g.title}</div>
                <div class="genre-tile-count">${g.channel_count || 0}</div>
                <div class="genre-tile-label">Channels</div>
            </div>
        `;
    }).join('');
}

function updateGenreModalStats() {
    const selectedTiles = document.querySelectorAll('#genreModalTilesContainer .genre-tile.selected');
    const totalTiles = document.querySelectorAll('#genreModalTilesContainer .genre-tile');

    let selectedChannels = 0;
    selectedTiles.forEach(tile => {
        const genreId = tile.dataset.genreId;
        const genre = genreModalGenres.find(g => g.id === genreId);
        if (genre) selectedChannels += (genre.channel_count || 0);
    });

    const totalChannels = genreModalGenres.reduce((sum, g) => sum + (g.channel_count || 0), 0);

    document.getElementById('genreModalStats').innerHTML =
        `<strong>${selectedTiles.length}</strong> / ${totalTiles.length} groups selected · ` +
        `<strong>${selectedChannels}</strong> / ${totalChannels} channels`;
}

function selectAllModalGenres() {
    document.querySelectorAll('#genreModalTilesContainer .genre-tile:not([style*="display: none"])').forEach(tile => {
        tile.classList.add('selected');
    });
    updateGenreModalStats();
}

function deselectAllModalGenres() {
    document.querySelectorAll('#genreModalTilesContainer .genre-tile').forEach(tile => {
        tile.classList.remove('selected');
    });
    updateGenreModalStats();
}

function applyDefaultGroupSelection() {
    const patternsRaw = (portalsData.__settings__ && portalsData.__settings__["auto group selection patterns"]) || "";
    const patterns = patternsRaw.split(/\r?\n/).map(p => p.trim()).filter(Boolean);
    if (!patterns.length) {
        showToast('Keine Standard-Patterns gesetzt (Settings).', 'warning');
        return;
    }
    document.querySelectorAll('#genreModalTilesContainer .genre-tile').forEach(tile => {
        const title = tile.dataset.genreTitle || '';
        let matched = false;
        for (const pattern of patterns) {
            try {
                if (new RegExp(pattern, 'i').test(title)) {
                    matched = true;
                    break;
                }
            } catch (e) {
                if (title.toLowerCase().includes(pattern.toLowerCase())) {
                    matched = true;
                    break;
                }
            }
        }
        tile.classList.toggle('selected', matched);
    });
    updateGenreModalStats();
}

function filterModalGenres() {
    const searchTerm = document.getElementById('genreModalSearchInput').value.toLowerCase();
    document.querySelectorAll('#genreModalTilesContainer .genre-tile').forEach(tile => {
        const title = tile.dataset.genreTitle;
        tile.style.display = title.includes(searchTerm) ? '' : 'none';
    });
}

async function savePortalGenres() {
    const allTiles = document.querySelectorAll('#genreModalTilesContainer .genre-tile');
    const selectedGenres = Array.from(document.querySelectorAll('#genreModalTilesContainer .genre-tile.selected'))
        .map(tile => tile.dataset.genreId);

    // Calculate total channels from all genres
    const totalChannels = genreModalGenres.reduce((sum, g) => sum + (g.channel_count || 0), 0);

    const btn = document.getElementById('saveGenresBtn');
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Saving & Refreshing...';

    try {
        const response = await fetch('/api/portal/genres', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                portal_id: currentGenreModalPortalId,
                selected_genres: selectedGenres,
                total_groups: allTiles.length,
                total_channels: totalChannels,
                genres_mac: currentGenreModalMac  // MAC used for genre selection
            })
        });

        const data = await response.json();

        if (data.success) {
            // Close modal
            bootstrap.Modal.getInstance(document.getElementById('genreModal')).hide();

            // Update stats dynamically without page reload
            updatePortalStats(currentGenreModalPortalId, {
                total_channels: data.total_channels,
                channels: data.active_channels,
                total_groups: data.total_groups,
                groups: data.active_groups
            });

            showToast(`Groups updated: ${data.active_groups}/${data.total_groups} groups, ${data.active_channels}/${data.total_channels} channels`, 'success');
            if (data.match_started) {
                pollMatchStatus(currentGenreModalPortalId);
            }
            if (data.refresh_status === 'queued') {
                showToast('Channel refresh queued.', 'info');
                pollRefreshStatus(currentGenreModalPortalId);
            } else if (data.refresh_status === 'running') {
                showToast('Channel refresh already running.', 'warning');
                pollRefreshStatus(currentGenreModalPortalId);
            }
        } else {
            showToast('Error saving groups: ' + (data.message || 'Unknown error'), 'error');
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-save"></i> Save';
        }
    } catch (error) {
        showToast('Error saving groups: ' + error, 'error');
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-save"></i> Save';
    }
}

function pollMatchStatus(portalId) {
    const startedAt = Date.now();
    const poll = () => {
        fetch('/api/portal/match/status', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ portal_id: portalId })
        })
            .then(res => res.json())
            .then(data => {
                if (!data.success) return;
                if (data.status === 'completed') {
                    showToast(`Matching completed: ${data.matched} channels`, 'success');
                    return;
                }
                if (data.status === 'error') {
                    showToast(`Matching failed: ${data.error || 'Unknown error'}`, 'error');
                    return;
                }
                if (Date.now() - startedAt < 300000) {
                    setTimeout(poll, 2000);
                }
            })
            .catch(() => {
                if (Date.now() - startedAt < 300000) {
                    setTimeout(poll, 3000);
                }
            });
    };
    setTimeout(poll, 1500);
}

// Reset genre modal when closed
document.getElementById('genreModal')?.addEventListener('hidden.bs.modal', function() {
    currentGenreModalPortalId = null;
    currentGenreModalMac = null;
    genreModalGenres = [];
    document.getElementById('genreModalTilesContainer').innerHTML = '';
    document.getElementById('genreModalSearchInput').value = '';

    // Reset save button state
    const btn = document.getElementById('saveGenresBtn');
    if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-save"></i> Save & Refresh';
    }
});

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    // Load saved view preference
    const savedView = localStorage.getItem('portalsView') || 'card';
    setView(savedView);

    // Sort MACs by expiration date (earliest first)
    sortMacTablesByExpiry();

    // Highlight expiring MACs
    highlightExpiringMacs();

    // Format watchdog badges
    formatWatchdogBadges();

    // Update portal expiry indicators in list view
    updatePortalExpiryIndicators();
});

        // expose functions used by inline handlers
        window.setView = setView;
        window.togglePortal = togglePortal;
        window.editPortal = editPortal;
        window.refreshPortalChannels = refreshPortalChannels;
        window.deletePortal = deletePortal;
        window.refreshPortalMacs = refreshPortalMacs;
        window.deleteMac = deleteMac;
        window.confirmMacDelete = confirmMacDelete;
        window.openGenreModal = openGenreModal;
        window.savePortalGenres = savePortalGenres;
        window.selectAllModalGenres = selectAllModalGenres;
        window.deselectAllModalGenres = deselectAllModalGenres;
        window.applyDefaultGroupSelection = applyDefaultGroupSelection;
        window.toggleGenreTile = toggleGenreTile;
        window.updateGenreModalStats = updateGenreModalStats;

    }
    window.App && window.App.register('portals', initPortalsPage);
})();
