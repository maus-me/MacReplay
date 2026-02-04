(function() {
    function initEpgPage(pageData) {
        pageData = pageData || {};

let epgData = { channels: [], programmes: [] };
let selectedChannel = null;

function normalizeImageUrl(url) {
    if (!url) return '';
    if (url.startsWith('data:')) return url;
    if (url.startsWith('http://') || url.startsWith('https://')) {
        return `/api/image-proxy?url=${encodeURIComponent(url)}`;
    }
    return url;
}

function loadEPG() {
    fetch('/api/epg')
        .then(response => response.json())
        .then(data => {
            epgData = data;
            renderChannelList();
            updateStats();

            // If a channel was selected, refresh its programmes
            if (selectedChannel) {
                showProgrammes(selectedChannel);
            }
        })
        .catch(error => {
            console.error('Error loading EPG:', error);
            document.getElementById('channelList').innerHTML =
                '<div class="text-center py-5 text-danger"><i class="fas fa-exclamation-triangle fa-2x"></i><p class="mt-2">Error loading EPG data</p></div>';
        });
}

function renderChannelList(filter = '') {
    const container = document.getElementById('channelList');
    const channels = epgData.channels || [];

    if (channels.length === 0) {
        container.innerHTML = '<div class="no-programmes"><i class="fas fa-tv fa-2x mb-2"></i><p>No channels available</p><p class="small">EPG data may still be loading</p></div>';
        return;
    }

    const filteredChannels = filter
        ? channels.filter(ch => ch.name.toLowerCase().includes(filter.toLowerCase()))
        : channels;

    let html = '';
    filteredChannels.forEach(channel => {
        const isActive = selectedChannel === channel.id ? 'active' : '';
        const currentProgramme = getCurrentProgramme(channel.id);
        const placeholderSvg = "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"36\" height=\"36\" viewBox=\"0 0 36 36\"><rect width=\"36\" height=\"36\" rx=\"6\" fill=\"#252b33\"/><path d=\"M11 12h14a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H11a2 2 0 0 1-2-2v-9a2 2 0 0 1 2-2zm2 14h10\" fill=\"none\" stroke=\"#8b99a6\" stroke-width=\"2\" stroke-linecap=\"round\"/></svg>";
        const placeholderLogo = `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(placeholderSvg)}`;
        const logoUrl = normalizeImageUrl(channel.logo) || placeholderLogo;
        const logo = `<img src="${logoUrl}" class="channel-logo" onerror="this.onerror=null; this.src='${placeholderLogo}';">`;

        const portalInfo = channel.portal ? `<small class="text-muted d-block" style="font-size: 0.7em; opacity: 0.7;">${escapeHtml(channel.portal)}</small>` : '';
        const sourceInfo = channel.source_name ? `<small class="text-muted d-block" style="font-size: 0.7em; opacity: 0.7;">Quelle: ${escapeHtml(channel.source_name)}</small>` : '';

        const numberInfo = channel.number ? `<span class="badge bg-secondary me-2">${escapeHtml(channel.number)}</span>` : '';

        html += `
            <div class="channel-item ${isActive}" data-channel-id="${escapeHtml(channel.id)}" data-channel-name="${escapeHtml(channel.name)}">
                <div class="d-flex align-items-center">
                    ${logo}
                    <div class="flex-grow-1">
                        <div class="fw-bold">${numberInfo}${escapeHtml(channel.name)}</div>
                        ${portalInfo}
                        ${sourceInfo}
                        ${currentProgramme ? `<small class="text-muted">${escapeHtml(currentProgramme.title)}</small>` : ''}
                    </div>
                </div>
            </div>
        `;
    });

    container.innerHTML = html || '<div class="no-programmes">No channels match your search</div>';
    container.querySelectorAll('.channel-item').forEach(item => {
        item.addEventListener('click', () => selectChannel(item));
    });
}

function getCurrentProgramme(channelId) {
    const now = new Date();
    // Find all current programmes for this channel
    const currentProgs = epgData.programmes.filter(p => {
        const start = new Date(p.start);
        const stop = new Date(p.stop);
        const duration = stop - start;
        return p.channel === channelId &&
               start <= now &&
               stop > now &&
               duration > 0;
    });

    if (currentProgs.length === 0) return null;

    // Return the one with shortest duration (most specific)
    return currentProgs.reduce((best, prog) => {
        const bestDuration = new Date(best.stop) - new Date(best.start);
        const progDuration = new Date(prog.stop) - new Date(prog.start);
        return progDuration < bestDuration ? prog : best;
    });
}

function selectChannel(element) {
    if (!element) return;
    const channelId = element.dataset.channelId;
    const channelName = element.dataset.channelName;
    selectedChannel = channelId;
    document.getElementById('selectedChannelName').textContent = channelName;

    // Update active state in channel list
    document.querySelectorAll('.channel-item').forEach(item => {
        item.classList.remove('active');
    });
    element.classList.add('active');

    showProgrammes(channelId);
}

function showProgrammes(channelId) {
    const container = document.getElementById('programmeList');
    const programmes = epgData.programmes.filter(p => p.channel === channelId);

    if (programmes.length === 0) {
        container.innerHTML = '<div class="no-programmes"><i class="fas fa-calendar-times fa-2x mb-2"></i><p>No program data available for this channel</p></div>';
        return;
    }

    const now = new Date();
    let html = '';
    let lastDateStr = '';

    // Find the BEST current programme (shortest duration that contains now)
    // This handles overlapping programmes from bad EPG data
    let bestCurrentProg = null;
    let bestCurrentDuration = Infinity;

    programmes.forEach(prog => {
        const start = new Date(prog.start);
        const stop = new Date(prog.stop);
        const duration = stop - start;

        if (start <= now && stop > now && duration > 0) {
            // This programme is currently running
            // Prefer the one with shortest duration (more specific)
            if (duration < bestCurrentDuration) {
                bestCurrentDuration = duration;
                bestCurrentProg = prog;
            }
        }
    });

    programmes.forEach(prog => {
        const start = new Date(prog.start);
        const stop = new Date(prog.stop);
        const duration = stop - start;
        const isPast = stop <= now;
        const isCurrent = start <= now && stop > now && duration > 0;
        const isFuture = start > now;

        // Only mark as "NOW" if this is the best current programme
        const isTheBestCurrent = bestCurrentProg && prog.start === bestCurrentProg.start && prog.stop === bestCurrentProg.stop;

        // Check if date changed - add date header
        const dateStr = formatDate(start);
        if (dateStr !== lastDateStr) {
            html += `<div class="programme-date-header">${dateStr}</div>`;
            lastDateStr = dateStr;
        }

        let statusClass = '';
        let nowBadge = '';
        let progressBar = '';

        if (isTheBestCurrent) {
            statusClass = 'current';
            nowBadge = '<span class="now-indicator">NOW</span>';

            // Calculate progress
            const total = stop - start;
            const elapsed = now - start;
            const progress = Math.min(100, Math.max(0, (elapsed / total) * 100));
            progressBar = `<div class="progress-bar-wrapper"><div class="progress-bar-fill" style="width: ${progress}%"></div></div>`;
        } else if (isPast) {
            statusClass = 'past';
        } else if (isFuture) {
            statusClass = 'future';
        } else if (isCurrent && !isTheBestCurrent) {
            // Overlapping current programme but not the best one
            statusClass = 'past';
        }

        const timeStr = `${formatTime(start)} - ${formatTime(stop)}`;
        const description = prog.description && prog.description !== prog.title
            ? `<div class="programme-desc">${escapeHtml(prog.description)}</div>`
            : '';
        const subTitle = prog.sub_title
            ? `<div class="programme-subtitle">${escapeHtml(prog.sub_title)}</div>`
            : '';
        const categories = Array.isArray(prog.categories) ? prog.categories : [];
        const categoryTags = categories.length
            ? `<div class="programme-tags">${categories.map(cat => `<span class="badge bg-secondary me-1">${escapeHtml(cat)}</span>`).join('')}</div>`
            : '';
        const episodeInfo = prog.episode_num
            ? `<div class="programme-episode text-muted">Episode: ${escapeHtml(prog.episode_num)}</div>`
            : '';
        const programmeIconUrl = normalizeImageUrl(prog.programme_icon);
        const programmeIcon = programmeIconUrl
            ? `<img src="${programmeIconUrl}" class="programme-icon" style="width:24px;height:24px;border-radius:4px;object-fit:cover;" onerror="this.style.display='none'">`
            : '';

        html += `
            <div class="programme-item ${statusClass}">
                <div class="programme-time">${timeStr}${nowBadge}</div>
                <div class="programme-title d-flex align-items-center justify-content-between">
                    <span>${escapeHtml(prog.title)}</span>
                    ${programmeIcon}
                </div>
                ${subTitle}
                ${episodeInfo}
                ${categoryTags}
                ${description}
                ${progressBar}
            </div>
        `;
    });

    container.innerHTML = html;

    // Scroll to current programme
    const currentProg = container.querySelector('.programme-item.current');
    if (currentProg) {
        currentProg.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
}

function formatDate(date) {
    const today = new Date();
    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    if (date.toDateString() === today.toDateString()) {
        return 'Heute';
    } else if (date.toDateString() === tomorrow.toDateString()) {
        return 'Morgen';
    } else if (date.toDateString() === yesterday.toDateString()) {
        return 'Gestern';
    } else {
        return date.toLocaleDateString('de-DE', { weekday: 'long', day: '2-digit', month: '2-digit' });
    }
}

function formatTime(date) {
    return date.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function updateStats() {
    document.getElementById('channelCount').textContent = epgData.channels?.length || 0;
    document.getElementById('programmeCount').textContent = epgData.programmes?.length || 0;

    if (epgData.last_updated) {
        const lastUpdate = new Date(epgData.last_updated * 1000);
        document.getElementById('epgLastUpdate').textContent = lastUpdate.toLocaleString('de-DE');
    }

    // Debug info from API
    if (epgData.debug) {
        document.getElementById('currentCount').textContent = epgData.debug.current_programme_count || 0;
        document.getElementById('serverTz').textContent = epgData.debug.container_tz || '-';

        // Show EPG time range
        if (epgData.debug.earliest_programme && epgData.debug.latest_programme) {
            const earliest = new Date(epgData.debug.earliest_programme);
            const latest = new Date(epgData.debug.latest_programme);
            document.getElementById('epgRange').textContent =
                earliest.toLocaleString('de-DE', {day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit'}) +
                ' - ' +
                latest.toLocaleString('de-DE', {day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit'});
        }
    }

    document.getElementById('lastUpdated').textContent = 'Updated: ' + new Date().toLocaleTimeString('de-DE');
}

// Search functionality
document.getElementById('channelSearch').addEventListener('input', function(e) {
    renderChannelList(e.target.value);
});

// Poll EPG refresh status
async function pollEpgStatus(toast) {
    const maxAttempts = 120; // Max 2 minutes (120 * 1 second)
    let attempts = 0;

    while (attempts < maxAttempts) {
        try {
            const response = await fetch('/api/epg/status');
            const data = await response.json();

            if (!data.is_refreshing) {
                // Refresh complete
                hideToast(toast);
                showToast('EPG refresh complete!', 'success', 3000);

                // Reload EPG data
                loadEPG();

                const btn = document.getElementById('refreshEpgBtn');
                btn.classList.remove('refreshing');
                btn.disabled = false;
                return true;
            }
        } catch (e) {
            console.error('Error polling EPG status:', e);
        }

        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
    }

    // Timeout
    hideToast(toast);
    showToast('EPG refresh timed out', 'error', 5000);
    const btn = document.getElementById('refreshEpgBtn');
    btn.classList.remove('refreshing');
    btn.disabled = false;
    return false;
}

// Refresh EPG data
function refreshEPG() {
    const btn = document.getElementById('refreshEpgBtn');
    btn.classList.add('refreshing');
    btn.disabled = true;

    const filterValue = document.getElementById('channelSearch')?.value || '';
    const channelIds = (epgData.channels || [])
        .filter(ch => !filterValue || ch.name.toLowerCase().includes(filterValue.toLowerCase()))
        .map(ch => ch.id);

    fetch('/api/epg/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ epg_ids: channelIds })
    })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'started' || data.status === 'success' || data.status === 'already_running') {
                // Show loading toast and start polling
                const toast = showToast('EPG wird aktualisiert...', 'info', 0);
                pollEpgStatus(toast);
            } else {
                showToast('Error: ' + (data.message || 'Unknown error'), 'error', 5000);
                btn.classList.remove('refreshing');
                btn.disabled = false;
            }
        })
        .catch(error => {
            console.error('Error refreshing EPG:', error);
            showToast('Error refreshing EPG', 'error', 5000);
            btn.classList.remove('refreshing');
            btn.disabled = false;
        });
}

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    loadEPG();

    // Refresh EPG data every 5 minutes
    setInterval(loadEPG, 5 * 60 * 1000);

    // Update current programme progress every 30 seconds
    setInterval(() => {
        if (selectedChannel) {
            showProgrammes(selectedChannel);
        }
    }, 30000);
});

        window.refreshEPG = refreshEPG;

    }
    window.App && window.App.register('epg', initEpgPage);
})();
