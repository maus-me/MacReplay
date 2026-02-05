(function() {
    let streamsRefreshInterval;

    function initDashboardPage(pageData) {
        pageData = pageData || {};

// Toast Notification Functions
function showNotification(message, type = 'success', duration = 3000) {
    const toastType = type === 'danger' ? 'error' : type;
    if (typeof showToast === 'function') {
        showToast(message, toastType, duration);
    }
}

function refreshStreams() {
    const container = document.getElementById('streamsContainer');
    if (!container) return;
    fetch('/streaming')
        .then(response => response.json())
        .then(data => {
            displayStreams(data);
        })
        .catch(error => {
            console.error('Error fetching streams:', error);
            if (container) {
                container.innerHTML =
                    '<div class="alert alert-danger"><i class="fas fa-exclamation-triangle"></i> Error loading stream data</div>';
            }
        });
}

function displayStreams(streams) {
    const container = document.getElementById('streamsContainer');
    if (!container) return;

    if (!streams || Object.keys(streams).length === 0) {
        container.innerHTML = '<div class="alert alert-info"><i class="fas fa-info-circle"></i> No active streams</div>';
        return;
    }

    let html = '<div class="table-responsive"><table class="table table-striped"><thead><tr><th>Portal</th><th>Channel</th><th>MAC</th><th>Client IP</th><th>Start Time</th><th>Duration</th></tr></thead><tbody>';

    Object.keys(streams).forEach(portalId => {
        streams[portalId].forEach(stream => {
            const startTime = new Date(stream['start time'] * 1000);
            const duration = Math.floor((Date.now() - startTime.getTime()) / 1000);
            const durationStr = formatDuration(duration);
            // Escape HTML to prevent XSS
            const portalName = escapeHtml(stream['portal name']);
            const channelName = escapeHtml(stream['channel name']);
            const mac = escapeHtml(stream.mac);
            const client = escapeHtml(stream.client);

            html += `
                <tr>
                    <td>${portalName}</td>
                    <td>${channelName}</td>
                    <td><code>${mac}</code></td>
                    <td>${client}</td>
                    <td>${startTime.toLocaleString()}</td>
                    <td>${durationStr}</td>
                </tr>
            `;
        });
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatDuration(seconds) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;

    if (hours > 0) {
        return `${hours}h ${minutes}m ${secs}s`;
    } else if (minutes > 0) {
        return `${minutes}m ${secs}s`;
    } else {
        return `${secs}s`;
    }
}

function refreshLineup() {
    fetch('/refresh_lineup', { method: 'POST' })
        .then(response => response.json())
        .then(data => {
            showNotification('Lineup refreshed successfully!', 'success');
        })
        .catch(error => {
            console.error('Error refreshing lineup:', error);
            showNotification('Error refreshing lineup', 'error');
        });
}

function updatePlaylist() {
    fetch('/update_playlistm3u', { method: 'POST' })
        .then(response => response.text())
        .then(data => {
            showNotification('Playlist updated successfully!', 'success');
        })
        .catch(error => {
            console.error('Error updating playlist:', error);
            showNotification('Error updating playlist', 'error');
        });
}

function copyToClipboard(elementId) {
    const element = document.getElementById(elementId);
    const text = element.value;

    // Use modern Clipboard API
    navigator.clipboard.writeText(text).then(() => {
        // Visual feedback
        const button = element.nextElementSibling;
        const originalHtml = button.innerHTML;
        button.innerHTML = '<i class="fas fa-check"></i>';
        button.classList.add('btn-success');
        button.classList.remove('btn-outline-secondary');

        setTimeout(() => {
            button.innerHTML = originalHtml;
            button.classList.remove('btn-success');
            button.classList.add('btn-outline-secondary');
        }, 2000);

        showNotification('Copied to clipboard!', 'success', 1500);
    }).catch(err => {
        console.error('Failed to copy:', err);
        showNotification('Failed to copy to clipboard', 'error');
    });
}

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
    const baseUrl = window.location.origin;
    const serverUrlEl = document.getElementById('serverUrl');
    const xmltvUrlEl = document.getElementById('xmltvUrl');
    const playlistUrlEl = document.getElementById('playlistUrl');
    const lastUpdatedEl = document.getElementById('lastUpdated');

    if (serverUrlEl) serverUrlEl.textContent = baseUrl;
    if (xmltvUrlEl) xmltvUrlEl.value = `${baseUrl}/xmltv`;
    if (playlistUrlEl) playlistUrlEl.value = `${baseUrl}/playlist.m3u`;
    if (lastUpdatedEl) lastUpdatedEl.textContent = new Date().toLocaleString();

    // Initial load
    refreshStreams();

    // Auto-refresh streams every 30 seconds
    streamsRefreshInterval = setInterval(refreshStreams, 30000);
});

        window.refreshStreams = refreshStreams;
        window.copyToClipboard = copyToClipboard;
        window.refreshLineup = refreshLineup;
        window.updatePlaylist = updatePlaylist;

    }
    function cleanup() {
        if (streamsRefreshInterval) {
            clearInterval(streamsRefreshInterval);
            streamsRefreshInterval = null;
        }
    }
    window.App && window.App.register('dashboard', initDashboardPage, cleanup);
})();
