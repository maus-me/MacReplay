(function() {
    function initLogsPage(pageData) {
        pageData = pageData || {};

let isPaused = false;
let autoScroll = true;
let refreshInterval;
let allLogLines = [];
const REFRESH_RATE = 2000; // 2 seconds

function fetchLogs() {
    if (isPaused) return;

    const lineCount = document.getElementById('lineCount').value;
    const url = lineCount === 'all' ? '/logs/stream' : `/logs/stream?lines=${lineCount}`;

    fetch(url)
        .then(response => response.json())
        .then(data => {
            allLogLines = data.lines || [];
            filterLogs();
            document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();
        })
        .catch(error => {
            console.error('Error fetching logs:', error);
            document.getElementById('statusIndicator').className = 'text-danger';
            document.getElementById('statusText').textContent = 'Error';
        });
}

function filterLogs() {
    const filter = document.getElementById('logLevelFilter').value;
    const container = document.getElementById('logContainer');

    let filteredLines = allLogLines;

    if (filter) {
        const levels = {
            'ERROR': ['ERROR'],
            'WARNING': ['ERROR', 'WARNING'],
            'INFO': ['ERROR', 'WARNING', 'INFO'],
            'DEBUG': ['ERROR', 'WARNING', 'INFO', 'DEBUG']
        };
        const allowedLevels = levels[filter] || [];
        filteredLines = allLogLines.filter(line => {
            return allowedLevels.some(level => line.includes(`[${level}]`));
        });
    }

    renderLogs(filteredLines);
}

function renderLogs(lines) {
    const container = document.getElementById('logContainer');
    const wasAtBottom = isScrolledToBottom();

    if (lines.length === 0) {
        container.innerHTML = '<div class="text-center text-muted py-5">No log entries found</div>';
        document.getElementById('lineCountDisplay').textContent = '0';
        return;
    }

    let html = '';
    lines.forEach(line => {
        const formattedLine = formatLogLine(line);
        html += `<div class="log-line">${formattedLine}</div>`;
    });

    container.innerHTML = html;
    document.getElementById('lineCountDisplay').textContent = lines.length;

    if (autoScroll && wasAtBottom) {
        scrollToBottom();
    }
}

function formatLogLine(line) {
    // Highlight log levels
    let formatted = line
        .replace(/\[INFO\]/g, '<span class="log-level-INFO">[INFO]</span>')
        .replace(/\[WARNING\]/g, '<span class="log-level-WARNING">[WARNING]</span>')
        .replace(/\[ERROR\]/g, '<span class="log-level-ERROR">[ERROR]</span>')
        .replace(/\[DEBUG\]/g, '<span class="log-level-DEBUG">[DEBUG]</span>');

    // Highlight timestamp
    formatted = formatted.replace(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})/, '<span class="log-timestamp">$1</span>');

    return formatted;
}

function togglePause() {
    isPaused = !isPaused;
    const btn = document.getElementById('btnPlay');
    const icon = document.getElementById('playIcon');
    const statusIndicator = document.getElementById('statusIndicator');
    const statusText = document.getElementById('statusText');

    if (isPaused) {
        btn.classList.remove('btn-success');
        btn.classList.add('btn-warning');
        icon.classList.remove('fa-pause');
        icon.classList.add('fa-play');
        statusIndicator.className = 'text-warning';
        statusText.textContent = 'Paused';
    } else {
        btn.classList.remove('btn-warning');
        btn.classList.add('btn-success');
        icon.classList.remove('fa-play');
        icon.classList.add('fa-pause');
        statusIndicator.className = 'text-success';
        statusText.textContent = 'Live';
        fetchLogs(); // Fetch immediately when resuming
    }
}

function clearDisplay() {
    document.getElementById('logContainer').innerHTML = '<div class="text-center text-muted py-5">Display cleared</div>';
    document.getElementById('lineCountDisplay').textContent = '0';
}

function scrollToBottom() {
    const container = document.getElementById('logContainer');
    container.scrollTop = container.scrollHeight;
}

function isScrolledToBottom() {
    const container = document.getElementById('logContainer');
    return container.scrollHeight - container.scrollTop <= container.clientHeight + 50;
}

function changeLineCount() {
    fetchLogs();
}

// Track scroll position to disable auto-scroll when user scrolls up
document.getElementById('logContainer').addEventListener('scroll', function() {
    autoScroll = isScrolledToBottom();
});

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    fetchLogs();
    refreshInterval = setInterval(fetchLogs, REFRESH_RATE);
});

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
});

        window.togglePause = togglePause;
        window.clearDisplay = clearDisplay;
        window.scrollToBottom = scrollToBottom;
        window.filterLogs = filterLogs;
        window.changeLineCount = changeLineCount;

    }
    window.App && window.App.register('logs', initLogsPage);
})();
