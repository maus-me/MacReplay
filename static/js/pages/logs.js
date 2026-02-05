(function() {
    let refreshInterval;

    function initLogsPage(pageData) {
        pageData = pageData || {};

let isPaused = false;
let autoScroll = true;
let allLogLines = [];
const REFRESH_RATE = 2000; // 2 seconds

function fetchLogs() {
    if (isPaused) return;

    const lineCountEl = document.getElementById('lineCount');
    if (!lineCountEl) return;
    const lineCount = lineCountEl.value;
    const url = lineCount === 'all' ? '/logs/stream' : `/logs/stream?lines=${lineCount}`;

    fetch(url)
        .then(response => response.json())
        .then(data => {
            allLogLines = data.lines || [];
            filterLogs();
            const lastUpdateEl = document.getElementById('lastUpdate');
            if (lastUpdateEl) {
                lastUpdateEl.textContent = new Date().toLocaleTimeString();
            }
        })
        .catch(error => {
            console.error('Error fetching logs:', error);
            const statusIndicator = document.getElementById('statusIndicator');
            const statusText = document.getElementById('statusText');
            if (statusIndicator) statusIndicator.className = 'text-danger';
            if (statusText) statusText.textContent = 'Error';
        });
}

function filterLogs() {
    const filterEl = document.getElementById('logLevelFilter');
    if (!filterEl) return;
    const filter = filterEl.value;
    const container = document.getElementById('logContainer');
    if (!container) return;

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
    if (!container) return;
    const wasAtBottom = isScrolledToBottom();

    if (lines.length === 0) {
        container.innerHTML = '<div class="text-center text-muted py-5">No log entries found</div>';
        const lineCountDisplay = document.getElementById('lineCountDisplay');
        if (lineCountDisplay) lineCountDisplay.textContent = '0';
        return;
    }

    let html = '';
    lines.forEach(line => {
        const formattedLine = formatLogLine(line);
        html += `<div class="log-line">${formattedLine}</div>`;
    });

    container.innerHTML = html;
    const lineCountDisplay = document.getElementById('lineCountDisplay');
    if (lineCountDisplay) lineCountDisplay.textContent = lines.length;

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
    if (!btn || !icon || !statusIndicator || !statusText) return;

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
    const container = document.getElementById('logContainer');
    const lineCountDisplay = document.getElementById('lineCountDisplay');
    if (container) container.innerHTML = '<div class="text-center text-muted py-5">Display cleared</div>';
    if (lineCountDisplay) lineCountDisplay.textContent = '0';
}

function scrollToBottom() {
    const container = document.getElementById('logContainer');
    if (!container) return;
    container.scrollTop = container.scrollHeight;
}

function isScrolledToBottom() {
    const container = document.getElementById('logContainer');
    if (!container) return true;
    return container.scrollHeight - container.scrollTop <= container.clientHeight + 50;
}

function changeLineCount() {
    fetchLogs();
}

// Track scroll position to disable auto-scroll when user scrolls up
const logContainer = document.getElementById('logContainer');
if (logContainer) {
    logContainer.addEventListener('scroll', function() {
        autoScroll = isScrolledToBottom();
    });
}

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    fetchLogs();
    refreshInterval = setInterval(fetchLogs, REFRESH_RATE);
});

// Cleanup on page unload
        window.togglePause = togglePause;
        window.clearDisplay = clearDisplay;
        window.scrollToBottom = scrollToBottom;
        window.filterLogs = filterLogs;
        window.changeLineCount = changeLineCount;

    }
    function cleanup() {
        if (refreshInterval) {
            clearInterval(refreshInterval);
            refreshInterval = null;
        }
    }
    window.App && window.App.register('logs', initLogsPage, cleanup);
})();
