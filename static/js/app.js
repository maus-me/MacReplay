// Global page init registry for HTMX + full page loads
(function() {
    const registry = {};
    let lastPage = null;

    function getPageMeta() {
        return document.getElementById('page-meta');
    }

    function getPageName() {
        const meta = getPageMeta();
        return meta ? meta.dataset.page : null;
    }

    function getPageData() {
        const node = document.getElementById('page-data');
        if (!node) return {};
        try {
            return JSON.parse(node.textContent || '{}');
        } catch (e) {
            return {};
        }
    }

    function runInit(options = {}) {
        const page = getPageName();
        if (!page || !registry[page]) return;
        if (lastPage && registry[lastPage] && typeof registry[lastPage].cleanup === 'function') {
            try {
                registry[lastPage].cleanup();
            } catch (e) {
                // ignore cleanup errors
            }
        }
        lastPage = page;
        const data = getPageData();
        const originalDocAdd = document.addEventListener.bind(document);
        const originalWinAdd = window.addEventListener.bind(window);
        document.addEventListener = function(type, listener, options) {
            if (type === 'DOMContentLoaded') {
                setTimeout(listener, 0);
                return;
            }
            return originalDocAdd(type, listener, options);
        };
        window.addEventListener = function(type, listener, options) {
            if (type === 'load') {
                setTimeout(listener, 0);
                return;
            }
            return originalWinAdd(type, listener, options);
        };
        try {
            registry[page].init(data);
        } catch (e) {
            // surface errors in console for debugging
            console.error('Page init failed for', page, e);
        } finally {
            document.addEventListener = originalDocAdd;
            window.addEventListener = originalWinAdd;
        }

        if (!options.preserveScroll) {
            window.scrollTo(0, 0);
        }
    }

    window.App = {
        register: function(name, initFn, cleanupFn) {
            registry[name] = { init: initFn, cleanup: cleanupFn };
        },
        run: runInit,
        getPageData: getPageData
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', runInit, { once: true });
    } else {
        runInit();
    }

    document.body.addEventListener('htmx:afterSettle', function(evt) {
        if (evt && evt.target && evt.target.id === 'app-content') {
            runInit({ preserveScroll: true });
            if (evt.target.querySelector('.settings-sidebar')) {
                window.scrollTo(0, 0);
            }
        }
    });
})();
