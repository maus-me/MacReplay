// Global page init registry for HTMX + full page loads
(function() {
    const registry = {};
    let lastPage = null;
    function cleanupModalArtifacts() {
        document.querySelectorAll('.modal.show').forEach(modalEl => {
            modalEl.classList.remove('show');
            modalEl.setAttribute('aria-hidden', 'true');
            modalEl.style.display = 'none';
        });
        document.querySelectorAll('.modal-backdrop').forEach(backdrop => backdrop.remove());
        document.body.classList.remove('modal-open');
        document.body.style.removeProperty('padding-right');
        document.body.style.removeProperty('overflow');
    }

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

    function isAppContentTarget(target) {
        if (!target) return false;
        if (target.id === 'app-content') return true;
        if (typeof target.closest === 'function' && target.closest('#app-content')) return true;
        return false;
    }

    function handleHtmxInit(evt) {
        const target = (evt && evt.detail && evt.detail.target) || (evt && evt.target) || null;
        if (!isAppContentTarget(target)) return;
        cleanupModalArtifacts();
        runInit({ preserveScroll: true });
        if (target && typeof target.querySelector === 'function' && target.querySelector('.settings-sidebar')) {
            window.scrollTo(0, 0);
        }
    }

    document.body.addEventListener('htmx:beforeSwap', function(evt) {
        const target = (evt && evt.detail && evt.detail.target) || null;
        if (isAppContentTarget(target)) {
            cleanupModalArtifacts();
        }
    });
    document.body.addEventListener('htmx:afterSwap', handleHtmxInit);
    document.body.addEventListener('htmx:afterSettle', handleHtmxInit);
})();
