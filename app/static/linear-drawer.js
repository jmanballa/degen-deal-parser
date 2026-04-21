/* linear-drawer.js — mobile slide-in drawer for the linear sidebar */
(function () {
    var OPEN_CLASS = 'linear-drawer-open';
    var FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    var prevFocus = null;
    var keydownHandler = null;

    function mainContainers() {
        return document.querySelectorAll('.lx-page, .page');
    }

    function trapTab(e, sidebar) {
        var nodes = sidebar.querySelectorAll(FOCUSABLE);
        if (!nodes.length) return;
        var first = nodes[0];
        var last = nodes[nodes.length - 1];
        var active = document.activeElement;
        if (e.shiftKey) {
            if (active === first || !sidebar.contains(active)) { last.focus(); e.preventDefault(); }
        } else {
            if (active === last) { first.focus(); e.preventDefault(); }
        }
    }

    function openDrawer() {
        document.body.classList.add(OPEN_CLASS);
        var hamburger = document.getElementById('linear-hamburger');
        if (hamburger) hamburger.setAttribute('aria-expanded', 'true');
        var sidebar = document.getElementById('linear-sidebar');
        prevFocus = document.activeElement;

        mainContainers().forEach(function (el) {
            el.setAttribute('inert', '');
            el.setAttribute('aria-hidden', 'true');
        });

        if (sidebar) {
            var firstFocusable = sidebar.querySelector(FOCUSABLE);
            if (firstFocusable) firstFocusable.focus();

            keydownHandler = function (e) {
                if (e.key === 'Tab') trapTab(e, sidebar);
            };
            document.addEventListener('keydown', keydownHandler);
        }
    }

    function closeDrawer() {
        document.body.classList.remove(OPEN_CLASS);
        var hamburger = document.getElementById('linear-hamburger');
        if (hamburger) hamburger.setAttribute('aria-expanded', 'false');

        mainContainers().forEach(function (el) {
            el.removeAttribute('inert');
            el.removeAttribute('aria-hidden');
        });

        if (keydownHandler) {
            document.removeEventListener('keydown', keydownHandler);
            keydownHandler = null;
        }

        if (prevFocus) {
            prevFocus.focus();
            prevFocus = null;
        }
    }

    function init() {
        var hamburger = document.getElementById('linear-hamburger');
        if (hamburger) hamburger.addEventListener('click', openDrawer);

        var backdrop = document.getElementById('linear-drawer-backdrop');
        if (backdrop) backdrop.addEventListener('click', closeDrawer);

        var closeBtn = document.getElementById('linear-drawer-close');
        if (closeBtn) closeBtn.addEventListener('click', closeDrawer);

        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && document.body.classList.contains(OPEN_CLASS)) {
                closeDrawer();
            }
        });

        /* Close drawer on any nav link tap */
        var items = document.querySelectorAll('.linear-sidebar-item');
        for (var i = 0; i < items.length; i++) {
            items[i].addEventListener('click', closeDrawer);
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
