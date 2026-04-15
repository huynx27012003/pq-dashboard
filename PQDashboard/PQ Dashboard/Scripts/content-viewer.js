/**
 * Content Viewer - Dual mode popup system
 * 
 * Mode 1: openPopup() - Opens in new browser window (for Event Details, waveforms, etc.)
 * Mode 2: openModal() - Opens in-page modal with blur overlay (for Quick Search, settings, etc.)
 * 
 * Legacy open() method uses modal by default for backwards compatibility
 */
(function (window, $) {
    'use strict';

    // Track open windows for management
    var openWindows = {};
    var windowCounter = 0;

    // ============================================================
    // POPUP WINDOW MODE (New browser window)
    // ============================================================
    var popupDefaults = {
        width: 1000,
        height: 700,
        resizable: true,
        scrollbars: true
    };

    function openPopupWindow(url, title, options) {
        options = $.extend({}, popupDefaults, options);

        // Calculate center position with offset for multiple windows
        windowCounter++;
        var offset = (windowCounter % 5) * 30;
        var left = Math.max(50, (screen.width - options.width) / 2 + offset);
        var top = Math.max(50, (screen.height - options.height) / 2 + offset);

        // Minimal browser chrome (only address bar + content)
        var features = [
            'width=' + options.width,
            'height=' + options.height,
            'left=' + left,
            'top=' + top,
            'resizable=yes',
            'scrollbars=yes',
            'menubar=no',
            'toolbar=no',
            'location=yes',
            'status=no'
        ].join(',');

        var windowName = 'Popup_' + windowCounter;
        var popup = window.open(url, windowName, features);

        if (popup) {
            popup.focus();
            openWindows[windowName] = popup;
        }

        return popup;
    }

    // ============================================================
    // MODAL OVERLAY MODE (In-page with blur)
    // ============================================================
    function createModalOverlay() {
        if ($('#contentViewerOverlay').length === 0) {
            var overlayHtml =
                '<div id="contentViewerOverlay" class="content-overlay">' +
                '<div class="content-modal">' +
                '<div class="content-modal-header">' +
                '<h4 class="content-modal-title"></h4>' +
                '<button type="button" class="content-modal-close" onclick="ContentViewer.closeModal()">' +
                '<i class="fa fa-times"></i>' +
                '</button>' +
                '</div>' +
                '<div class="content-modal-body">' +
                '<iframe id="contentViewerFrame" src="about:blank"></iframe>' +
                '</div>' +
                '</div>' +
                '</div>';
            $('body').append(overlayHtml);

            // Close on overlay background click
            $('#contentViewerOverlay').on('click', function (e) {
                if (e.target === this) {
                    ContentViewer.closeModal();
                }
            });

            // Close on Escape key
            $(document).on('keydown', function (e) {
                if (e.key === 'Escape' && $('#contentViewerOverlay').hasClass('active')) {
                    ContentViewer.closeModal();
                }
            });
        }
    }

    function openModalOverlay(url, title) {
        createModalOverlay();

        var $overlay = $('#contentViewerOverlay');
        var $frame = $('#contentViewerFrame');
        var $title = $overlay.find('.content-modal-title');

        $title.text(title || 'Content Viewer');
        $frame.attr('src', url);
        $overlay.addClass('active');

        // Prevent body scroll
        $('body').css('overflow', 'hidden');
    }

    function closeModalOverlay() {
        var $overlay = $('#contentViewerOverlay');
        var $frame = $('#contentViewerFrame');

        $overlay.removeClass('active');
        $frame.attr('src', 'about:blank');

        // Restore body scroll
        $('body').css('overflow', '');
    }

    // ============================================================
    // Content Viewer API
    // ============================================================
    var ContentViewer = {
        /**
         * Open content in a NEW BROWSER WINDOW (popup)
         * Use for: Event Details, Waveforms, Charts that need comparison
         */
        openPopup: function (url, title, options) {
            return openPopupWindow(url, title, options);
        },

        /**
         * Open content in IN-PAGE MODAL with blur overlay
         * Use for: Quick Search, Settings, Help, non-data views
         */
        openModal: function (url, title) {
            openModalOverlay(url, title);
            return this;
        },

        /**
         * Close the modal overlay
         */
        closeModal: function () {
            closeModalOverlay();
            return this;
        },

        /**
         * Legacy open method - uses MODAL by default
         * For backwards compatibility
         */
        open: function (url, title, options) {
            return this.openModal(url, title);
        },

        /**
         * Legacy close method - closes modal
         */
        close: function () {
            return this.closeModal();
        },

        /**
         * Close a specific popup window
         */
        closePopup: function (name) {
            if (openWindows[name] && !openWindows[name].closed) {
                openWindows[name].close();
                delete openWindows[name];
            }
        },

        /**
         * Close all popup windows
         */
        closeAllPopups: function () {
            for (var name in openWindows) {
                if (!openWindows[name].closed) {
                    openWindows[name].close();
                }
            }
            openWindows = {};
        },

        /**
         * Check if modal is currently open
         */
        isModalOpen: function () {
            return $('#contentViewerOverlay').hasClass('active');
        },

        /**
         * Get number of open popup windows
         */
        getPopupCount: function () {
            var count = 0;
            for (var name in openWindows) {
                if (!openWindows[name].closed) {
                    count++;
                }
            }
            return count;
        }
    };

    // ============================================================
    // Navigation Utilities
    // ============================================================
    var Navigation = {
        navigate: function (url, params) {
            if (params) {
                url += (url.indexOf('?') === -1 ? '?' : '&') + $.param(params);
            }
            window.location.href = url;
        },
        back: function () {
            if (window.history.length > 1) {
                window.history.back();
            } else {
                window.location.href = homePath || '/';
            }
        },
        openExternal: function (url) {
            window.open(url, '_blank');
        }
    };

    // ============================================================
    // Sidebar Toggle
    // ============================================================
    var Sidebar = {
        toggle: function () {
            $('.app-wrapper').toggleClass('sidebar-collapsed');
            var isCollapsed = $('.app-wrapper').hasClass('sidebar-collapsed');
            localStorage.setItem('sidebarCollapsed', isCollapsed);
        },
        init: function () {
            var isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
            if (isCollapsed) {
                $('.app-wrapper').addClass('sidebar-collapsed');
            }
            $(document).on('click', '.sidebar-toggle', function () {
                Sidebar.toggle();
            });
        }
    };

    // ============================================================
    // Menu
    // ============================================================
    var Menu = {
        setActive: function (menuId) {
            $('.menu-item').removeClass('active');
            $('#' + menuId).addClass('active');
            var title = $('#' + menuId).find('.menu-item-text').text();
            if (title) {
                $('.header-title').text(title);
            }
        },
        initFromUrl: function () {
            var path = window.location.pathname.toLowerCase();
            var menuMap = {
                '/main/home': 'menu-dashboard',
                '/main/systemevents': 'menu-system-events',
                '/main/meterlist': 'menu-meter-list'
            };
            for (var key in menuMap) {
                if (path.indexOf(key) !== -1) {
                    Menu.setActive(menuMap[key]);
                    break;
                }
            }
        }
    };

    // Initialize
    $(document).ready(function () {
        Sidebar.init();
        Menu.initFromUrl();
    });

    // Expose to global scope
    window.ContentViewer = ContentViewer;
    window.Navigation = Navigation;
    window.Sidebar = Sidebar;
    window.Menu = Menu;

})(window, jQuery);
