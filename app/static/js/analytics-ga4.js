/**
 * Enhanced Google Analytics tracking for Pabulib
 * Provides easy methods to track custom events specific to your app
 */

// Enhanced tracking functions
window.pabulibTrack = {
    // Track page views (automatic with GA4, but useful for SPA navigation)
    pageView: function(pagePath, pageTitle) {
        if (typeof gtag !== 'undefined') {
            // Get the analytics ID from the global config set by the template
            const analyticsId = window.googleAnalyticsId;
            if (analyticsId) {
                gtag('config', analyticsId, {
                    page_path: pagePath || window.location.pathname,
                    page_title: pageTitle || document.title
                });
            }
        }
    },

    // Track file downloads
    download: function(filename, downloadType = 'single', fileCount = 1) {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'file_download', {
                event_category: 'downloads',
                event_label: filename,
                custom_parameters: {
                    download_type: downloadType,
                    file_count: fileCount,
                    file_extension: filename.split('.').pop()
                }
            });
        }
    },

    // Track tab switches
    tabSwitch: function(tabName, pageSection = null) {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'tab_switch', {
                event_category: 'engagement',
                event_label: tabName,
                custom_parameters: {
                    page_section: pageSection || 'unknown'
                }
            });
        }
    },

    // Track button clicks
    buttonClick: function(buttonName, category = 'interaction') {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'click', {
                event_category: category,
                event_label: buttonName
            });
        }
    },

    // Track form submissions
    formSubmit: function(formName, formType = 'general') {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'form_submit', {
                event_category: 'forms',
                event_label: formName,
                custom_parameters: {
                    form_type: formType
                }
            });
        }
    },

    // Track search queries
    search: function(searchTerm, resultsCount = null) {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'search', {
                search_term: searchTerm,
                custom_parameters: {
                    results_count: resultsCount
                }
            });
        }
    },

    // Track file previews/visualizations
    filePreview: function(filename, previewType = 'preview') {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'file_view', {
                event_category: 'engagement',
                event_label: filename,
                custom_parameters: {
                    view_type: previewType
                }
            });
        }
    },

    // Track upload events
    upload: function(fileCount, uploadType = 'single') {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'file_upload', {
                event_category: 'uploads',
                custom_parameters: {
                    file_count: fileCount,
                    upload_type: uploadType
                }
            });
        }
    },

    // Track filter usage
    filterUsage: function(activeFilters, resultsCount) {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'filter_usage', {
                event_category: 'search',
                custom_parameters: {
                    filters_applied: activeFilters.join(','),
                    filter_count: activeFilters.length,
                    results_count: resultsCount
                }
            });
        }
    },

    // Track sort changes
    sortChange: function(sortBy, sortDirection) {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'sort_change', {
                event_category: 'engagement',
                custom_parameters: {
                    sort_by: sortBy,
                    sort_direction: sortDirection
                }
            });
        }
    },

    // Track dataset interactions (hover/preview mini cards)
    datasetHover: function(datasetName, hoverDuration = null) {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'dataset_hover', {
                event_category: 'engagement',
                event_label: datasetName,
                custom_parameters: {
                    hover_duration_ms: hoverDuration
                }
            });
        }
    },

    // Track pagination/lazy loading
    paginationLoad: function(itemsLoaded, totalVisible) {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'pagination_load', {
                event_category: 'engagement',
                custom_parameters: {
                    items_loaded: itemsLoaded,
                    total_visible: totalVisible
                }
            });
        }
    },

    // Track performance metrics
    performanceMetric: function(metricName, value, unit = 'ms') {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'performance_metric', {
                event_category: 'performance',
                event_label: metricName,
                value: Math.round(value),
                custom_parameters: {
                    metric_unit: unit
                }
            });
        }
    },

    // Track errors
    trackError: function(errorType, errorMessage, errorLocation = null) {
        if (typeof gtag !== 'undefined') {
            gtag('event', 'exception', {
                description: errorMessage,
                fatal: false,
                custom_parameters: {
                    error_type: errorType,
                    error_location: errorLocation
                }
            });
        }
    }
};

// Auto-track common interactions
document.addEventListener('DOMContentLoaded', function() {
    // Track page load performance
    window.addEventListener('load', function() {
        const perfData = performance.getEntriesByType('navigation')[0];
        if (perfData) {
            pabulibTrack.performanceMetric('page_load_time', perfData.loadEventEnd - perfData.loadEventStart);
            pabulibTrack.performanceMetric('dom_content_loaded', perfData.domContentLoadedEventEnd - perfData.domContentLoadedEventStart);
            pabulibTrack.performanceMetric('time_to_first_byte', perfData.responseStart - perfData.requestStart);
        }
    });
    // Track downloads automatically
    document.addEventListener('click', function(e) {
        const target = e.target.closest('a[href*="/download"], button[data-download]');
        if (target) {
            const filename = target.getAttribute('data-filename') || 
                           target.href?.split('/').pop() || 
                           'unknown_file';
            const downloadType = target.getAttribute('data-download-type') || 'single';
            pabulibTrack.download(filename, downloadType);
        }
    });

    // Track tab switches automatically
    document.addEventListener('click', function(e) {
        const tabTarget = e.target.closest('[data-tab-target]');
        if (tabTarget) {
            const tabName = tabTarget.getAttribute('data-tab-target') || 
                           tabTarget.textContent.trim();
            pabulibTrack.tabSwitch(tabName);
        }
    });

    // Track button clicks automatically
    document.addEventListener('click', function(e) {
        const button = e.target.closest('button[data-track], a[data-track]');
        if (button) {
            const eventName = button.getAttribute('data-track') || 
                            button.textContent.trim();
            const category = button.getAttribute('data-track-category') || 'interaction';
            pabulibTrack.buttonClick(eventName, category);
        }
    });

    // Track form submissions automatically
    document.addEventListener('submit', function(e) {
        const form = e.target;
        if (form.hasAttribute('data-track-form')) {
            const formName = form.getAttribute('data-track-form') || form.id || 'unnamed_form';
            const formType = form.getAttribute('data-form-type') || 'general';
            
            // Enhanced tracking for download forms
            if (formName === 'batch_download') {
                // Count selected files
                const checkedBoxes = form.querySelectorAll('input[type="checkbox"]:checked:not(#selectAll)');
                const selectAllBox = form.querySelector('#selectAll');
                const isSelectAll = selectAllBox && selectAllBox.checked;
                const fileCount = checkedBoxes.length;
                
                // Determine download type
                let downloadType = 'batch';
                if (isSelectAll) {
                    downloadType = 'download_all';
                } else if (fileCount === 1) {
                    downloadType = 'single_via_form';
                }
                
                // Enhanced form submit event
                if (typeof gtag !== 'undefined') {
                    gtag('event', 'form_submit', {
                        event_category: 'forms',
                        event_label: formName,
                        custom_parameters: {
                            form_type: formType,
                            download_type: downloadType,
                            file_count: fileCount,
                            is_select_all: isSelectAll
                        }
                    });
                }
            } else {
                // Standard form submit tracking
                pabulibTrack.formSubmit(formName, formType);
            }
        }
    });

    // Track JavaScript errors
    window.addEventListener('error', function(e) {
        pabulibTrack.trackError('javascript_error', e.message, e.filename + ':' + e.lineno);
    });

    // Track unhandled promise rejections
    window.addEventListener('unhandledrejection', function(e) {
        pabulibTrack.trackError('promise_rejection', e.reason?.toString() || 'Unknown promise rejection');
    });
});