/*jslint browser: true */
/*global $, WebSocket, jQuery, moment */

var flower = (function () {
    "use strict";

    var alertContainer = document.getElementById('alert-container');
    function show_alert(message, type) {
        var wrapper = document.createElement('div');
        wrapper.innerHTML = `
            <div class="alert alert-${type} alert-dismissible" role="alert">
                <div>${message}</div>
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>`;
        alertContainer.appendChild(wrapper);
    }

    function calculateDuration(started, timestamp) {
        if (started && timestamp) {
            return ((timestamp - started) / 1000).toFixed(2) + ' sec';
        }
        return 'N/A';
    }

    function url_prefix() {
        var prefix = $('#url_prefix').val();
        if (prefix) {
            prefix = prefix.replace(/\/+$/, '');
            if (prefix.startsWith('/')) {
                return prefix;
            } else {
                return '/' + prefix;
            }
        }
        return '';
    }

    function htmlEscapeEntities(d) {
        return typeof d === 'string' ?
            d.replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;') :
            d;
    }

    function active_page(name) {
        var pathname = $(location).attr('pathname');
        if (name === '/') {
            return pathname === (url_prefix() + name);
        }
        else {
            return pathname.startsWith(url_prefix() + name);
        }
    }

    function format_time(timestamp) {
        var time = $('#time').val(),
            prefix = time.startsWith('natural-time') ? 'natural-time' : 'time',
            tz = time.substr(prefix.length + 1) || 'UTC';

        if (prefix === 'natural-time') {
            return moment.unix(timestamp).tz(tz).fromNow();
        }
        return moment.unix(timestamp).tz(tz).format('YYYY-MM-DD HH:mm:ss.SSS');
    }

    function isColumnVisible(name) {
        var columns = $('#columns').val();
        if (columns === "all")
            return true;
        if (columns) {
            columns = columns.split(',').map(function (e) {
                return e.trim();
            });
            return columns.indexOf(name) !== -1;
        }
        return true;
    }

    $.urlParam = function (name) {
        var results = new RegExp('[\\?&]' + name + '=([^&#]*)').exec(window.location.href);
        return (results && results[1]) || 0;
    };

    $(document).ready(function () {
        if (!active_page('/tasks')) {
            return;
        }

        $('#tasks-table').DataTable({
            rowId: 'uuid',
            searching: true,
            scrollX: true,
            scrollCollapse: true,
            processing: true,
            serverSide: true,
            colReorder: true,
            lengthMenu: [15, 30, 50, 100],
            pageLength: 15,
            stateSave: true,
            language: {
                lengthMenu: 'Show _MENU_ tasks',
                info: 'Showing _START_ to _END_ of _TOTAL_ tasks',
                infoFiltered: '(filtered from _MAX_ total tasks)'
            },
            ajax: {
                type: 'POST',
                url: url_prefix() + '/tasks/datatable',
                data: function (d) {
                    d.taskname = $('#task-name-filter').val();
                    d.workername = $('#worker-filter').val();
                }
            },
            order: [
                [7, "desc"]
            ],
            oSearch: {
                "sSearch": $.urlParam('state') ? 'state:' + $.urlParam('state') : ''
            },
            columnDefs: [{
                targets: 0,
                data: 'name',
                visible: isColumnVisible('name'),
                render: function (data, type, full, meta) {
                    return data;
                }
            }, {
                targets: 1,
                data: 'uuid',
                visible: isColumnVisible('uuid'),
                orderable: false,
                className: "text-nowrap",
                render: function (data, type, full, meta) {
                    return '<a href="' + url_prefix() + '/task/' + encodeURIComponent(data) + '">' + data + '</a>';
                }
            }, {
                targets: 2,
                data: 'state',
                visible: isColumnVisible('state'),
                className: "text-center",
                render: function (data, type, full, meta) {
                    switch (data) {
                        case 'SUCCESS':
                            return '<span class="badge bg-success">' + data + '</span>';
                        case 'FAILURE':
                            return '<span class="badge bg-danger">' + data + '</span>';
                        default:
                            return '<span class="badge bg-secondary">' + data + '</span>';
                    }
                }
            }, {
                targets: 3,
                data: 'args',
                className: "text-nowrap overflow-auto",
                visible: isColumnVisible('args'),
                render: htmlEscapeEntities
            }, {
                targets: 4,
                data: 'kwargs',
                className: "text-nowrap overflow-auto",
                visible: isColumnVisible('kwargs'),
                render: htmlEscapeEntities
            }, {
                targets: 5,
                data: 'result',
                visible: isColumnVisible('result'),
                className: "text-nowrap overflow-auto",
                render: htmlEscapeEntities
            }, {
                targets: 6,
                data: 'received',
                className: "text-nowrap",
                visible: isColumnVisible('received'),
                render: function (data, type, full, meta) {
                    if (data) {
                        return format_time(data);
                    }
                    return data;
                }
            }, {
                targets: 7,
                data: 'started',
                className: "text-nowrap",
                visible: isColumnVisible('started'),
                render: function (data, type, full, meta) {
                    if (data) {
                        return format_time(data);
                    }
                    return data;
                }
            }, {
                targets: 8,
                data: null,
                className: "text-center",
                visible: isColumnVisible('duration'),
                render: function (data, type, full, meta) {
                    return calculateDuration(full.started, full.timestamp);
                }
            }, {
                targets: 9,
                data: 'runtime',
                className: "text-center",
                visible: isColumnVisible('runtime'),
                render: function (data, type, full, meta) {
                    return data ? data.toFixed(2) : data;
                }
            }, {
                targets: 10,
                data: 'worker',
                visible: isColumnVisible('worker'),
                render: function (data, type, full, meta) {
                    return '<a href="' + url_prefix() + '/worker/' + encodeURIComponent(data) + '">' + data + '</a>';
                }
            }, {
                targets: 11,
                data: 'exchange',
                visible: isColumnVisible('exchange')
            }, {
                targets: 12,
                data: 'routing_key',
                visible: isColumnVisible('routing_key')
            }, {
                targets: 13,
                data: 'retries',
                className: "text-center",
                visible: isColumnVisible('retries')
            }, {
                targets: 14,
                data: 'revoked',
                className: "text-nowrap",
                visible: isColumnVisible('revoked'),
                render: function (data, type, full, meta) {
                    if (data) {
                        return format_time(data);
                    }
                    return data;
                }
            }, {
                targets: 15,
                data: 'exception',
                className: "text-nowrap",
                visible: isColumnVisible('exception')
            }, {
                targets: 16,
                data: 'expires',
                visible: isColumnVisible('expires')
            }, {
                targets: 17,
                data: 'eta',
                visible: isColumnVisible('eta')
            },],
        });

        $('#task-name-filter, #worker-filter').on('change', function () {
            $('#tasks-table').DataTable().ajax.reload();
        });
    });

    // Other existing functions (show_alert, url_prefix, etc.) should remain unchanged

    return {
        // Public methods and properties if needed
    };
}(jQuery));
