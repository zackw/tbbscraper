// Copyright © 2010, 2013, 2014 Zack Weinberg
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// There is NO WARRANTY.

// Subprocess of s_canonize.py.  Canonicalizes one URL, provided as a
// command-line argument, by following redirections.  The result is
// reported as a JSON-formatted dictionary on stdout, like this:
//
// { "canon":   "canonicalized URL",
//   "status":  "HTTP status phrase, or other one-line error message",
//   "anomaly": "details of any error that may have occurred" }
//
// 'canon' and 'anomaly' may be null.
// 'status' and 'anomaly' will be base64ed if they contain non-ASCII
// characters.

var system = require('system');
if (system.args.length < 2) {
    console.error('Usage: phantomjs pj-trace-redir.js URL');
    phantom.exit(1);
}
var address = system.args[1];
var page = require('webpage').create();


// Log events as Phantom passes them back up.  The report we want has
// to be reconstructed from several of these once the task is  complete.
var event_log = [];
var most_recent_nav_target;

function log_event(evt) {
    event_log.push(evt);
    //console.log(JSON.stringify(evt));
}

function report(final_url) {
    if (!final_url || final_url === "about:blank") {
        final_url = most_recent_nav_target;
    }

    var last_event;
    var i;
    // Scan the event log twice.  On the first pass, look specifically
    // for a successful page load for the final URL; if we find that,
    // don't treat subsequent errors as interesting.
    for (i = event_log.length - 1; i >= 0; i--) {
        if (event_log[i].url === final_url &&
            event_log[i].what === "receive" &&
            event_log[i].data.code === 200) {
            last_event = event_log[i];
            break;
        }
    }
    if (!last_event) {
        for (i = event_log.length - 1; i >= 0; i--) {
            if (event_log[i].url == final_url) {
                last_event = event_log[i];
                break;
            }
        }
    }
    if (!last_event) {
        if (event_log.length) {
            last_event = event_log[event_log.length-1];
        } else {
            last_event = {"what":"?"};
        }
    }

    var status, log = null;
    switch (last_event.what) {
    case "receive":
        switch (last_event.data.code) {
        case 200: // success!
            status = "200 OK";
            break;

            // "Normal" HTTP error codes.
        case 400:
        case 401:
        case 403:
        case 404:
        case 410:
        case 500:
        case 503:
            status = last_event.data.status;
            break;

            // All other HTTP errors are anomalous
            // (that is, they trigger a dump of the entire event log).
        default:
            status = last_event.data.status;
            log = event_log;
        }
        break;

    case "neterror":
        // Network errors are not anomalous.
        status = last_event.data.status;
        break;

    case "timeout":
        // Resource timeouts are not anomalous.
        status = "timed out";
        break;

    default:
        // All other last-events are anomalous.
        status = last_event.what;
        if (last_event.hasOwnProperty("data")) {
            if (typeof last_event.data === "string") {
                status = status + ": " + last_event.data;
            } else if (last_event.data.hasOwnProperty("status")) {
                status = status + ": " + last_event.data.status;
            }
        }
        log = event_log;
    }

    system.stdout.writeLine(JSON.stringify({
        "canon": final_url,
        "status": status,
        "log": log
    }));
    phantom.exit(0);
}

// Global 60-second timeout; per-resource 10-second timeout.
page.settings.resourceTimeout = 10000;
setTimeout(function () {
    log_event({ what: "global-timeout" });
    report();
}, 60000);


// Our modified user agent should not be _too_ much of a lie; in particular
// if the PhantomJS embedded Webkit changes too much we should change ours
// to match.
if (!/ AppleWebKit\/534\.34 /.test(page.settings.userAgent)) {
    console.error("Unexpected stock user agent: " + page.settings.userAgent);
    phantom.exit(1);
}
// Pretend to be Chrome 27 on Windows.
page.settings.userAgent =
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) ' +
    'Chrome/27.0.1453.93 Safari/537.36';

page.onAlert = function(msg) {
    log_event({what: "alert", url: page.url, data: msg});
};
page.onConfirm = function(msg) {
    log_event({what: "confirm", url: page.url, data: msg});
    return true;
};
page.onPrompt = function(msg) {
    log_event({what: "prompt", url: page.url, data: msg});
    return "fuzzy wuzzy";
};

page.onConsoleMessage = function(msg, lineNum, sourceId) {
    log_event({what: "console", url: page.url, data: msg});
};

page.onError = function(msg, trace) {
    var msgStack = [];
    if (trace && trace.length) {
        trace.forEach(function(t) {
            msgStack.push(' -> ' + t.file + ': ' + t.line +
                          (t.function ? ' (in function "' + t.function + '")'
                           : ''));
        });
    }
    log_event({what: "jserror", url: page.url, data: {
        status: msg,
        trace: msgStack
    }});
};


var really_loaded_timeout = null;
page.onLoadStarted = function () {
    if (really_loaded_timeout !== null) {
        clearTimeout(really_loaded_timeout);
        really_loaded_timeout = null;
        log_event({what: "bounce", url: page.url })
    }
}
page.onLoadFinished = function(status) {
    // Don't report quite yet - give JS and meta refresh
    // a chance to send us somewhere else.
    really_loaded_timeout = setTimeout(function () {
        report(page.url);
    }, 250);
}
page.onNavigationRequested = function(url, type, willNavigate, main) {
    if (main) {
        most_recent_nav_target = url;
        if (really_loaded_timeout !== null) {
            clearTimeout(really_loaded_timeout);
            really_loaded_timeout = null;
            log_event({what: "nav", url: page.url })
        }
    }
}

pending_resources = {}
page.onResourceRequested = function(requestData, networkRequest) {
    pending_resources[requestData.id] = true;
    log_event({
        what: "request",
        url: requestData.url,
        data: {
            method: requestData.method,
            headers: requestData.headers
        }});
}

page.onResourceReceived = function(response) {
    if (pending_resources[response.id]) {
        pending_resources[response.id] = false;
        log_event({
            what: "receive",
            url: response.url,
            data: {
                status: response.status + " " + response.statusText,
                code: response.status,
                headers: response.headers
            }});
    }
}

page.onResourceTimeout = function(request) {
    if (pending_resources[request.id]) {
        pending_resources[request.id] = false;
        log_event({ what: "timeout", url: request.url });
    }
}

page.onResourceError = function(resourceError) {
    if (pending_resources[resourceError.id]) {
        pending_resources[resourceError.id] = false;

        var status = null;

        // Some error strings are unnecessarily detailed.
        switch (resourceError.errorCode) {
        case 1:
        case 2:
        case 6:
        case 99:
        case 301:
            status = resourceError.errorString;
            break;

        case 3:
            if (/^Host \S* not found$/.test(resourceError.errorString))
                status = "host not found";
            break;

        case 299:
            if (/^Error downloading http/.test(resourceError.errorString))
                status = resourceError.errorString.replace(/^.* replied: /,
                                                   "Bad server response: ");
            break;
        }

        if (status === null) {
            status = resourceError.errorString + " (" +
                resourceError.errorCode + ")";
        }

        log_event({ what: "neterror", url: resourceError.url,
                    data: { status: status,
                            code: resourceError.errorCode }});
    }
}

page.open(address);
