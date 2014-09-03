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
// { "ourl":    "original URL",
//   "status":  "final HTTP response code or network error code",
//   "detail":  "HTTP status phrase, or other one-line error message",
//   "canon":   "canonicalized URL",
//   "redirs":  { "http": x, "html": y, "js": z } // total # of redirections
//                                                // of each type
//   "log":     [ /* array of events; only present for anomalous failures */ ]
// }
//
// 'canon' and 'log' may be absent; "detail" may be null.
//
// If invoked with --capture, successful canonicalizations (status == 200)
// will include two more entries in the dictionary, both string-valued:
// "content" the text content of the page, and "render" a base64-ed PNG of
// the rendering of the page.  These properties will be absent for
// unsuccessful canonicalizations.

function usage() {
    console.error('Usage: phantomjs pj-trace-redir.js [--capture] URL');
    phantom.exit(1);
}

var system = require('system');
if (system.args.length < 2 || system.args.length > 3)
    usage();
var capture = (system.args[1] === "--capture");
if (capture && system.args.length < 3)
    usage();
var address = capture ? system.args[2] : system.args[1];
var redirs = { http: 0, html: 0, js: 0 };

var page = require('webpage').create();

// Log events as Phantom passes them back up.  The report we want has
// to be reconstructed from several of these once the task is  complete.
var pending_resources = {};
var resource_status = {};
var event_log = [];
var probable_top_level_resources = [address];

function log_event(evt) {
    //console.log(JSON.stringify(evt));
    event_log.push(evt);
}

function report() {
    var final_url, status, output;

    //console.log(JSON.stringify(probable_top_level_resources));

    // this is an approximation, likely to be invalid in the presence
    // of iframes
    redirs.http = probable_top_level_resources.length -
        (redirs.html + redirs.js);

    while ((final_url = probable_top_level_resources.pop())) {
        if (/^about:/.test(final_url))
            continue;

        if (!resource_status.hasOwnProperty(final_url))
            continue;

        status = resource_status[final_url];
        output = {
            ourl:   address,
            status: status.code,
            detail: status.detail,
            canon:  final_url,
            redirs: redirs,
            log:    event_log
        };
        if (capture) {
            output.content = page.content.replace(/\s+/g, " ");
            output.render = page.renderBase64("PNG");
        }

        system.stdout.writeLine(JSON.stringify(output));
        phantom.exit(0);
        return;
    }

    system.stdout.writeLine(JSON.stringify({
        ourl: address,
        status: "abnormal failure",
        detail: null,
        redirs: redirs,
        log: event_log
    }));
    phantom.exit(0);
}

// Despite the rise of widescreen, the equally meteoric rise of mobile
// devices means that 1024 is still a very common screen width.  768
// is, alas, still the single most common screen height.  If the page
// is very tall, PJS will render all of it despite the viewport
// height, so it is also necessary to set .clipRect.
page.viewportSize = { width: 1024, height: 768 };
page.clipRect = { top: 0, left: 0, width: 1024, height: 768 };

// Global 9-minute timeout (just below isolate.c's 10-minute SIGKILL);
// per-resource 30-second timeout.
var really_loaded_timeout = null;
page.settings.resourceTimeout = 30 * 1000;
setTimeout(function () {
    // If this fires in the middle of onLoadFinished's ten-second
    // delay to give JS a chance to send us somewhere else, just cut
    // that off early.  Otherwise, make note of it in the log and mark
    // all outstanding resources as timed out.
    if (really_loaded_timeout === null) {
        var i;
        for (i in pending_resources)
            resource_status[pending_resources[i]] = { code: "timeout" };
        log_event({ what: "global-timeout" });
    }
    report();
}, 9 * 60 * 1000);

// Our modified user agent should not be _too_ much of a lie; in particular
// if the PhantomJS embedded Webkit changes too much we should change ours
// to match.  Unfortunately, neither of the AppleWebKit/xxx.yy strings
// corresponding to PhantomJS's *actual* WebKit are common in real browsers.
if (/ AppleWebKit\/534\.34 /.test(page.settings.userAgent)) {
    // PhantomJS 1.9. Pretend to be Safari 5.1 on OSX.
    page.settings.userAgent =
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7) AppleWebKit/534.34.4 ' +
        '(KHTML, like Gecko) Version/5.1 Safari/534.34.4';
} else if (/ AppleWebKit\/538.1 /.test(page.settings.userAgent)) {
    // PhantomJS 2.0. Pretend to be Safari 6.0.5 on OSX.
    page.settings.userAgent =
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/538.1+ ' +
        '(KHTML, like Gecko) Version/6.0.5 Safari/536.30.1';
} else {
    console.error("Unexpected stock user agent: " + page.settings.userAgent);
    phantom.exit(1);
}

//
// These are here mostly for logging.
//
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
    log_event({what: "jserror", url: page.url, data: msg });
};

//
// onLoadFinished is called when each page load completes - but that
// means _after_ all HTTP-level redirections, and if there's an error
// in the middle of the chain, it won't see a useful page.url.
// We have to work around that using the resource hooks, below.
//

page.onLoadFinished = function(status) {
    log_event({what: "onLoadFinished", url: page.url, data: status});
    if (status === "success") {
        if (/^https?:/.test(page.url))
            probable_top_level_resources.push(page.url);
    }

    // Look for <meta refresh> and <link rel="canonical">.
    // If we find either of them, and they aren't circular, immediately
    // load the target.  Prefer <meta refresh>.
    var html_redir_target = page.evaluate(function() {
        var cand = document.querySelector("meta[http-equiv=refresh]");
        if (cand) {
            var content = cand.getAttribute("content");
            if (typeof content === "string") {
                content = content.split(";")[1].trim();
                content = content.replace(/^url=/i, '').trim();

                if (content && /^https?:/.test(content))
                    return content;
            }
        }

        // There's not supposed to be more than one <link rel="canonical">;
        // we just take the first one if there are.
        cand = document.querySelector("link[rel=canonical]");
        if (cand)
            return cand.getAttribute("href");

        return null;
    });

    if (html_redir_target && html_redir_target !== page.url) {
        log_event({ what: "html-redir", url: html_redir_target });
        redirs.html += 1;
        page.open(html_redir_target);
        return;
    }

    // If there were any scripts, don't report quite yet; give them
    // ten seconds to send us somewhere else.
    if (page.evaluate(function() {
        function documentHasScript(doc) {
            return (doc.getElementsByTagName("script").length > 0 ||
                    doc.evaluate("count(//@*[starts-with(name(), 'on')])",
                                 doc, null, XPathResult.NUMBER_TYPE,
                                 null).numberValue > 0);
        }
        function windowHasScript(win) {
            var i;
            if (documentHasScript(win.document))
                return true;
            for (i = 0; i < win.frames.length; i++)
                if (windowHasScript(win.frames[i]))
                    return true;
            return false;
        }
        return windowHasScript(window);

    })) {
        really_loaded_timeout = setTimeout(function () {
            report();
        }, 10 * 1000);
    } else {
        report();
    }
};

//
// These don't see HTTP-level redirections either, and they aren't
// told the destination URL reliably.  Their main function is to
// cancel the really_loaded_timeout when appropriate.
//

page.onNavigationRequested = function(url, type, willNavigate, main) {
    if (main && really_loaded_timeout !== null) {
        clearTimeout(really_loaded_timeout);
        really_loaded_timeout = null;
        log_event({what: "nav", url: url });
        redirs.js += 1;
    }
};
page.onLoadStarted = function () {
    if (really_loaded_timeout !== null) {
        clearTimeout(really_loaded_timeout);
        really_loaded_timeout = null;
        log_event({what: "bounce", url: page.url });
        redirs.js += 1;
    }
};

//
// The next four hooks see _every_ resource load.
//

function hasHeader(headers, key, val) {
    var i;
    for (i = 0; i < headers.length; i++)
        if (headers[i].name === key)
            return headers[i].value === val;
    return false;
}

page.onResourceRequested = function(requestData, networkRequest) {
    pending_resources[requestData.id] = requestData.url;
    log_event({
        what: "request",
        url: requestData.url,
        data: {
            method: requestData.method,
            headers: requestData.headers
        }});

    // There is no _good_ way of telling whether this is a top-level page
    // load.  This looks for HTML page loads (which could be for (i)frames).
    // JavaScript might cause POSTs.
    if (/^https?:/.test(requestData.url) &&
        hasHeader(requestData.headers, "Accept",
                  "text/html,application/xhtml+xml,"+
                  "application/xml;q=0.9,*/*;q=0.8")) {
        probable_top_level_resources.push(requestData.url);

        // Webkit catches HTTP redirect loops, but not HTML/JS redirect
        // loops.  Webkit's limit is 20.  Add a hefty buffer for frames
        // and suchlike.
        if (probable_top_level_resources.length >= 100)
            report();
    }
};

page.onResourceReceived = function(response) {
    if (pending_resources[response.id]) {
        var origUrl = pending_resources[response.id];
        var status = {
            code: response.status,
            detail: response.status + " " + response.statusText,
            headers: response.headers
        };
        resource_status[response.url] = status;
        if (response.url != origUrl)
            resource_status[origUrl] = status;
        log_event({
            what: "receive",
            url: response.url,
            origUrl: origUrl,
            data: status
        });

        pending_resources[response.id] = false;
    }
};

page.onResourceTimeout = function(request) {
    if (pending_resources[request.id]) {
        var origUrl = pending_resources[request.id];
        var status = {
            code: "timeout",
            detail: null
        };
        resource_status[request.url] = status;
        if (request.url != origUrl)
            resource_status[origUrl] = status;
        log_event({
            what: "timeout",
            url: request.url,
            origUrl: origUrl
        });
        pending_resources[request.id] = false;
    }
};

page.onResourceError = function(resourceError) {
    if (pending_resources[resourceError.id]) {
        var origUrl = pending_resources[resourceError.id];

        var status;
        if (resourceError.status) {
            status = {
                code: resourceError.status,
                detail: resourceError.status + " " + resourceError.statusText
            };
        } else {
            status = {
                code: "N"+resourceError.errorCode
            };

            // Some error strings are unnecessarily detailed.
            switch (resourceError.errorCode) {
            case 3:
                status.detail = "N3 Host not found";
                break;

            default:
                status.detail = status.code + " " +
                    resourceError.errorString;
            }
        }
        resource_status[resourceError.url] = status;
        if (resourceError.url != origUrl)
            resource_status[origUrl] = status;
        log_event({
            what: "error",
            url: resourceError.url,
            origUrl: origUrl,
            data: status
        });
        pending_resources[resourceError.id] = false;
    }
};

page.open(address);

/*global require, console, phantom, setTimeout, clearTimeout */
