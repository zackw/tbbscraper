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
//   "log":     [ /* array of network-level events */ ]
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
    console.error(
        'Usage: phantomjs pj-trace-redir.js [--capture|--render] URL');
    phantom.exit(1);
}

var system = require('system');
if (system.args.length < 2 || system.args.length > 3)
    usage();
var capture = (system.args[1] === "--capture" ||
               system.args[1] === "--render");
var render = (system.args[1] === "--render");
if (capture && system.args.length < 3)
    usage();
var address = capture ? system.args[2] : system.args[1];
var WebPage = require('webpage');

// Log events as Phantom passes them back up.  The report we want has
// to be reconstructed from several of these once the task is  complete.
var pending_resources = {};
var resource_status = {};
var event_log = [];
var redirection_chain = [];
var redirs = { http: 0, html: 0, js: 0 };
var window_serial = 0;
var really_loaded_timeout = null;
var navPending = false;

function redirection_chain_last() {
    if (redirection_chain.length > 0)
        return redirection_chain[redirection_chain.length - 1];
    else
        return "";
}

function in_redirection_chain(url) {
    var i;
    for (i = 0; i < redirection_chain.length; i++)
        // It is not unheard of for sites to redirect e.g.
        // "http://company.example/" to "http://company.example" with
        // <link rel="canonical"> and simultaneously redirect the
        // latter to the former with HTTP, which will put us into an
        // infinite loop.
        if (   url       === redirection_chain[i]
            || url + "/" === redirection_chain[i]
            || url       === redirection_chain[i] + "/")
            return true;
    return false;
}

function log_event(evt) {
    //console.log(JSON.stringify(evt));
    event_log.push(evt);
}

function report(page) {
    var i, final_url, status, output;

    for (i = redirection_chain.length - 1; i >= 0; i--) {
        final_url = redirection_chain[i];
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
            chain:  redirection_chain,
            log:    event_log
        };
        if (capture)
            output.content = page.content.replace(/\s+/g, " ");
        if (render)
            output.render = page.renderBase64("PNG");

        system.stdout.writeLine(JSON.stringify(output));
        phantom.exit(0);
        // phantom.exit does not exit immediately.
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

//
// These are mostly for logging. They also ensure that the process will not
// get stuck on a JS prompt, nor will any JS messages get written to stdout
// to confuse the caller.
//
function p_onConsoleMessage(msg, lineNum, sourceId) {
    log_event({e: "console", w: this.serial, u: this.url, d: msg});
};
function p_onError(msg, trace) {
    log_event({e: "jserror", w: this.serial, u: this.url, d: msg});
};
function p_onAlert(msg) {
    log_event({e: "alert",   w: this.serial, u: this.url, d: msg});
};
function p_onConfirm(msg) {
    log_event({e: "confirm", w: this.serial, u: this.url, d: msg});
    return true;
};
function p_onPrompt(msg) {
    log_event({e: "prompt",  w: this.serial, u: this.url, d: msg});
    return "fuzzy wuzzy";
};

//
// onLoadFinished is called when each page load completes - but that
// means _after_ all HTTP-level redirections, and if there's an error
// in the middle of the chain, it won't see a useful page.url.
// We have to work around that using the resource hooks, below.
//

function p_onLoadFinished(status) {

    var serial = this.serial;
    var url = this.url;

    // Under some circumstances (notably, Qt giving up on a
    // redirection loop) this.url will be "", in which case
    // we actually want to pay attention to the most recent
    // thing pushed on the redirection chain.
    if (url === "")
        url = redirection_chain_last();

    log_event({e: "onLoadFinished", w: serial, u: url,
               d: { status: status,
                    pending: navPending } });

    // Ignore anything loaded in a window other than the original.
    if (serial !== 0)
        return;

    // If we appear to be stuck in some sort of redirection loop, stop.
    // QtWebkit's limit on HTTP redirections is order of 30; leave room
    // for a few JS/HTML redirections on top of that.
    if (redirection_chain.length >= 50) {
        report(this);
        return;
    }

    // If navigation to a different page is pending at this point,
    // that means JS redirected us somewhere _before_ the page load
    // completed; allow that load to fire and complete.
    if (navPending && navPending !== url)
        return;

    // Look for <meta refresh> and <link rel="canonical">.
    // If we find either of them, and they aren't circular, immediately
    // load the target.  Prefer <meta refresh>.
    var html_redir_target = this.evaluate(function() {
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

    if (html_redir_target && !in_redirection_chain(html_redir_target)) {
        log_event({e: "html-redir", w: serial, u: url,
                   d: html_redir_target});
        redirs.html += 1;
        navPending = phantom.resolveRelativeUrl(html_redir_target, url);
        this.open(navPending);
        return;
    }

    // If there were any scripts, don't report quite yet; give them
    // ten seconds to send us somewhere else.
    if (this.evaluate(function() {
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
        really_loaded_timeout = setTimeout((function () {
            report(this);
        }).bind(this), 10 * 1000);
    } else {
        report(this);
    }
};

//
// With Qt5-based PhantomJS, onNavigationRequested reliably sees
// every HTTP-level replacement of the top-level page, and can
// therefore be used to maintain the redirection chain.
//

function p_onNavigationRequested(url, type, willNavigate, main) {
    log_event({e: "nav", w: this.serial, u: this.url,
               d: { dest: url,
                    main: main,
                    type: type,
                    will: willNavigate }});

    if (main && willNavigate && this.serial === 0) {
        if (really_loaded_timeout !== null) {
            clearTimeout(really_loaded_timeout);
            really_loaded_timeout = null;
        }

        if (!navPending) {
            navPending = url;
            redirs.js += 1;
        } else if (navPending === redirection_chain_last())
            navPending = url;

        redirection_chain.push(url);
    }
};

// This function may not actually be necessary.
function p_onLoadStarted () {
    var cleared;
    if (really_loaded_timeout !== null && this.serial === 0) {
        clearTimeout(really_loaded_timeout);
        really_loaded_timeout = null;
        redirs.js += 1;
        cleared = true;
    } else {
        cleared = false;
    }
    log_event({e: "bounce", w: this.serial, u: this.url,
               d: {clearedTimeout: cleared}});
};

//
// The next four hooks see _every_ resource load.
//

function frobHeaders(headers) {
    var frobbed = {};
    var i;
    for (i = 0; i < headers.length; i++) {
        var key = headers[i].name.toLowerCase();
        var val = headers[i].value;
        if (frobbed.hasOwnProperty(key)) {
            if (Array.isArray(frobbed[key]))
                frobbed[key].push(val);
            else
                frobbed[key] = [frobbed[key], val];
        } else
            frobbed[key] = val;
    }

    return frobbed;
}

function p_onResourceRequested(requestData, networkRequest) {
    pending_resources[requestData.id] = requestData.url;
    log_event({
        e: "request",
        w: this.serial,
        u: requestData.url,
        d: {
            method: requestData.method,
            headers: frobHeaders(requestData.headers)
        }});
}

function p_onResourceReceived(response) {
    if (pending_resources[response.id]) {
        var origUrl = pending_resources[response.id];
        var status = {
            code: response.status,
            detail: response.status + " " + response.statusText,
            headers: frobHeaders(response.headers)
        };
        resource_status[response.url] = status;
        if (response.url != origUrl) {
            resource_status[origUrl] = status;
            status.newUrl = response.url;
        }
        log_event({
            e: "receive",
            w: this.serial,
            u: origUrl,
            d: status
        });

        if (this.serial === 0 && origUrl === redirection_chain_last()) {
            if (status.headers.hasOwnProperty("location") &&
                (status.code === 301 ||
                 status.code === 302 ||
                 status.code === 303 ||
                 status.code === 307 ||
                 status.code === 308)) {
                redirs.http += 1;
            } else {
                // This resource load completes a pending navigation.
                navPending = false;
            }
        }

        pending_resources[response.id] = false;
    }
};

function p_onResourceTimeout(request) {
    if (pending_resources[request.id]) {
        var origUrl = pending_resources[request.id];
        var status = {
            code: "timeout",
            detail: null
        };
        resource_status[request.url] = status;
        if (request.url != origUrl) {
            resource_status[origUrl] = status;
            status.newUrl = request.url;
        }
        log_event({
            e: "timeout",
            w: this.serial,
            u: origUrl,
            d: status
        });
        if (this.serial === 0 && origUrl === redirection_chain_last())
            navPending = false;

        pending_resources[request.id] = false;
    }
};

function p_onResourceError(resourceError) {
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
        if (resourceError.url != origUrl) {
            resource_status[origUrl] = status;
            status.newUrl = resourceError.url;
        }
        log_event({
            e: "error",
            w: this.serial,
            u: origUrl,
            d: status
        });
        if (this.serial === 0 && origUrl === redirection_chain_last())
            navPending = false;

        pending_resources[resourceError.id] = false;
    }
};

var userAgent;
function p_onPageCreated(page) {
    page.serial = window_serial++;

    // A just-created page does not know its URL yet.
    log_event({e: "open", w: page.serial, u: null, d: { parent: this.serial }});

    // Per-resource 30-second timeout.
    page.settings.resourceTimeout = 30 * 1000;

    // Despite the rise of widescreen, the equally meteoric rise of
    // smartphones means that 1024 is still a very common screen width.
    // 768 is, alas, still the single most common screen height.
    // If the page is very tall, PJS will render all of it despite
    // the viewport height, so it is also necessary to set .clipRect.
    page.viewportSize = { width: 1024, height: 768 };
    page.clipRect = { top: 0, left: 0, width: 1024, height: 768 };

    // Set below.
    page.settings.userAgent = userAgent;

    // Page hooks.
    page.onAlert               = p_onAlert.bind(page);
    page.onConfirm             = p_onConfirm.bind(page);
    page.onPrompt              = p_onPrompt.bind(page);
    page.onConsoleMessage      = p_onConsoleMessage.bind(page);
    page.onError               = p_onError.bind(page);
    page.onLoadFinished        = p_onLoadFinished.bind(page);
    page.onNavigationRequested = p_onNavigationRequested.bind(page);
    page.onLoadStarted         = p_onLoadStarted.bind(page);
    page.onResourceRequested   = p_onResourceRequested.bind(page);
    page.onResourceReceived    = p_onResourceReceived.bind(page);
    page.onResourceTimeout     = p_onResourceTimeout.bind(page);
    page.onResourceError       = p_onResourceError.bind(page);
    page.onPageCreated         = p_onPageCreated.bind(page);
}


var page = WebPage.create();

// Our modified user agent should not be _too_ much of a lie; in
// particular if the PhantomJS embedded Webkit changes too much we
// should change ours to match.  Unfortunately, neither of the
// AppleWebKit/xxx.yy strings corresponding to PhantomJS's
// *actual* WebKit are common in real browsers.

if (/ AppleWebKit\/534\.34 /.test(page.settings.userAgent)) {
    // PhantomJS 1.9. Pretend to be Safari 5.1 on OSX.
    userAgent =
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7) ' +
        'AppleWebKit/534.34.4 ' +
        '(KHTML, like Gecko) Version/5.1 Safari/534.34.4';
} else if (/ AppleWebKit\/538.1 /.test(page.settings.userAgent)) {
    // PhantomJS 2.0. Pretend to be Safari 6.0.5 on OSX.
    userAgent =
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) ' +
        'AppleWebKit/538.1+ ' +
        '(KHTML, like Gecko) Version/6.0.5 Safari/536.30.1';
} else {
    console.error("Unexpected stock user agent: " +
                  page.settings.userAgent);
    phantom.exit(1);
}

p_onPageCreated.bind({serial:-1})(page);

// Global 9-minute timeout (just below isolate.c's 10-minute SIGKILL);
setTimeout(function () {
    // If this fires in the middle of onLoadFinished's ten-second
    // delay to give JS a chance to send us somewhere else, just cut
    // that off early.  Otherwise, make note of it in the log and mark
    // all outstanding resources as timed out.
    if (really_loaded_timeout === null) {
        var i;
        for (i in pending_resources)
            resource_status[pending_resources[i]] = { code: "timeout" };
        log_event({ e: "global-timeout", w: null, u: null, d: null });
    }
    report(page);
}, 9 * 60 * 1000);

navPending = address;
page.open(address);

/*global require, console, phantom, setTimeout, clearTimeout */
