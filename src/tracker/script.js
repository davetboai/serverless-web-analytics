(function () {
  "use strict";
  var script = document.currentScript;
  var siteId = script.getAttribute("data-website-id");
  if (!siteId) return;

  var endpoint = script.src.replace(/\/script\.js$/, "");
  var sessionId =
    sessionStorage.getItem("_sa_sid") ||
    Math.random().toString(36).substring(2, 10);
  sessionStorage.setItem("_sa_sid", sessionId);
  var lastPingTime = Date.now();

  function send(type) {
    var now = Date.now();
    var dur = 0;
    if (type === "ping") {
      dur = Math.round((now - lastPingTime) / 1000);
      lastPingTime = now;
    }
    var data = JSON.stringify({
      sid: siteId,
      type: type || "pageview",
      url: location.pathname + location.search,
      ref: document.referrer,
      sw: screen.width,
      sh: screen.height,
      lang: navigator.language,
      ses: sessionId,
      dur: dur,
    });
    var url = endpoint + "/api/collect";
    if (navigator.sendBeacon) {
      navigator.sendBeacon(url, data);
    } else {
      var xhr = new XMLHttpRequest();
      xhr.open("POST", url, true);
      xhr.setRequestHeader("Content-Type", "application/json");
      xhr.send(data);
    }
  }

  // Track initial pageview
  send("pageview");

  // Track SPA navigation
  var origPush = history.pushState;
  history.pushState = function () {
    origPush.apply(history, arguments);
    lastPingTime = Date.now();
    send("pageview");
  };
  window.addEventListener("popstate", function () {
    lastPingTime = Date.now();
    send("pageview");
  });

  // Custom event API: window.sa.event("signup", {plan: "pro"})
  function trackEvent(name, props) {
    var data = JSON.stringify({
      sid: siteId,
      type: "event",
      name: name,
      props: props || {},
      url: location.pathname + location.search,
      ses: sessionId,
    });
    var url = endpoint + "/api/collect";
    if (navigator.sendBeacon) {
      navigator.sendBeacon(url, data);
    } else {
      var xhr = new XMLHttpRequest();
      xhr.open("POST", url, true);
      xhr.setRequestHeader("Content-Type", "application/json");
      xhr.send(data);
    }
  }

  // Expose API on window
  window.sa = window.sa || {};
  window.sa.event = trackEvent;

  // Auto-track outbound link clicks
  document.addEventListener("click", function (e) {
    var link = e.target;
    while (link && link.tagName !== "A") link = link.parentElement;
    if (!link || !link.href) return;
    try {
      var u = new URL(link.href);
      if (u.hostname !== location.hostname) {
        trackEvent("outbound", { url: u.hostname + u.pathname });
      }
    } catch (err) {}
  });

  // Heartbeat ping every 30s while page is visible
  var pingInterval = null;
  function startPing() {
    if (!pingInterval) {
      pingInterval = setInterval(function () {
        send("ping");
      }, 30000);
    }
  }
  function stopPing() {
    if (pingInterval) {
      clearInterval(pingInterval);
      pingInterval = null;
    }
  }
  startPing();
  document.addEventListener("visibilitychange", function () {
    if (document.hidden) {
      send("ping");
      stopPing();
    } else {
      lastPingTime = Date.now();
      startPing();
    }
  });
})();
