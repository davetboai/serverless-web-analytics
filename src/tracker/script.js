(function () {
  "use strict";
  var script = document.currentScript;
  var siteId = script.getAttribute("data-website-id");
  if (!siteId) return;

  var endpoint = script.src.replace(/\/script\.js$/, "");

  function send() {
    var data = JSON.stringify({
      sid: siteId,
      url: location.pathname + location.search,
      ref: document.referrer,
      sw: screen.width,
      sh: screen.height,
      lang: navigator.language,
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
  send();

  // Track SPA navigation
  var origPush = history.pushState;
  history.pushState = function () {
    origPush.apply(history, arguments);
    send();
  };
  window.addEventListener("popstate", send);
})();
