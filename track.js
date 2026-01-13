(function () {

  /* ------------------------------
     CONFIG
     ------------------------------ */
  const analytics_SESSION_RESET_AFTER = 90_000; // 90 seconds

  const API_ENDPOINT = "https://tracksk.onrender.com/siteanalysis/enqueue"; // unchanged
  let analytics_HEARTBEAT_INTERVAL = 10_000; // 10 seconds
  let analytics_hidden_at = null;

  /* ------------------------------
     VISITOR ID (cached per browser)
     ------------------------------ */
  function analytics_generateVisitorId() {
    return (
      "V" +
      Date.now().toString(36).slice(-4).toUpperCase() +
      Math.random().toString(36).slice(2, 6).toUpperCase()
    );
  }

  let analytics_visitor_id = localStorage.getItem("visitor_id");
  if (!analytics_visitor_id) {
    analytics_visitor_id = analytics_generateVisitorId();
    localStorage.setItem("visitor_id", analytics_visitor_id);
  }

  /* ------------------------------
     SESSION START
     ------------------------------ */
  let analytics_session_started_at = new Date().toISOString();
  let analytics_last_url = analytics_getCurrentUrl();


  /* ------------------------------
     VISIBILITY TRACKING
     ------------------------------ */
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
      analytics_hidden_at = Date.now();
    }

    if (document.visibilityState === "visible" && analytics_hidden_at) {
      const hiddenDuration = Date.now() - analytics_hidden_at;

      if (hiddenDuration > analytics_SESSION_RESET_AFTER) {
        // Reset session
        analytics_session_started_at = new Date().toISOString();
      }

      analytics_hidden_at = null;
    }
  });


  /* ------------------------------
     CURRENT URL
     ------------------------------ */
  function analytics_getCurrentUrl() {
    return window.location.href;
  }

  /* ------------------------------
     CITY (fetch once per session)
     ------------------------------ */
  let analytics_city = null;

  (async function analytics_fetchCityOnce() {
    try {
      const res = await fetch("https://ipapi.co/json/", {
        cache: "no-store",
        mode: "cors"
      });

      if (res.ok) {
        const data = await res.json();
        analytics_city =
          typeof data.city === "string" ? data.city : null;
      }
    } catch {
      analytics_city = null; // VPN / ad blocker / network issue
    }
  })();

  /* ------------------------------
     HEARTBEAT LOOP
     ------------------------------ */
  setInterval(() => {
    const currentUrl = analytics_getCurrentUrl();

    // 🔁 URL changed → new session
    if (currentUrl !== analytics_last_url) {
      analytics_session_started_at = new Date().toISOString();
      analytics_last_url = currentUrl;
    }

    const analytics_payload = {
      visitor_id: analytics_visitor_id,
      city: analytics_city,
      url: currentUrl,
      session_started_at: analytics_session_started_at
    };

    fetch(API_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(analytics_payload),
      keepalive: true
    }).catch(() => {});
  }, analytics_HEARTBEAT_INTERVAL);


  /* ------------------------------
     FINAL SEND ON PAGE EXIT
     ------------------------------ */
  window.addEventListener("pagehide", () => {
    const analytics_payload = {
      visitor_id: analytics_visitor_id,
      city: analytics_city,
      url: analytics_getCurrentUrl(),
      session_started_at: analytics_session_started_at
    };

    navigator.sendBeacon(
      API_ENDPOINT,
      JSON.stringify(analytics_payload)
    );
  });

})();
