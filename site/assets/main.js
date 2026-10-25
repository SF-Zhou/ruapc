/* RuaPC landing page — small, dependency-free interactions. */
(() => {
  "use strict";

  const root = document.documentElement;
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  const header = document.querySelector("[data-header]");
  const menuButton = document.querySelector("[data-menu-btn]");
  const mobileNav = document.querySelector("[data-mobile-nav]");
  const revealItems = Array.from(document.querySelectorAll(".reveal"));

  function revealAll() {
    revealItems.forEach((item) => item.classList.add("in"));
  }

  /* The inline head script swaps .no-js for .js. If anything fails below,
     fail open instead of leaving the page blank. */
  const failOpen = () => root.classList.add("reveal-fallback");
  window.addEventListener("error", failOpen);

  /* Header state and scrollspy -------------------------------------------- */

  const navLinks = Array.from(document.querySelectorAll('.site-nav a[href^="#"]'));
  const spyTargets = ["top", "why", "code", "transports", "memory", "kit", "quickstart"]
    .map((id) => document.getElementById(id))
    .filter(Boolean);

  function setActiveNav(id) {
    for (const link of navLinks) {
      const active = link.getAttribute("href") === `#${id}`;
      link.classList.toggle("is-active", active);
      if (active) {
        link.setAttribute("aria-current", "location");
      } else {
        link.removeAttribute("aria-current");
      }
    }
  }

  /* A safety net for very fast scrolling: IntersectionObserver callbacks can be
     coalesced past an element, which would leave it invisible if the user
     scrolls back. Anything that has reached the viewport — or is already above
     it — is revealed by the rAF-throttled scroll handler. */
  function revealVisibleNow() {
    if (reduceMotion) return;
    const threshold = window.innerHeight * 0.94;
    for (const item of revealItems) {
      if (item.classList.contains("in")) continue;
      const rect = item.getBoundingClientRect();
      if (rect.top < threshold || rect.bottom < 0) {
        item.classList.add("in");
      }
    }
  }

  let scrollFrame = 0;
  function updateHeader() {
    scrollFrame = 0;
    if (header) {
      header.classList.toggle("is-scrolled", window.scrollY > 6);
    }
    revealVisibleNow();
  }

  window.addEventListener(
    "scroll",
    () => {
      if (!scrollFrame) scrollFrame = window.requestAnimationFrame(updateHeader);
    },
    { passive: true },
  );
  updateHeader();
  window.addEventListener("load", revealVisibleNow);
  if (document.fonts && document.fonts.ready) {
    document.fonts.ready.then(revealVisibleNow);
  }

  if ("IntersectionObserver" in window) {
    const spy = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (!entry.isIntersecting) continue;
          if (entry.target.id === "top" || entry.target.id === "quickstart") {
            setActiveNav("");
          } else {
            setActiveNav(entry.target.id);
          }
        }
      },
      { rootMargin: "-26% 0px -62% 0px", threshold: 0 },
    );
    spyTargets.forEach((section) => spy.observe(section));
  }

  /* Mobile menu ----------------------------------------------------------- */

  function setMenu(open) {
    if (!menuButton || !mobileNav) return;
    menuButton.setAttribute("aria-expanded", String(open));
    menuButton.setAttribute("aria-label", open ? "Close menu" : "Open menu");
    mobileNav.hidden = !open;
    document.body.classList.toggle("nav-open", open);
  }

  if (menuButton && mobileNav) {
    menuButton.addEventListener("click", () => {
      setMenu(menuButton.getAttribute("aria-expanded") !== "true");
    });

    mobileNav.addEventListener("click", (event) => {
      if (event.target.closest("a")) setMenu(false);
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && menuButton.getAttribute("aria-expanded") === "true") {
        setMenu(false);
        menuButton.focus();
      }
    });

    document.addEventListener("click", (event) => {
      if (
        menuButton.getAttribute("aria-expanded") === "true" &&
        header &&
        !header.contains(event.target)
      ) {
        setMenu(false);
      }
    });

    window.addEventListener("resize", () => {
      if (window.innerWidth > 1000) setMenu(false);
    });
  }

  /* Reveal on scroll ------------------------------------------------------ */

  if (reduceMotion || !("IntersectionObserver" in window)) {
    revealAll();
  } else {
    const revealObserver = new IntersectionObserver(
      (entries, observer) => {
        for (const entry of entries) {
          if (!entry.isIntersecting) continue;
          entry.target.classList.add("in");
          observer.unobserve(entry.target);
        }
      },
      { rootMargin: "0px 0px -8% 0px", threshold: 0.08 },
    );
    revealItems.forEach((item) => revealObserver.observe(item));
  }

  /* Transport switchboard ------------------------------------------------- */

  const board = document.querySelector("[data-board]");
  const boardButtons = Array.from(document.querySelectorAll(".wire[data-transport]"));
  const schemeEl = board ? board.querySelector("[data-scheme]") : null;
  const boardStatus = board ? board.querySelector("[data-board-status]") : null;
  const clientNode = board ? board.querySelector(".client-node") : null;
  const transports = {
    tcp: "tcp",
    ws: "ws",
    http: "http",
    rdma: "tcp+rdma",
  };

  for (const button of boardButtons) {
    button.addEventListener("click", () => {
      const transport = button.dataset.transport;
      if (!transport || !transports[transport]) return;

      for (const candidate of boardButtons) {
        candidate.setAttribute("aria-pressed", String(candidate === button));
      }

      if (schemeEl) schemeEl.textContent = transports[transport];
      if (board) board.dataset.transport = transport;

      if (clientNode) {
        clientNode.classList.remove("is-switching");
        // Restart the endpoint flash without keeping a timer around.
        void clientNode.offsetWidth;
        clientNode.classList.add("is-switching");
      }

      if (boardStatus) {
        boardStatus.textContent = `Endpoint preview set to ${transports[transport]}://127.0.0.1:8000`;
      }
    });
  }

  /* Code tabs ------------------------------------------------------------- */

  const tabs = Array.from(document.querySelectorAll(".code-tab"));
  const codeFile = document.querySelector("[data-code-file]");
  const codeStatus = document.querySelector("[data-code-status]");
  const copyCodeButton = document.querySelector("[data-copy-panel]");
  const codeMeta = {
    define: {
      file: "src/service.rs",
      status: "typed client + dispatch + schema",
      copyLabel: "Copy the define example",
    },
    serve: {
      file: "src/main.rs",
      status: "UNIFIED listener + dispatch",
      copyLabel: "Copy the serve example",
    },
    call: {
      file: "src/client.rs",
      status: "same method · any transport",
      copyLabel: "Copy the call example",
    },
  };

  function selectTab(tab, focus = false) {
    if (!tab) return;
    const key = tab.id.replace("tab-", "");

    for (const candidate of tabs) {
      const selected = candidate === tab;
      candidate.setAttribute("aria-selected", String(selected));
      candidate.tabIndex = selected ? 0 : -1;
      const panel = document.getElementById(candidate.getAttribute("aria-controls"));
      if (panel) panel.hidden = !selected;
    }

    const note = document.querySelector(`[data-note="${key}"]`);
    for (const candidate of document.querySelectorAll("[data-note]")) {
      candidate.hidden = candidate !== note;
    }

    if (codeFile && codeMeta[key]) codeFile.textContent = codeMeta[key].file;
    if (codeStatus && codeMeta[key]) codeStatus.textContent = codeMeta[key].status;

    const panelId = tab.getAttribute("aria-controls");
    if (copyCodeButton && panelId) {
      copyCodeButton.dataset.copyPanel = panelId;
      if (codeMeta[key]) copyCodeButton.setAttribute("aria-label", codeMeta[key].copyLabel);
    }

    if (focus) tab.focus();
  }

  for (const tab of tabs) {
    tab.addEventListener("click", () => selectTab(tab));
    tab.addEventListener("keydown", (event) => {
      const index = tabs.indexOf(tab);
      let next = null;
      if (event.key === "ArrowRight") next = tabs[(index + 1) % tabs.length];
      if (event.key === "ArrowLeft") next = tabs[(index - 1 + tabs.length) % tabs.length];
      if (event.key === "Home") next = tabs[0];
      if (event.key === "End") next = tabs[tabs.length - 1];
      if (next) {
        event.preventDefault();
        selectTab(next, true);
      }
    });
  }

  /* Copy buttons ---------------------------------------------------------- */

  const clipboardTimers = new WeakMap();

  async function writeClipboard(text) {
    if (navigator.clipboard && window.isSecureContext) {
      try {
        await navigator.clipboard.writeText(text);
        return true;
      } catch {
        // Fall through to the legacy path.
      }
    }

    try {
      const helper = document.createElement("textarea");
      helper.value = text;
      helper.setAttribute("readonly", "");
      helper.style.position = "fixed";
      helper.style.top = "-1000px";
      helper.style.opacity = "0";
      document.body.appendChild(helper);
      helper.select();
      const copied = document.execCommand("copy");
      helper.remove();
      return copied;
    } catch {
      return false;
    }
  }

  function copyTextFor(button) {
    if (button.dataset.copyText) return button.dataset.copyText;
    if (button.dataset.copyPanel) {
      const panel = document.getElementById(button.dataset.copyPanel);
      const code = panel ? panel.querySelector("code") : null;
      if (code) return code.textContent.trim();
    }
    return "";
  }

  async function handleCopy(button) {
    const text = copyTextFor(button);
    if (!text) return;

    const label = button.querySelector(".copy-label");
    const originalLabel = label ? label.textContent : "";
    const originalAria = button.getAttribute("aria-label") || "";
    const status = button.parentElement
      ? button.parentElement.querySelector("[data-copy-status]")
      : null;

    const copied = await writeClipboard(text);
    clearTimeout(clipboardTimers.get(button));

    if (copied) {
      button.classList.add("is-copied");
      if (label) label.textContent = "Copied";
      button.setAttribute("aria-label", "Copied to clipboard");
      if (status) status.textContent = "Copied to clipboard.";
    } else {
      if (status) status.textContent = "Copy failed. Select the text and copy it manually.";
    }

    const timer = window.setTimeout(() => {
      button.classList.remove("is-copied");
      if (label) label.textContent = originalLabel;
      button.setAttribute("aria-label", originalAria);
      if (status) status.textContent = "";
    }, 1700);
    clipboardTimers.set(button, timer);
  }

  for (const button of document.querySelectorAll("[data-copy-text], [data-copy-panel]")) {
    button.addEventListener("click", () => handleCopy(button));
  }

  /* First paint ----------------------------------------------------------- */

  if (board && !board.dataset.transport) {
    board.dataset.transport = "tcp";
  }
})();
