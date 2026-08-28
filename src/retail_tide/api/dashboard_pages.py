from __future__ import annotations

PAGES = {"overview", "trends", "posts", "research"}


def dashboard_html(page: str = "overview") -> str:
    """Return a focused dependency-free dashboard page."""

    selected = page if page in PAGES else "overview"
    titles = {
        "overview": "市场总览",
        "trends": "趋势与价格",
        "posts": "历史帖子",
        "research": "研究与溯源",
    }
    return _HTML.replace("__PAGE__", selected).replace("__PAGE_TITLE__", titles[selected])


_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__PAGE_TITLE__ · RetailTide 散户潮汐</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #09111f;
      --panel: #111d31;
      --panel-2: #16253e;
      --line: #2a3b59;
      --text: #edf4ff;
      --muted: #91a4c0;
      --cyan: #59d9f5;
      --green: #67e8a1;
      --rose: #fb7185;
      --amber: #fbbf24;
      --violet: #a78bfa;
      --shadow: 0 16px 42px rgba(0, 0, 0, .22);
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      min-width: 320px;
      background: radial-gradient(circle at 7% -4%, #183456 0, transparent 34rem), var(--bg);
      color: var(--text);
      font: 14px/1.55 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
        "Segoe UI", sans-serif;
    }
    a { color: inherit; }
    button, select, input {
      border: 1px solid var(--line);
      border-radius: 9px;
      background: var(--panel-2);
      color: var(--text);
      font: inherit;
    }
    button { cursor: pointer; padding: 8px 12px; }
    button:hover, select:hover, input:hover { border-color: var(--cyan); }
    button:focus-visible, select:focus-visible, input:focus-visible, a:focus-visible {
      outline: 2px solid var(--cyan);
      outline-offset: 2px;
    }
    button:disabled { cursor: not-allowed; opacity: .45; }
    select { min-width: 160px; padding: 8px 30px 8px 10px; }
    input[type="date"] { min-width: 148px; padding: 8px 10px; color-scheme: dark; }
    h1, h2, h3, p { margin-top: 0; }
    h1 { margin-bottom: 8px; font-size: clamp(28px, 4vw, 44px); letter-spacing: -.045em; }
    h2 { margin-bottom: 5px; font-size: 20px; letter-spacing: -.025em; }
    h3 { margin-bottom: 5px; font-size: 15px; }
    .muted, .hint { color: var(--muted); }
    .eyebrow { margin: 0 0 5px; color: var(--cyan); font-size: 10px; letter-spacing: .17em; }
    .site-header {
      position: sticky;
      z-index: 30;
      top: 0;
      border-bottom: 1px solid rgba(42, 59, 89, .85);
      background: rgba(9, 17, 31, .92);
      backdrop-filter: blur(16px);
    }
    .header-inner {
      display: flex;
      width: min(1480px, calc(100% - 40px));
      min-height: 68px;
      margin: 0 auto;
      align-items: center;
      justify-content: space-between;
      gap: 24px;
    }
    .brand { display: flex; align-items: baseline; gap: 10px; text-decoration: none; }
    .brand strong { font-size: 18px; letter-spacing: -.02em; }
    .brand span { color: var(--muted); font-size: 11px; }
    .nav { display: flex; gap: 6px; }
    .nav a {
      border-radius: 8px;
      padding: 8px 11px;
      color: var(--muted);
      text-decoration: none;
      white-space: nowrap;
    }
    .nav a:hover { color: var(--text); background: rgba(89, 217, 245, .07); }
    body[data-page="overview"] [data-nav="overview"],
    body[data-page="trends"] [data-nav="trends"],
    body[data-page="posts"] [data-nav="posts"],
    body[data-page="research"] [data-nav="research"] {
      color: var(--cyan);
      background: rgba(89, 217, 245, .11);
    }
    .shell { width: min(1480px, calc(100% - 40px)); margin: 0 auto; padding: 30px 0 60px; }
    .page-head {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 24px;
      margin-bottom: 18px;
    }
    .page-head p:last-child { max-width: 760px; margin-bottom: 0; }
    .toolbar { display: flex; align-items: end; gap: 9px; flex-wrap: wrap; }
    label.control { display: grid; gap: 4px; color: var(--muted); font-size: 11px; }
    .notice {
      margin-bottom: 16px;
      border: 1px solid var(--line);
      border-radius: 11px;
      padding: 10px 13px;
      color: var(--muted);
      background: rgba(17, 29, 49, .7);
    }
    .notice.error { border-color: #9f4259; color: #fecdd3; background: rgba(127, 29, 52, .2); }
    .day-coverage {
      display: grid;
      grid-template-columns: minmax(240px, 1.2fr) minmax(0, 2fr);
      gap: 18px;
      margin-bottom: 16px;
      border-left: 3px solid var(--coverage-color, var(--amber));
    }
    .day-coverage h2 { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .day-status { border-radius: 999px; padding: 2px 8px; color: var(--coverage-color); background: color-mix(in srgb, var(--coverage-color) 11%, transparent); font-size: 10px; }
    .coverage-facts { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }
    .coverage-fact { border: 1px solid rgba(42, 59, 89, .8); border-radius: 9px; padding: 9px 10px; background: rgba(9, 17, 31, .32); }
    .coverage-fact strong, .coverage-fact small { display: block; }
    .coverage-fact strong { font-size: 17px; }
    .coverage-fact small { color: var(--muted); font-size: 10px; }
    .source-coverage { display: flex; gap: 6px; margin-top: 9px; flex-wrap: wrap; }
    .source-coverage .coverage-chip[data-status="complete"] { border-color: rgba(103, 232, 161, .42); color: var(--green); }
    .source-coverage .coverage-chip[data-status="degraded"],
    .source-coverage .coverage-chip[data-status="partial"],
    .source-coverage .coverage-chip[data-status="window_partial"] { border-color: rgba(251, 191, 36, .42); color: var(--amber); }
    .panel, .track-card, .signal-card, .content-card {
      border: 1px solid var(--line);
      background: linear-gradient(145deg, rgba(22, 37, 62, .96), rgba(13, 23, 40, .96));
      box-shadow: var(--shadow);
    }
    .panel { margin-bottom: 18px; border-radius: 15px; padding: 18px; }
    .panel-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 13px;
    }
    .panel-head p { margin-bottom: 0; }
    .market-layout { display: grid; grid-template-columns: 350px minmax(0, 1fr); gap: 16px; }
    .market-reading { display: flex; min-height: 336px; flex-direction: column; justify-content: space-between; }
    .heat-number { font-size: 74px; font-weight: 850; line-height: .96; letter-spacing: -.065em; }
    .status { margin: 11px 0 4px; font-size: 18px; font-weight: 720; }
    .window-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 18px; }
    .window-cell { border: 1px solid rgba(42, 59, 89, .8); border-radius: 9px; padding: 9px 10px; background: rgba(9, 17, 31, .34); }
    .window-cell strong { display: block; font-size: 17px; }
    .window-cell small { color: var(--muted); }
    .market-facts { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; margin-top: 12px; }
    .fact { border-radius: 9px; padding: 9px; background: rgba(9, 17, 31, .38); }
    .fact strong { display: block; font-size: 17px; }
    .fact small { color: var(--muted); }
    .section-head { display: flex; align-items: end; justify-content: space-between; gap: 16px; margin: 26px 2px 12px; }
    .section-head p { margin-bottom: 0; }
    .track-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
    .track-card { position: relative; overflow: hidden; border-radius: 13px; padding: 16px; }
    .track-card::before { content: ""; position: absolute; inset: 0 0 auto; height: 3px; background: var(--track); }
    .track-card-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
    .track-card-head strong { font-size: 16px; }
    .heat-pill { border-radius: 999px; padding: 2px 8px; color: var(--track); background: color-mix(in srgb, var(--track) 12%, transparent); font-size: 10px; }
    .track-score { margin: 14px 0 4px; color: var(--track); font-size: 43px; font-weight: 820; line-height: 1; letter-spacing: -.055em; }
    .track-score small { margin-left: 5px; color: var(--muted); font-size: 10px; font-weight: 450; letter-spacing: 0; }
    .trend-note { min-height: 38px; margin-bottom: 12px; color: var(--muted); font-size: 11px; }
    .mini-signals { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
    .mini-signal { border-radius: 8px; padding: 7px 8px; background: rgba(9, 17, 31, .38); }
    .mini-signal strong { display: block; font-size: 14px; }
    .mini-signal small { color: var(--muted); font-size: 10px; }
    .track-links { display: flex; gap: 7px; margin-top: 12px; }
    .track-links a { flex: 1; border: 1px solid var(--line); border-radius: 8px; padding: 6px 7px; color: var(--muted); text-align: center; text-decoration: none; font-size: 11px; }
    .track-links a:hover { border-color: var(--track); color: var(--track); }
    .rank-layout { display: grid; grid-template-columns: 250px minmax(0, 1fr); gap: 14px; }
    .signal-tabs { display: grid; gap: 8px; }
    .signal-tabs button { text-align: left; color: var(--muted); }
    .signal-tabs button.active { border-color: var(--tab); color: var(--tab); background: color-mix(in srgb, var(--tab) 10%, var(--panel-2)); }
    .ranking-list { display: grid; }
    .rank-row { display: grid; grid-template-columns: 28px minmax(0, 1fr) auto; align-items: center; gap: 10px; border-bottom: 1px solid rgba(42, 59, 89, .72); padding: 10px 8px; text-decoration: none; }
    .rank-row:last-child { border-bottom: 0; }
    .rank-row:hover { background: rgba(89, 217, 245, .04); }
    .rank-no { display: grid; width: 24px; height: 24px; place-items: center; border-radius: 50%; color: var(--muted); background: rgba(145, 164, 192, .1); font-size: 11px; }
    .rank-row strong, .rank-row small { display: block; }
    .rank-row small { color: var(--muted); }
    .rank-value { color: var(--rank); text-align: right; font-size: 16px; }
    .rank-value small { font-size: 10px; font-weight: 400; }
    .chart-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
    .chart-grid.single { grid-template-columns: minmax(0, 1fr); }
    .chart-card { --chart-color: var(--cyan); overflow: hidden; }
    .chart-card .panel-head { margin-bottom: 6px; }
    .chart-title-line { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .chart-summary { color: var(--muted); font-size: 11px; }
    .chart-meta { display: flex; min-height: 23px; align-items: center; gap: 12px; flex-wrap: wrap; color: var(--muted); font-size: 11px; }
    .legend-dot::before { content: ""; display: inline-block; width: 9px; height: 9px; margin-right: 5px; border-radius: 50%; background: var(--legend); }
    .legend-line::before { content: ""; display: inline-block; width: 15px; margin: 0 5px 3px 0; border-top: 2px dashed var(--legend); }
    .legend-gap::before { content: ""; display: inline-block; width: 15px; margin: 0 5px 3px 0; border-top: 2px dashed var(--legend); opacity: .65; }
    .asset-select { min-width: 145px; padding-top: 5px; padding-bottom: 5px; font-size: 11px; }
    .coverage-strip { display: flex; gap: 6px; margin-top: 7px; flex-wrap: wrap; }
    .coverage-chip { border: 1px solid rgba(42, 59, 89, .8); border-radius: 999px; padding: 2px 8px; color: #c5d4e9; background: rgba(9, 17, 31, .35); font-size: 10px; }
    .coverage-note { margin: 6px 0 0; color: var(--muted); font-size: 10px; }
    .chart-shell { position: relative; min-height: 286px; margin-top: 4px; }
    .chart-svg { display: block; width: 100%; height: 286px; }
    .chart-tooltip {
      position: absolute;
      z-index: 5;
      top: 12px;
      left: 12px;
      min-width: 178px;
      border: 1px solid #3c5277;
      border-radius: 9px;
      padding: 9px 10px;
      background: rgba(8, 15, 27, .95);
      box-shadow: 0 12px 28px rgba(0, 0, 0, .3);
      pointer-events: none;
      font-size: 11px;
    }
    .chart-tooltip[hidden] { display: none; }
    .chart-tooltip strong { display: block; margin-bottom: 4px; font-size: 12px; }
    .tip-row { display: flex; justify-content: space-between; gap: 15px; color: var(--muted); }
    .tip-row b { color: var(--text); font-weight: 650; }
    .hover-band { cursor: crosshair; pointer-events: all; }
    .chart-empty { display: grid; min-height: 260px; place-items: center; color: var(--muted); text-align: center; }
    .post-controls {
      position: sticky;
      z-index: 20;
      top: 78px;
      margin-bottom: 16px;
      border: 1px solid var(--line);
      border-radius: 13px;
      padding: 13px;
      background: rgba(13, 23, 40, .95);
      backdrop-filter: blur(16px);
      box-shadow: var(--shadow);
    }
    .post-control-row { display: flex; align-items: end; gap: 9px; flex-wrap: wrap; }
    .filter-buttons { display: flex; gap: 6px; flex-wrap: wrap; }
    .filter-buttons button { padding: 7px 10px; color: var(--muted); font-size: 11px; }
    .filter-buttons button.active { border-color: var(--cyan); color: var(--cyan); background: rgba(89, 217, 245, .09); }
    .post-summary { display: flex; align-items: center; justify-content: space-between; gap: 14px; margin: 4px 2px 12px; }
    .facet-list { display: flex; gap: 6px; flex-wrap: wrap; }
    .facet { border-radius: 999px; padding: 3px 8px; color: var(--muted); background: rgba(145, 164, 192, .09); font-size: 10px; }
    .content-list { display: grid; gap: 11px; }
    .content-card { border-radius: 13px; padding: 15px 16px; }
    .content-meta, .content-tags, .content-stats { display: flex; align-items: center; gap: 7px; flex-wrap: wrap; }
    .content-meta { justify-content: space-between; color: var(--muted); font-size: 11px; }
    .source-line { display: flex; align-items: center; gap: 7px; flex-wrap: wrap; }
    .source-badge { display: inline-flex; align-items: center; border: 1px solid rgba(145, 164, 192, .35); border-radius: 999px; padding: 3px 9px; color: #dce8f8; background: rgba(145, 164, 192, .1); font-weight: 720; }
    .source-badge[data-source="xiaohongshu"] { border-color: rgba(251, 113, 133, .55); color: #fecdd3; background: rgba(251, 113, 133, .14); }
    .source-badge[data-source="guba"] { border-color: rgba(251, 191, 36, .5); color: #fde68a; background: rgba(251, 191, 36, .12); }
    .source-badge[data-source="taoguba"] { border-color: rgba(89, 217, 245, .5); color: #a5f3fc; background: rgba(89, 217, 245, .12); }
    .source-badge[data-source="zhihu"] { border-color: rgba(124, 156, 255, .5); color: #c7d2fe; background: rgba(124, 156, 255, .12); }
    .content-card h3 { margin: 10px 0 7px; font-size: 16px; line-height: 1.45; }
    .content-body { color: #d8e5f5; white-space: pre-wrap; word-break: break-word; }
    .intent-review { margin-top: 10px; color: #b9c9df; font-size: 13px; }
    .intent-review summary { cursor: pointer; color: #8fadd2; }
    .intent-review p { margin: 7px 0 0; line-height: 1.65; }
    .intent-evidence { color: #f5d48a; }
    details.content-more { border: 0; padding: 0; background: transparent; }
    details.content-more summary { color: var(--cyan); cursor: pointer; font-size: 11px; }
    .content-tags { margin-top: 10px; }
    .content-stats { margin-top: 9px; color: var(--muted); font-size: 11px; }
    .tag { display: inline-flex; border-radius: 999px; padding: 2px 7px; color: var(--cyan); background: rgba(89, 217, 245, .1); font-size: 10px; }
    .tag.buy { color: var(--green); background: rgba(103, 232, 161, .1); }
    .tag.sell, .tag.panic { color: var(--rose); background: rgba(251, 113, 133, .11); }
    .tag.fomo { color: var(--amber); background: rgba(251, 191, 36, .11); }
    .external { color: var(--cyan); text-decoration: none; }
    .external:hover { text-decoration: underline; }
    .load-more { display: block; margin: 16px auto 0; }
    .load-more[hidden] { display: none; }
    .empty { padding: 24px 4px; color: var(--muted); text-align: center; }
    .research-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
    .research-status { margin-bottom: 10px; border: 1px solid rgba(89, 217, 245, .25); border-radius: 9px; padding: 9px 10px; color: #c9d8ef; background: rgba(89, 217, 245, .055); font-size: 11px; }
    .research-status strong { display: block; margin-bottom: 2px; color: var(--text); }
    .research-status.waiting { border-color: rgba(251, 191, 36, .28); background: rgba(251, 191, 36, .055); }
    .research-status.empty-state { border-color: rgba(145, 164, 192, .28); background: rgba(145, 164, 192, .045); }
    .replay-lead { margin: 8px 0 13px; color: #e8f1ff; font-size: 15px; line-height: 1.55; }
    .replay-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }
    .replay-cell { border: 1px solid rgba(42, 59, 89, .8); border-radius: 9px; padding: 10px; background: rgba(9, 17, 31, .34); }
    .replay-cell strong { display: block; font-size: 18px; }
    .replay-cell small { color: var(--muted); }
    .table-wrap { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid rgba(42, 59, 89, .7); padding: 9px 8px; text-align: left; vertical-align: top; white-space: nowrap; }
    th { color: var(--muted); font-size: 10px; font-weight: 600; letter-spacing: .05em; text-transform: uppercase; }
    tr:last-child td { border-bottom: 0; }
    .event-button { border: 0; padding: 0; color: var(--cyan); background: transparent; }
    .raw-detail { margin-top: 12px; border: 1px solid var(--line); border-radius: 10px; padding: 12px; background: rgba(9, 17, 31, .38); }
    pre { max-height: 360px; overflow: auto; color: #c9d8ef; white-space: pre-wrap; word-break: break-word; font-size: 10px; }
    .footer { margin-top: 28px; color: var(--muted); font-size: 11px; }
    @media (max-width: 1120px) {
      .track-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .market-layout { grid-template-columns: 300px minmax(0, 1fr); }
    }
    @media (max-width: 850px) {
      .header-inner, .page-head { align-items: flex-start; flex-direction: column; }
      .header-inner { gap: 5px; padding: 10px 0; }
      .nav { width: 100%; overflow-x: auto; }
      .market-layout, .rank-layout, .research-grid, .day-coverage { grid-template-columns: 1fr; }
      .replay-grid { grid-template-columns: 1fr 1fr; }
      .chart-grid { grid-template-columns: 1fr; }
      .track-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .post-controls { top: 116px; }
    }
    @media (max-width: 540px) {
      .header-inner, .shell { width: min(100% - 24px, 1480px); }
      .shell { padding-top: 20px; }
      .brand span { display: none; }
      .track-grid { grid-template-columns: 1fr; }
      .market-facts { grid-template-columns: 1fr 1fr; }
      .coverage-facts { grid-template-columns: 1fr 1fr; }
      .post-summary { align-items: flex-start; flex-direction: column; }
      .chart-svg { height: 255px; }
    }
  </style>
</head>
<body data-page="__PAGE__">
  <header class="site-header">
    <div class="header-inner">
      <a class="brand" href="/dashboard"><strong>散户潮汐</strong><span>RETAILTIDE OBSERVATORY</span></a>
      <nav class="nav" aria-label="主导航">
        <a data-nav="overview" href="/dashboard">市场总览</a>
        <a data-nav="trends" href="/trends">趋势与价格</a>
        <a data-nav="posts" href="/posts">历史帖子</a>
        <a data-nav="research" href="/research">研究与溯源</a>
      </nav>
    </div>
  </header>
  <main id="main" class="shell"><section class="notice">正在加载数据…</section></main>
  <script>
    const PAGE = document.body.dataset.page;
    const state = {
      topics: [], overview: null, configuration: null, rankMetric: "buy",
      overviewDate: new URLSearchParams(location.search).get("date") || "",
      trendTopic: new URLSearchParams(location.search).get("topic") || "all",
      trendFrom: new URLSearchParams(location.search).get("from") || "",
      trendTo: new URLSearchParams(location.search).get("to") || "",
      assetChoice: {}, posts: null, loadingPosts: false,
      postTopic: new URLSearchParams(location.search).get("topic") || "all",
      postFilter: new URLSearchParams(location.search).get("filter") || "all",
      postSource: new URLSearchParams(location.search).get("source") || "all",
      postDate: new URLSearchParams(location.search).get("date") || "",
      postPeriod: new URLSearchParams(location.search).get("period") || (new URLSearchParams(location.search).get("date") ? "custom" : "7d"),
      attentionSignals: [],
      researchTopic: new URLSearchParams(location.search).get("topic") || "gold",
      researchEvent: new URLSearchParams(location.search).get("event") || "",
      researchMetric: new URLSearchParams(location.search).get("metric") || "",
      researchHorizon: new URLSearchParams(location.search).get("horizon") || "1d"
    };
    const $ = (selector, root = document) => root.querySelector(selector);
    const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
    const esc = (value) => String(value ?? "").replace(/[&<>\"']/g, (char) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;"
    }[char]));
    const num = (value, digits = 0) => value == null || Number.isNaN(Number(value))
      ? "—" : new Intl.NumberFormat("zh-CN", { maximumFractionDigits: digits }).format(Number(value));
    const pct = (value, digits = 1) => value == null ? "—" : `${num(Number(value) * 100, digits)}%`;
    const heat = (value) => value == null ? "—" : num(value, 1);
    const percentile = (value) => value == null ? "基线预热" : `历史分位 ${num(value, 1)}`;
    const signed = (value) => value == null ? "—" : `${Number(value) > 0 ? "+" : ""}${num(value, 1)}`;
    const money = (value, currency) => value == null ? "—" : `${currency === "CNY" ? "¥" : currency === "USD" ? "$" : ""}${num(value, value < 10 ? 3 : 2)}`;
    const dateText = (value, withTime = true) => {
      if (!value) return "—";
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString("zh-CN", {
        timeZone: "Asia/Shanghai", month: "2-digit", day: "2-digit",
        hour: withTime ? "2-digit" : undefined, minute: withTime ? "2-digit" : undefined,
        year: withTime ? undefined : "numeric", hour12: false
      });
    };
    const dateKey = (value) => {
      const parts = new Intl.DateTimeFormat("en", {
        timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit"
      }).formatToParts(new Date(value));
      const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
      return `${values.year}-${values.month}-${values.day}`;
    };
    const shiftDateKey = (value, days) => dateKey(new Date(new Date(`${value}T12:00:00+08:00`).getTime() + Number(days) * 86400000));
    const shanghaiToday = () => dateKey(new Date());
    const calendarDayBounds = (value) => {
      const start = new Date(`${value}T00:00:00+08:00`);
      const end = new Date(start.getTime() + 86400000 - 1);
      return { from_at: start.toISOString(), to_at: end.toISOString() };
    };
    const selectedDayPostsUrl = (topic = "", filter = "all") => {
      const params = new URLSearchParams({ period: "custom", date: state.overview?.selected_date || state.overviewDate });
      if (topic) params.set("topic", topic);
      if (filter && filter !== "all") params.set("filter", filter);
      return `/posts?${params.toString()}`;
    };
    const sourceLabel = (name) => ({ guba: "东方股吧", taoguba: "淘股吧", zhihu: "知乎", xiaohongshu: "小红书", "common-crawl": "Common Crawl 归档", "wikimedia-pageviews": "Wikimedia 浏览量" }[name] || name || "未知来源");
    const postSourceNames = ["all", "xiaohongshu", "guba", "taoguba", "zhihu"];
    const signalLabel = (name) => ({ all: "全部", retail: "散户", buy: "买入/偏买", sell: "卖出/偏卖", hold: "持有", wait: "等待观察", fomo: "FOMO 追涨", panic: "恐慌", promotion: "广告/推广" }[name] || name);
    const api = async (path, params = {}) => {
      const url = new URL(path, location.origin);
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, value);
      });
      const response = await fetch(url);
      if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
      return response.json();
    };
    const optional = async (path, params, fallback) => {
      try { return await api(path, params); } catch (_error) { return fallback; }
    };
    const setNotice = (message, error = false) => {
      const node = $("#notice");
      if (!node) return;
      node.textContent = message;
      node.classList.toggle("error", error);
    };
    const profileFor = (score) => {
      if (score == null) return { label: "暂无当日指数", color: "#91a4c0" };
      if (score >= 80) return { label: "极热", color: "#fb7185" };
      if (score >= 60) return { label: "偏热", color: "#fbbf24" };
      if (score >= 40) return { label: "正常", color: "#67e8a1" };
      if (score >= 20) return { label: "偏冷", color: "#59d9f5" };
      return { label: "极冷", color: "#7c9cff" };
    };
    const trendFor = (summary) => {
      const direction = summary?.direction;
      const map = {
        accelerating: ["↗", "快速升温", "#fb7185"], rising: ["↗", "温和升温", "#fbbf24"],
        stable: ["→", "震荡持平", "#67e8a1"], cooling: ["↘", "温和降温", "#59d9f5"],
        cooling_fast: ["↘", "快速降温", "#7c9cff"], insufficient: ["·", "趋势待积累", "#91a4c0"]
      };
      const item = map[direction] || map.insufficient;
      return { icon: item[0], label: summary?.label || item[1], color: item[2] };
    };
    const pageHead = (eyebrow, title, description, controls = "") => `
      <header class="page-head"><div><p class="eyebrow">${esc(eyebrow)}</p><h1>${esc(title)}</h1><p class="hint">${esc(description)}</p></div>${controls ? `<div class="toolbar">${controls}</div>` : ""}</header>
      <section id="notice" class="notice">正在加载数据…</section>`;
    const topicOptions = (selected, includeAll = false) => `${includeAll ? `<option value="all"${selected === "all" ? " selected" : ""}>全部赛道</option>` : ""}${state.topics.map((topic) => `<option value="${esc(topic.slug)}"${topic.slug === selected ? " selected" : ""}>${esc(topic.name)}</option>`).join("")}`;
    const updateUrl = (values) => {
      const url = new URL(location.href);
      Object.entries(values).forEach(([key, value]) => value ? url.searchParams.set(key, value) : url.searchParams.delete(key));
      history.replaceState({}, "", url);
    };

    function buildChart(row, asset = null, compact = false) {
      const history = [...(row?.history || [])].sort((a, b) => new Date(a.bucket_at) - new Date(b.bucket_at));
      const wikimedia = [...(row?.wikimedia_history || [])].sort((a, b) => new Date(a.bucket_at) - new Date(b.bucket_at));
      const prices = [...(asset?.price_history || [])].filter((item) => item.close != null).sort((a, b) => new Date(a.ts) - new Date(b.ts));
      const byDate = new Map();
      history.forEach((item) => byDate.set(dateKey(item.bucket_at), { date: dateKey(item.bucket_at), history: item, wiki: null, price: null }));
      wikimedia.forEach((item) => {
        const key = dateKey(item.bucket_at);
        const point = byDate.get(key) || { date: key, history: null, wiki: null, price: null };
        point.wiki = item;
        byDate.set(key, point);
      });
      prices.forEach((item) => {
        const key = dateKey(item.ts);
        const point = byDate.get(key) || { date: key, history: null, wiki: null, price: null };
        point.price = item;
        byDate.set(key, point);
      });
      const points = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
      if (!points.length) return `<div class="chart-empty">暂无趋势数据</div>`;
      const width = 920, height = compact ? 270 : 300;
      const pad = { left: 50, right: asset?.has_price_data ? 62 : 24, top: 18, bottom: 32 };
      const plotW = width - pad.left - pad.right, plotH = height - pad.top - pad.bottom;
      const x = (index) => pad.left + (points.length === 1 ? plotW / 2 : index * plotW / (points.length - 1));
      const heatY = (value) => pad.top + plotH - Math.max(0, Math.min(100, Number(value))) / 100 * plotH;
      const wikiValues = wikimedia.map((item) => Number(item.value)).filter((value) => Number.isFinite(value));
      const wikiMin = wikiValues.length ? Math.min(...wikiValues) : 0, wikiMax = wikiValues.length ? Math.max(...wikiValues) : 1;
      const wikiY = (value) => heatY(wikiMax === wikiMin ? 50 : (Number(value) - wikiMin) / (wikiMax - wikiMin) * 100);
      const priceValues = prices.map((item) => Number(item.close));
      let priceMin = priceValues.length ? Math.min(...priceValues) : 0;
      let priceMax = priceValues.length ? Math.max(...priceValues) : 1;
      if (priceMax === priceMin) { const delta = Math.max(Math.abs(priceMax) * .02, .01); priceMin -= delta; priceMax += delta; }
      const priceY = (value) => pad.top + plotH - (Number(value) - priceMin) / (priceMax - priceMin) * plotH;
      const path = (items, value, y) => items.map((item, index) => `${index ? "L" : "M"}${x(item.index).toFixed(1)},${y(value(item.point)).toFixed(1)}`).join(" ");
      const heatItems = points.map((point, index) => ({ point, index })).filter(({ point }) => point.history?.heat_index != null);
      const wikiItems = points.map((point, index) => ({ point, index })).filter(({ point }) => point.wiki?.value != null);
      const priceItems = points.map((point, index) => ({ point, index })).filter(({ point }) => point.price?.close != null);
      const hoverH = plotH;
      const pathSegments = (items, value, y) => {
        const segments = []; let current = [];
        items.forEach((item) => {
          if (current.length && item.index !== current.at(-1).index + 1) { segments.push(current); current = []; }
          current.push(item);
        });
        if (current.length) segments.push(current);
        return segments.filter((segment) => segment.length > 1).map((segment) => `<path d="${path(segment, value, y)}" fill="none" stroke="var(--chart-color)" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>`).join("");
      };
      const heatPath = pathSegments(heatItems, (point) => point.history.heat_index, heatY);
      const wikiPath = wikiItems.length > 1 ? `<path d="${path(wikiItems, (point) => point.wiki.value, wikiY)}" fill="none" stroke="#a78bfa" stroke-width="2.4" stroke-dasharray="4 4" stroke-linecap="round" stroke-linejoin="round"/>` : "";
      const heatBridges = heatItems.slice(1).map((item, index) => {
        const previous = heatItems[index];
        if (item.index === previous.index + 1) return "";
        return `<path d="${path([previous, item], (point) => point.history.heat_index, heatY)}" fill="none" stroke="var(--chart-color)" stroke-width="2" stroke-dasharray="5 5" stroke-linecap="round" opacity=".6"/>`;
      }).join("");
      const pricePath = priceItems.length > 1 ? `<path d="${path(priceItems, (point) => point.price.close, priceY)}" fill="none" stroke="#dbe7f7" stroke-width="2" stroke-dasharray="6 5" stroke-linecap="round" stroke-linejoin="round"/>` : "";
      const heatDots = heatItems.map(({ point, index }) => `<circle cx="${x(index)}" cy="${heatY(point.history.heat_index)}" r="3.2" fill="#111d31" stroke="var(--chart-color)" stroke-width="2"/>`).join("");
      const wikiDots = wikiItems.map(({ point, index }) => `<circle cx="${x(index)}" cy="${wikiY(point.wiki.value)}" r="2.8" fill="#111d31" stroke="#a78bfa" stroke-width="1.8"/>`).join("");
      const grid = [0, 25, 50, 75, 100].map((value) => {
        const y = heatY(value);
        return `<line x1="${pad.left}" y1="${y}" x2="${width - pad.right}" y2="${y}" stroke="#2a3b59" stroke-width="1"/><text x="8" y="${y + 4}" fill="#91a4c0" font-size="10">${value}</text>`;
      }).join("");
      const labelIndexes = [0, Math.floor((points.length - 1) / 2), points.length - 1].filter((value, index, all) => all.indexOf(value) === index);
      const labels = labelIndexes.map((index) => `<text x="${x(index)}" y="${height - 8}" fill="#91a4c0" font-size="10" text-anchor="${index === 0 ? "start" : index === points.length - 1 ? "end" : "middle"}">${esc(points[index].date.slice(5))}</text>`).join("");
      const priceAxis = prices.length ? `<text x="${width - 4}" y="${pad.top + 4}" fill="#dbe7f7" font-size="10" text-anchor="end">${money(priceMax, asset.currency)}</text><text x="${width - 4}" y="${pad.top + plotH}" fill="#dbe7f7" font-size="10" text-anchor="end">${money(priceMin, asset.currency)}</text>` : "";
      const bands = points.map((point, index) => {
        const left = index === 0 ? pad.left : (x(index - 1) + x(index)) / 2;
        const right = index === points.length - 1 ? width - pad.right : (x(index) + x(index + 1)) / 2;
        const tip = encodeURIComponent(JSON.stringify({
          date: point.date,
          heat: point.history?.heat_index ?? null,
          percentile: point.history?.historical_percentile ?? null,
          confidence: point.history?.daily_index_confidence?.label ?? null,
          retail: point.history?.retail_count ?? null,
          attention: point.history?.attention ?? null,
          fomo: point.history?.fomo_count ?? null,
          panic: point.history?.panic_count ?? null,
          analyzed: point.history?.analyzed_count ?? null,
          baseline: point.history?.baseline_sample_days ?? null,
          heatStatus: point.history?.heat_status ?? "unobserved",
          hasObservation: Boolean(point.history),
          price: point.price?.close ?? null,
          asset: asset?.name ?? null,
          currency: asset?.currency ?? null,
          provider: point.price?.provider ?? null,
          wikiValue: point.wiki?.value ?? null,
          wikiChange: point.wiki?.change_ratio ?? null,
          wikiPercentile: point.wiki?.percentile ?? null,
          wikiKeyword: point.wiki?.keyword ?? null,
          wikiKeywords: point.wiki?.keywords ?? []
        }));
        return `<rect class="hover-band" x="${left}" y="${pad.top}" width="${Math.max(2, right - left)}" height="${hoverH}" fill="transparent" data-x="${x(index)}" data-heat-y="${point.history?.heat_index == null ? "" : heatY(point.history.heat_index)}" data-wiki-y="${point.wiki?.value == null ? "" : wikiY(point.wiki.value)}" data-price-y="${point.price?.close == null ? "" : priceY(point.price.close)}" data-tip="${tip}"/>`;
      }).join("");
      return `<div class="chart-shell"><svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="自然日散户情绪热度、Wikimedia关注分位与代表资产价格走势">${grid}${heatBridges}${heatPath}${wikiPath}${pricePath}${heatDots}${wikiDots}${priceAxis}${labels}<line class="crosshair" x1="0" x2="0" y1="${pad.top}" y2="${pad.top + hoverH}" stroke="#91a4c0" stroke-width="1" stroke-dasharray="3 3" visibility="hidden"/><circle class="focus-heat" r="5" fill="var(--chart-color)" stroke="#09111f" stroke-width="2" visibility="hidden"/><circle class="focus-wiki" r="4" fill="#a78bfa" stroke="#09111f" stroke-width="2" visibility="hidden"/><circle class="focus-price" r="4" fill="#dbe7f7" stroke="#09111f" stroke-width="2" visibility="hidden"/>${bands}</svg><div class="chart-tooltip" hidden></div></div>`;
    }

    function attachChartHover() {
      $$(".chart-shell").forEach((shell) => {
        if (shell.dataset.hoverReady === "1") return;
        shell.dataset.hoverReady = "1";
        const tooltip = $(".chart-tooltip", shell);
        const crosshair = $(".crosshair", shell);
        const heatDot = $(".focus-heat", shell);
        const wikiDot = $(".focus-wiki", shell);
        const priceDot = $(".focus-price", shell);
        const svg = $(".chart-svg", shell);
        const bands = $$(".hover-band", shell);
        const viewXFromEvent = (event) => {
          const matrix = svg.getScreenCTM();
          if (matrix) {
            const point = svg.createSVGPoint();
            point.x = event.clientX;
            point.y = event.clientY;
            return point.matrixTransform(matrix.inverse()).x;
          }
          const bounds = svg.getBoundingClientRect();
          return bounds.width ? (event.clientX - bounds.left) * 920 / bounds.width : 0;
        };
        const shellXFromViewX = (viewX) => {
          const matrix = svg.getScreenCTM();
          if (matrix) {
            const point = svg.createSVGPoint();
            point.x = viewX;
            point.y = 0;
            return point.matrixTransform(matrix).x - shell.getBoundingClientRect().left;
          }
          return viewX / 920 * shell.clientWidth;
        };
        const show = (band) => {
          const tip = JSON.parse(decodeURIComponent(band.dataset.tip));
          const x = Number(band.dataset.x);
          crosshair.setAttribute("x1", x); crosshair.setAttribute("x2", x); crosshair.setAttribute("visibility", "visible");
          if (band.dataset.heatY) { heatDot.setAttribute("cx", x); heatDot.setAttribute("cy", band.dataset.heatY); heatDot.setAttribute("visibility", "visible"); }
          else heatDot.setAttribute("visibility", "hidden");
          if (band.dataset.wikiY) { wikiDot.setAttribute("cx", x); wikiDot.setAttribute("cy", band.dataset.wikiY); wikiDot.setAttribute("visibility", "visible"); }
          else wikiDot.setAttribute("visibility", "hidden");
          if (band.dataset.priceY) { priceDot.setAttribute("cx", x); priceDot.setAttribute("cy", band.dataset.priceY); priceDot.setAttribute("visibility", "visible"); }
          else priceDot.setAttribute("visibility", "hidden");
          const sampleStatus = tip.heat != null ? (tip.confidence || "已计算") : tip.hasObservation && Number(tip.analyzed || 0) === 0 ? "帖子尚未完成分析" : "当日无有效帖子样本";
          const percentileStatus = tip.percentile == null ? (tip.heat == null ? "无当日指数" : `未计算（更早有效日 ${num(tip.baseline)} / 5）`) : num(tip.percentile, 1);
          const wikiDetails = (tip.wikiKeywords || []).map((item) => `${esc(item.keyword)} ${num(item.value)} / ${pct(item.percentile)}`).join("；");
          tooltip.innerHTML = `<strong>${esc(tip.date)}</strong><div class="tip-row"><span>当日情绪/热度指数</span><b>${heat(tip.heat)}</b></div><div class="tip-row"><span>历史分位</span><b>${esc(percentileStatus)}</b></div><div class="tip-row"><span>样本状态</span><b>${esc(sampleStatus)}</b></div><div class="tip-row"><span>散户内容 / 全部</span><b>${num(tip.retail)} / ${num(tip.attention)}</b></div><div class="tip-row"><span>FOMO / 恐慌</span><b>${num(tip.fomo)} / ${num(tip.panic)}</b></div>${tip.wikiValue != null ? `<div class="tip-row"><span>Wikimedia 综合</span><b>${num(tip.wikiValue)} · ${pct(tip.wikiChange)} · 分位 ${pct(tip.wikiPercentile)}</b></div><div class="tip-row"><span>组成词条</span><b>${wikiDetails || esc(tip.wikiKeyword || "—")}</b></div>` : ""}${tip.asset ? `<div class="tip-row"><span>${esc(tip.asset)} 收盘</span><b>${money(tip.price, tip.currency)}</b></div>` : ""}${tip.provider ? `<div class="tip-row"><span>行情来源</span><b>${esc(tip.provider)}</b></div>` : ""}`;
          tooltip.hidden = false;
          const shellX = shellXFromViewX(x);
          const relative = shell.clientWidth ? shellX / shell.clientWidth : 0;
          tooltip.style.left = relative > .64 ? "auto" : `${Math.max(10, shellX + 9)}px`;
          tooltip.style.right = relative > .64 ? "10px" : "auto";
        };
        bands.forEach((band) => {
          ["pointerenter", "pointermove", "mouseenter", "mousemove"].forEach((eventName) => band.addEventListener(eventName, (event) => {
            event.stopPropagation();
            show(band);
          }));
        });
        const showNearest = (event) => {
          const bounds = svg.getBoundingClientRect();
          if (!bounds.width || !bands.length) return;
          const viewX = viewXFromEvent(event);
          const nearest = bands.reduce((best, candidate) => Math.abs(Number(candidate.dataset.x) - viewX) < Math.abs(Number(best.dataset.x) - viewX) ? candidate : best);
          show(nearest);
        };
        ["pointermove", "mousemove"].forEach((eventName) => svg.addEventListener(eventName, showNearest));
        const hide = () => {
          tooltip.hidden = true;
          crosshair.setAttribute("visibility", "hidden");
          heatDot.setAttribute("visibility", "hidden");
          wikiDot.setAttribute("visibility", "hidden");
          priceDot.setAttribute("visibility", "hidden");
        };
        ["pointerleave", "mouseleave"].forEach((eventName) => svg.addEventListener(eventName, hide));
      });
    }

    function windowCells(windows) {
      const rows = [["所选自然日", windows?.today], ["前一自然日", windows?.yesterday], ["截至所选日 7日均值", windows?.["7d"]], ["截至所选日 30日均值", windows?.["30d"]]];
      return rows.map(([label, item]) => `<div class="window-cell"><strong>${heat(item?.index)}</strong><small>${esc(label)}</small></div>`).join("");
    }

    function renderDayCoverage() {
      const coverage = state.overview?.coverage || {};
      const market = coverage.market || {};
      const hasContent = Number(coverage.content_count || 0) > 0;
      const label = coverage.is_collecting ? "今日采集中" : hasContent ? "已采集" : "暂无帖子";
      const color = coverage.is_collecting ? "#fbbf24" : hasContent ? "#67e8a1" : "#91a4c0";
      const note = coverage.is_collecting
        ? "今天展示截至当前已落库的阶段性帖子数量。"
        : hasContent
          ? "历史日期仅展示已经落库的帖子数量，不声明各平台的绝对全量。"
          : "该日暂无已落库帖子。";
      const sourceRows = (coverage.sources || []).map((row) => `<span class="coverage-chip">${esc(sourceLabel(row.name))} ${num(row.content_count)} 条</span>`).join("");
      const marketText = Number(market.exact_day_asset_count || 0)
        ? `${num(market.exact_day_asset_count)} / ${num(market.linked_asset_count)}`
        : `0 / ${num(market.linked_asset_count)}`;
      return `<section class="panel day-coverage" style="--coverage-color:${color}"><div><p class="eyebrow">DAILY DATA COVERAGE</p><h2>${esc(coverage.selected_date || state.overview?.selected_date || "指定日期")} <span class="day-status">${esc(label)}</span></h2><p class="hint">${esc(note)}</p><div class="source-coverage">${sourceRows || `<span class="coverage-chip">暂无来源记录</span>`}</div></div><div class="coverage-facts"><div class="coverage-fact"><strong>${num(coverage.content_count)}</strong><small>该日去重内容</small></div><div class="coverage-fact"><strong>${num(coverage.indexed_content_count)}</strong><small>纳入赛道指数</small></div><div class="coverage-fact"><strong>${num(coverage.analyzed_content_count)} / ${num(coverage.indexed_content_count)}</strong><small>已完成分析</small></div><div class="coverage-fact"><strong>${marketText}</strong><small>有当日日线 / 关联资产</small></div></div></section>`;
    }

    function renderMarketOverview() {
      const market = state.overview?.market || {};
      const profile = profileFor(market.heat_score);
      const trend = trendFor(market.trend_summary);
      const cutoff = state.overview?.data_cutoff_at ? `该日最后一条帖子 ${dateText(state.overview.data_cutoff_at)}` : "该日暂无帖子";
      return `<section class="market-layout"><article class="panel market-reading" style="--chart-color:${profile.color}"><div><p class="eyebrow">DAILY RETAIL HEAT</p><div class="heat-number" style="color:${profile.color}">${heat(market.heat_score)}</div><div class="status" style="color:${trend.color}">${trend.icon} ${esc(profile.label)} · ${esc(trend.label)}</div><p class="hint">${esc(percentile(market.historical_percentile))} · ${esc(market.daily_index_confidence?.label || "无已分析样本")}<br>${esc(state.overview?.selected_date || "所选上海自然日")} · ${esc(cutoff)}</p></div><div class="window-grid">${windowCells(market.trend_windows)}</div></article><article class="panel chart-card" style="--chart-color:${profile.color}"><div class="panel-head"><div><p class="eyebrow">HISTORICAL TREND</p><h2>截至所选日的散户情绪/热度趋势</h2></div><div><a class="external" href="${esc(selectedDayPostsUrl())}">该日全部帖子 →</a></div></div>${buildChart(market, null, false)}<div class="market-facts"><div class="fact"><strong>${num(market.attention)}</strong><small>纳入指数内容 · 所选日</small></div><div class="fact"><strong>${num(market.retail_count)}</strong><small>散户内容</small></div><div class="fact"><strong>${num(market.fomo_count)}</strong><small>FOMO</small></div><div class="fact"><strong>${num(market.panic_count)}</strong><small>恐慌</small></div></div></article></section>`;
    }

    function renderTrackCards() {
      const rows = state.overview?.topics || [];
      if (!rows.length) return `<div class="empty">暂无赛道数据。</div>`;
      return `<section class="track-grid">${rows.map((row) => {
        const profile = profileFor(row.heat_score), trend = trendFor(row.trend_summary);
        const asset = selectedAsset(row), bar = asset?.selected_day_bar;
        const priceLine = asset ? `${esc(asset.name)} · ${bar ? `收盘 ${money(bar.close, asset.currency)}` : "当日无日线"}` : "暂无关联资产";
        return `<article class="track-card" style="--track:${profile.color}"><div class="track-card-head"><strong>${esc(row.name)}</strong><span class="heat-pill">${esc(profile.label)}</span></div><div class="track-score">${heat(row.heat_score)}<small>所选日情绪/热度指数</small></div><div class="trend-note" style="color:${trend.color}">${trend.icon} ${esc(trend.label)} · ${esc(percentile(row.historical_percentile))}</div><p class="hint">${priceLine}</p><div class="mini-signals"><div class="mini-signal"><strong style="color:var(--green)">${pct(row.buy_intent_ratio)}</strong><small>买入意图 · ${num(row.buy_intent_count)}</small></div><div class="mini-signal"><strong style="color:var(--rose)">${pct(row.sell_intent_ratio)}</strong><small>卖出意图 · ${num(row.sell_intent_count)}</small></div><div class="mini-signal"><strong style="color:var(--amber)">${num(row.fomo_count)}</strong><small>FOMO · ${pct(row.fomo_ratio)}</small></div><div class="mini-signal"><strong style="color:var(--rose)">${num(row.panic_count)}</strong><small>恐慌 · ${pct(row.panic_ratio)}</small></div></div><div class="track-links"><a href="/trends?topic=${encodeURIComponent(row.slug)}">趋势与价格</a><a href="${esc(selectedDayPostsUrl(row.slug))}">该日帖子</a></div></article>`;
      }).join("")}</section>`;
    }

    const rankConfigs = {
      buy: { label: "散户买入意图", count: "buy_intent_count", ratio: "buy_intent_ratio", color: "#67e8a1", filter: "buy" },
      sell: { label: "散户卖出意图", count: "sell_intent_count", ratio: "sell_intent_ratio", color: "#fb7185", filter: "sell" },
      fomo: { label: "FOMO 追涨", count: "fomo_count", ratio: "fomo_ratio", color: "#fbbf24", filter: "fomo" },
      panic: { label: "恐慌情绪", count: "panic_count", ratio: "panic_ratio", color: "#a78bfa", filter: "panic" }
    };
    function renderRanking() {
      const target = $("#ranking");
      if (!target) return;
      const config = rankConfigs[state.rankMetric];
      const rows = [...(state.overview?.topics || [])].sort((a, b) => Number(b[config.count] || 0) - Number(a[config.count] || 0));
      target.innerHTML = `<div class="rank-layout"><div class="signal-tabs">${Object.entries(rankConfigs).map(([key, item]) => `<button type="button" data-rank="${key}" class="${key === state.rankMetric ? "active" : ""}" style="--tab:${item.color}">${esc(item.label)}</button>`).join("")}</div><div class="panel" style="margin:0;--rank:${config.color}"><div class="panel-head"><div><h2>${esc(state.overview?.selected_date || "所选日")} · ${esc(config.label)}排行</h2></div></div><div class="ranking-list">${rows.map((row, index) => `<a class="rank-row" href="${esc(selectedDayPostsUrl(row.slug, config.filter))}"><span class="rank-no">${index + 1}</span><span><strong>${esc(row.name)}</strong><small>散户 ${num(row.retail_count)} / 全部 ${num(row.attention)}</small></span><span class="rank-value">${num(row[config.count])}<small>${pct(row[config.ratio])}</small></span></a>`).join("")}</div></div></div>`;
      $$('[data-rank]', target).forEach((button) => button.addEventListener("click", () => { state.rankMetric = button.dataset.rank; renderRanking(); }));
    }

    async function loadOverviewPage() {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(state.overviewDate)) state.overviewDate = shiftDateKey(shanghaiToday(), -1);
      $("#main").innerHTML = pageHead("MARKET OVERVIEW", "散户市场总览", "按上海自然日核对帖子、指数、来源覆盖与行情", `<button id="previousDay" type="button" aria-label="前一天">← 前一天</button><label class="control">查看日期<input id="overviewDate" type="date" value="${esc(state.overviewDate)}" max="${esc(shanghaiToday())}"></label><button id="nextDay" type="button">后一天 →</button><button id="today" type="button">今天（采集中）</button><button id="reload" type="button">刷新</button>`)
        + `<div id="dayCoverage"></div><div id="marketOverview"></div><div class="section-head"><div><p class="eyebrow">ALL TRACKS</p><h2>赛道热度、帖子与行情</h2></div><span id="trackMeta" class="muted"></span></div><div id="tracks"></div><div class="section-head"><div><p class="eyebrow">SIGNAL RANKING</p><h2>所选日方向与情绪排行</h2></div></div><div id="ranking"></div><p class="footer">指标用于观察散户参与和情绪结构，不构成交易建议；来源覆盖状态是采集任务证据，不代表平台公开结果之外的绝对全量。</p>`;
      $("#reload").addEventListener("click", loadOverviewData);
      $("#overviewDate").addEventListener("change", (event) => {
        if (!event.target.value) { event.target.value = state.overviewDate; return; }
        state.overviewDate = event.target.value;
        updateUrl({ date: state.overviewDate });
        loadOverviewData();
      });
      $("#previousDay").addEventListener("click", () => {
        state.overviewDate = shiftDateKey(state.overviewDate, -1);
        updateUrl({ date: state.overviewDate });
        loadOverviewData();
      });
      $("#nextDay").addEventListener("click", () => {
        if (state.overviewDate >= shanghaiToday()) return;
        state.overviewDate = shiftDateKey(state.overviewDate, 1);
        updateUrl({ date: state.overviewDate });
        loadOverviewData();
      });
      $("#today").addEventListener("click", () => {
        state.overviewDate = shanghaiToday();
        updateUrl({ date: state.overviewDate });
        loadOverviewData();
      });
      updateUrl({ date: state.overviewDate });
      await loadOverviewData();
    }
    async function loadOverviewData() {
      setNotice(`正在加载 ${state.overviewDate} 的帖子、指数与行情…`);
      try {
        [state.overview, state.configuration] = await Promise.all([api("/topics/overview", { date: state.overviewDate }), optional("/config/status", {}, null)]);
        state.overviewDate = state.overview.selected_date || state.overviewDate;
        $("#overviewDate").value = state.overviewDate;
        $("#nextDay").disabled = state.overviewDate >= shanghaiToday();
        $("#dayCoverage").innerHTML = renderDayCoverage();
        $("#marketOverview").innerHTML = renderMarketOverview();
        $("#tracks").innerHTML = renderTrackCards();
        $("#trackMeta").textContent = `${state.overviewDate} · ${num(state.overview.topics?.length)} 个赛道 · ${num(state.overview.coverage?.indexed_content_count)} 条指数内容`;
        renderRanking(); attachChartHover();
        const market = state.configuration?.market;
        const status = state.overview.coverage?.is_collecting
          ? "今日仍在采集中"
          : Number(state.overview.coverage?.content_count || 0) > 0
            ? "该日已有帖子"
            : "该日暂无帖子";
        setNotice(`${state.overviewDate} · ${status} · 去重内容 ${num(state.overview.coverage?.content_count)} 条${market?.configured ? ` · 行情 ${market.provider}` : ""}`);
      } catch (error) { setNotice(`总览加载失败：${error.message}`, true); }
    }

    function selectedAsset(row) {
      const assets = row.assets?.length ? row.assets : row.asset ? [row.asset] : [];
      if (!assets.length) return null;
      const requested = Number(state.assetChoice[row.slug]);
      return assets.find((asset) => asset.id === requested) || assets.find((asset) => asset.has_price_data) || assets[0];
    }
    function coverageBlock(row, asset = null) {
      const coverage = row?.history_coverage || {};
      const observed = Number(coverage.observed_days || 0), indexed = Number(coverage.index_days || 0), percentiled = Number(coverage.percentile_days || 0), warming = Number(coverage.warming_up_days || 0), baseline = Number(coverage.current_baseline_days || 0), minimum = Number(coverage.minimum_baseline_days || 5);
      const windowDays = Number(coverage.window_days || 30);
      const priceDays = (asset?.price_history || []).filter((item) => item.close != null).length;
      const baselineNote = warming ? `<p class="coverage-note">${num(warming)} 个基线不足日未计算分位（每个日期至少需要 ${num(minimum)} 个更早有效日）；这些是状态，不是分位数点。</p>` : "";
      return `<div class="coverage-strip"><span class="coverage-chip">自然日样本 ${num(observed)} / ${num(windowDays)} 天</span><span class="coverage-chip">当日指数 ${num(indexed)} 天</span><span class="coverage-chip">历史分位已计算 ${num(percentiled)} / ${num(indexed)} 个指数日</span>${baseline < minimum ? `<span class="coverage-chip">当前基线 ${num(baseline)} / ${num(minimum)} 天</span>` : ""}${asset ? `<span class="coverage-chip">行情 ${num(priceDays)} 个交易日</span>` : ""}</div>${baselineNote}`;
    }
    function trendCard(row) {
      const profile = profileFor(row.heat_score), trend = trendFor(row.trend_summary), asset = selectedAsset(row);
      const assets = row.assets?.length ? row.assets : row.asset ? [row.asset] : [];
      const selector = assets.length > 1 ? `<select class="asset-select" data-asset-topic="${esc(row.slug)}" aria-label="${esc(row.name)}代表资产">${assets.map((item) => `<option value="${item.id}"${asset?.id === item.id ? " selected" : ""}>${esc(item.name)} · ${esc(item.symbol)}</option>`).join("")}</select>` : "";
      const priceLegend = asset?.has_price_data ? `<span class="legend-line" style="--legend:#dbe7f7">${esc(asset.name)}收盘价</span><span>最新 ${money(asset.price_history.at(-1)?.close, asset.currency)}</span>` : `<span>${asset ? `${esc(asset.name)}（${esc(asset.symbol)}）暂无行情` : "暂无代表资产"}</span>`;
      const wikiLegend = row.wikimedia_history?.length ? `<span class="legend-line" style="--legend:#a78bfa">Wikimedia 浏览量走势（窗口归一化）</span>` : "";
      const gapLegend = Number(row.history_coverage?.index_days || 0) > 1 ? `<span class="legend-gap" style="--legend:${profile.color}">数据间断</span>` : "";
      return `<article class="panel chart-card" style="--chart-color:${profile.color}"><div class="panel-head"><div><div class="chart-title-line"><h2>${esc(row.name)} · 热度、外部关注与价格</h2><span style="color:${trend.color}">${trend.icon} ${esc(trend.label)}</span></div><p class="chart-summary">当日指数 ${heat(row.heat_score)} · ${esc(percentile(row.historical_percentile))} · 当日 ${num(row.retail_count)} 条散户内容 · ${esc(row.daily_index_confidence?.label || "无已分析样本")}</p></div><a class="external" href="/posts?topic=${encodeURIComponent(row.slug)}&period=30d">历史帖子 →</a></div><div class="chart-meta"><span class="legend-dot" style="--legend:${profile.color}">当日情绪/热度指数</span>${wikiLegend}${gapLegend}${priceLegend}${selector}</div>${coverageBlock(row, asset)}${buildChart(row, asset, false)}</article>`;
    }
    function referenceComparison(topic, wiki) {
      const external = wiki?.percentile == null ? null : Number(wiki.percentile) * 100;
      const retail = topic?.historical_percentile == null ? null : Number(topic.historical_percentile);
      if (external == null || retail == null) return "等待双侧样本";
      if (external >= 80 && retail >= 80) return "内外关注共振";
      if (external >= 80 && retail < 50) return "外部关注领先";
      if (external < 40 && retail >= 70) return "社区热度领先";
      return "关注度基本同步";
    }
    function renderAttentionSignals() {
      const targetDate = state.overview?.selected_date;
      const byTopic = new Map();
      for (const row of state.attentionSignals || []) {
        if (targetDate && dateKey(row.observed_at) !== targetDate) continue;
        if (row.topic_id == null) continue;
        const values = byTopic.get(row.topic_id) || [];
        values.push(row); byTopic.set(row.topic_id, values);
      }
      const comparisons = (state.overview?.topics || state.topics || []).flatMap((topic) => {
        const rows = byTopic.get(topic.id) || [];
        return rows.length ? rows.map((wiki) => ({ topic, wiki })) : [{ topic, wiki: null }];
      });
      const body = comparisons.length
        ? `<div class="table-wrap"><table><thead><tr><th>赛道</th><th>Wikimedia 词条</th><th>观测日</th><th>浏览量</th><th>较前值</th><th>外部关注分位</th><th>散户热度</th><th>热度历史分位</th><th>参考校验</th></tr></thead><tbody>${comparisons.map(({ topic, wiki }) => `<tr><td>${esc(topic.name)}</td><td>${esc(wiki?.keyword || "暂无对应日数据")}</td><td>${wiki ? dateText(wiki.observed_at, false) : esc(targetDate || "—")}</td><td>${num(wiki?.value)}</td><td>${pct(wiki?.change_ratio)}</td><td>${wiki ? wiki.percentile == null ? "基线形成中" : pct(wiki.percentile) : "—"}</td><td>${heat(topic.heat_score)}</td><td>${topic.historical_percentile == null ? "基线积累中" : num(topic.historical_percentile, 1)}</td><td>${esc(referenceComparison(topic, wiki))}</td></tr>`).join("")}</tbody></table></div>`
        : `<div class="empty">尚未采集到 Wikimedia Pageviews 趋势信号。</div>`;
      return `<article class="panel"><div class="panel-head"><div><p class="eyebrow">SUPPLEMENTAL ATTENTION</p><h2>Wikimedia 与赛道热度对比</h2><p class="hint">表格用于横向校验各赛道；浏览量分位也以紫色虚线融合到下方趋势图，不直接改写散户热度。</p></div></div>${body}</article>`;
    }
    function renderTrendPage() {
      const rows = state.trendTopic === "all" ? state.overview?.topics || [] : (state.overview?.topics || []).filter((row) => row.slug === state.trendTopic);
      $("#trendCharts").classList.toggle("single", state.trendTopic !== "all");
      $("#trendCharts").innerHTML = rows.length ? rows.map(trendCard).join("") : `<div class="empty">没有匹配的赛道。</div>`;
      $("#trendTopic").innerHTML = topicOptions(state.trendTopic, true);
      $$("[data-asset-topic]").forEach((select) => select.addEventListener("change", () => { state.assetChoice[select.dataset.assetTopic] = Number(select.value); renderTrendPage(); }));
      attachChartHover();
    }
    async function loadTrendsPage() {
      $("#main").innerHTML = pageHead("HEAT & PRICE", "趋势与价格", "上海自然日情绪/热度、历史分位与代表资产价格", `<label class="control">赛道<select id="trendTopic"></select></label><label class="control">开始日期<input id="trendFrom" type="date" max="${esc(shanghaiToday())}"></label><label class="control">结束日期<input id="trendTo" type="date" max="${esc(shanghaiToday())}"></label><button id="applyTrendRange" type="button">应用范围</button><button id="reload" type="button">刷新</button>`)
        + `<section id="marketTrend"></section><section id="attentionSignals"></section><div class="section-head"><div><p class="eyebrow">TRACK CHARTS</p><h2>赛道热度与代表资产</h2></div></div><section id="trendCharts" class="chart-grid"></section><p class="footer">价格口径：前复权日线收盘价。</p>`;
      try {
        const rangeParams = state.trendFrom && state.trendTo ? { from_date: state.trendFrom, to_date: state.trendTo } : {};
        [state.topics, state.overview, state.configuration, state.attentionSignals] = await Promise.all([api("/topics"), api("/topics/overview", rangeParams), optional("/config/status", {}, null), optional("/trends/attention", { limit: 5000 }, [])]);
        state.trendTo = state.trendTo || state.overview?.selected_date || shanghaiToday();
        state.trendFrom = state.trendFrom || shiftDateKey(state.trendTo, -29);
        $("#trendFrom").value = state.trendFrom;
        $("#trendFrom").max = state.trendTo;
        $("#trendTo").value = state.trendTo;
        $("#trendTo").min = state.trendFrom;
        if (!state.topics.some((topic) => topic.slug === state.trendTopic) && state.trendTopic !== "all") state.trendTopic = "all";
        const market = state.overview.market || {};
        const profile = profileFor(market.heat_score);
        const marketWikiLegend = market.wikimedia_history?.length ? `<span class="legend-line" style="--legend:#a78bfa">Wikimedia 浏览量走势（窗口归一化）</span>` : "";
        const gapLegend = Number(market.history_coverage?.index_days || 0) > 1 ? `<span class="legend-gap" style="--legend:${profile.color}">数据间断</span>` : "";
        $("#marketTrend").innerHTML = `<article class="panel chart-card" style="--chart-color:${profile.color}"><div class="panel-head"><div><p class="eyebrow">WHOLE MARKET</p><h2>散户整体市场情绪/热度 · 历史趋势</h2><p class="hint">当日 ${heat(market.heat_score)} · ${esc(percentile(market.historical_percentile))} · ${esc(market.trend_summary?.label || "趋势待积累")}</p></div></div><div class="chart-meta"><span class="legend-dot" style="--legend:${profile.color}">当日情绪/热度指数</span>${marketWikiLegend}${gapLegend}</div>${coverageBlock(market)}${buildChart(market, null, false)}</article>`;
        $("#attentionSignals").innerHTML = renderAttentionSignals();
        renderTrendPage(); attachChartHover();
        setNotice(`已更新 ${num(state.overview.topics?.length)} 个赛道 · ${esc(state.trendFrom)} → ${esc(state.trendTo)} · 行情 ${state.configuration?.market?.provider || "未配置"}`);
        $("#trendTopic").addEventListener("change", (event) => { state.trendTopic = event.target.value; updateUrl({ topic: state.trendTopic === "all" ? "" : state.trendTopic }); renderTrendPage(); });
        $("#trendFrom").addEventListener("change", (event) => { $("#trendTo").min = event.target.value; });
        $("#trendTo").addEventListener("change", (event) => { $("#trendFrom").max = event.target.value; });
        $("#applyTrendRange").addEventListener("click", () => {
          const from = $("#trendFrom").value, to = $("#trendTo").value;
          if (!from || !to) return setNotice("请选择完整的开始和结束日期。", true);
          if (from > to) return setNotice("开始日期不能晚于结束日期。", true);
          if (to > shanghaiToday()) return setNotice("结束日期不能晚于今天。", true);
          state.trendFrom = from; state.trendTo = to;
          updateUrl({ from, to, topic: state.trendTopic === "all" ? "" : state.trendTopic });
          location.reload();
        });
        $("#reload").addEventListener("click", () => location.reload());
      } catch (error) { setNotice(`趋势页加载失败：${error.message}`, true); }
    }

    function postFrame() {
      return pageHead("POST ARCHIVE", "历史帖子", "按赛道、来源、时间和信号筛选")
        + `<section class="post-controls"><div class="post-control-row"><label class="control">赛道<select id="postTopic"></select></label><label class="control">来源<select id="postSource"></select></label><label class="control">时间范围<select id="postPeriod"><option value="custom">指定自然日</option><option value="24h">最新 24 小时</option><option value="7d">最近 7 天</option><option value="30d">最近 30 天</option><option value="all">全部历史</option></select></label><label class="control">指定日期<input id="postDate" type="date" max="${esc(shanghaiToday())}"></label><div class="filter-buttons" id="postFilters">${["all", "retail", "buy", "sell", "hold", "wait", "fomo", "panic", "promotion"].map((name) => `<button type="button" data-filter="${name}">${signalLabel(name)}</button>`).join("")}</div><button id="reloadPosts" type="button">刷新</button></div></section><div id="postSummary" class="post-summary"></div><section id="contentList" class="content-list"></section><button id="loadMore" class="load-more" type="button" hidden>加载更多</button>`;
    }
    function postSourceOptions() {
      const facets = state.posts?.source_facets || {};
      const names = [...new Set([...postSourceNames, ...Object.keys(facets)])];
      const total = Object.values(facets).reduce((sum, value) => sum + Number(value || 0), 0);
      return names.map((name) => {
        const count = name === "all" ? total : Number(facets[name] || 0);
        const label = name === "all" ? "全部来源" : sourceLabel(name);
        return `<option value="${esc(name)}"${name === state.postSource ? " selected" : ""}>${esc(label)}${facets[name] != null || name === "all" ? `（${num(count)}）` : ""}</option>`;
      }).join("");
    }
    function syncPostControls() {
      $("#postTopic").innerHTML = topicOptions(state.postTopic, true);
      $("#postSource").innerHTML = postSourceOptions();
      $("#postPeriod").value = state.postPeriod;
      $("#postDate").value = state.postDate || shiftDateKey(shanghaiToday(), -1);
      $$("[data-filter]").forEach((button) => button.classList.toggle("active", button.dataset.filter === state.postFilter));
    }
    function updatePostUrl() {
      updateUrl({
        topic: state.postTopic === "all" ? "" : state.postTopic,
        source: state.postSource === "all" ? "" : state.postSource,
        filter: state.postFilter,
        period: state.postPeriod,
        date: state.postPeriod === "custom" ? state.postDate : ""
      });
    }
    function analysisTags(analysis) {
      if (!analysis) return `<span class="tag">未分析</span>`;
      const tags = [`<span class="tag">${esc(analysis.actor_type === "retail" ? "散户" : analysis.actor_type || "角色未知")}</span>`];
      const tendency = ["advice_or_recommendation", "market_directional_view", "risk_warning"].includes(analysis.review?.intent_basis);
      if (analysis.intent === "buy") tags.push(`<span class="tag buy">${tendency ? "偏买倾向" : "买入意图"} · ${pct(analysis.intent_confidence)}</span>`);
      if (analysis.intent === "sell") tags.push(`<span class="tag sell">${tendency ? "偏卖倾向" : "卖出意图"} · ${pct(analysis.intent_confidence)}</span>`);
      if (analysis.intent === "hold") tags.push(`<span class="tag">持有 · ${pct(analysis.intent_confidence)}</span>`);
      if (analysis.intent === "wait") tags.push(`<span class="tag">等待观察 · ${pct(analysis.intent_confidence)}</span>`);
      if (analysis.fomo) tags.push(`<span class="tag fomo">FOMO · ${num(analysis.fomo_score, 2)}</span>`);
      if (analysis.panic) tags.push(`<span class="tag panic">恐慌</span>`);
      if (analysis.promotion) tags.push(`<span class="tag">广告/推广 · ${pct(analysis.promotion_confidence)}</span>`);
      return tags.join("");
    }
    function intentReview(analysis) {
      const review = analysis?.review;
      if (!review) return "";
      const evidence = String(review.intent_evidence || "").trim();
      const evidenceHtml = evidence ? `<span class="intent-evidence">原文：“${esc(evidence)}”</span> · ` : "";
      const explicit = ["buy", "sell", "hold", "wait"].includes(analysis.intent) && evidence;
      return `<details class="intent-review"${explicit ? " open" : ""}><summary>${explicit ? "意图/倾向判断依据（原文证据）" : "查看意图/倾向判断依据"}</summary><p>${evidenceHtml}${esc(review.rationale || "没有足够的交易动作或方向倾向证据。")}<br><span class="muted">${esc(review.reviewer || "语义审查")} · ${esc(review.intent_basis || "insufficient")}</span></p></details>`;
    }
    function contentCard(item) {
      const body = item.body || "（正文为空）", long = body.length > 260;
      const bodyHtml = long ? `<p class="content-body">${esc(body.slice(0, 260))}…</p><details class="content-more"><summary>展开全文</summary><p class="content-body">${esc(body)}</p></details>` : `<p class="content-body">${esc(body)}</p>`;
      const timeLabel = item.time_semantics === "market_session_reference" ? `参考交易日 ${esc(item.reference_date || dateKey(item.published_at))} · 知乎高赞回答` : `${esc(item.kind || "post")} · ${dateText(item.published_at)}`;
      return `<article class="content-card"><div class="content-meta"><span class="source-line"><span class="source-badge" data-source="${esc(item.source_name)}">${esc(sourceLabel(item.source_name))}</span><span>${timeLabel}</span></span>${item.url ? `<a class="external" href="${esc(item.url)}" target="_blank" rel="noreferrer">查看原帖 ↗</a>` : ""}</div><h3>${esc(item.title || "无标题")}</h3>${bodyHtml}<div class="content-tags">${analysisTags(item.analysis)}</div>${intentReview(item.analysis)}<div class="content-stats"><span>赞 ${num(item.likes)}</span><span>收藏 ${num(item.favorites)}</span><span>评论 ${num(item.comments)}</span><span>分享 ${num(item.shares)}</span><span>浏览 ${num(item.views)}</span><span>互动合计 ${num(item.engagement_sum)}</span></div></article>`;
    }
    function renderPosts() {
      const payload = state.posts || { items: [], total: 0, facets: {} };
      const topic = state.topics.find((item) => item.slug === state.postTopic);
      const topicLabel = state.postTopic === "all" ? "全部赛道" : topic?.name || "赛道";
      const selectedSource = state.postSource === "all" ? "全部来源" : sourceLabel(state.postSource);
      $("#postSummary").innerHTML = `<div><strong>${esc(topicLabel)}</strong> · ${esc(selectedSource)} · ${signalLabel(state.postFilter)} · ${num(payload.total)} 条<br><span class="muted">${dateText(payload.from_at, false)} → ${dateText(payload.to_at, false)}</span></div><div class="facet-list">${Object.entries(payload.source_facets || {}).map(([name, value]) => `<span class="source-badge" data-source="${esc(name)}">${esc(sourceLabel(name))} ${num(value)}</span>`).join("")}</div>`;
      $("#contentList").innerHTML = payload.items?.length ? payload.items.map(contentCard).join("") : `<div class="empty">这个时间范围和筛选条件下没有帖子。</div>`;
      syncPostControls();
      const button = $("#loadMore");
      button.hidden = Number(payload.items?.length || 0) >= Number(payload.total || 0);
      button.textContent = `加载更多（已显示 ${num(payload.items?.length)} / ${num(payload.total)}）`;
      button.disabled = false;
    }
    async function fetchPosts(reset = true) {
      if (state.loadingPosts) return;
      state.loadingPosts = true;
      const previousScroll = window.scrollY;
      const allTopics = state.postTopic === "all";
      const topic = state.topics.find((item) => item.slug === state.postTopic);
      if (!allTopics && !topic) { state.loadingPosts = false; return; }
      if (reset) {
        const list = $("#contentList");
        if (!list.querySelector(".content-card")) list.innerHTML = `<div class="empty">正在读取历史帖子…</div>`;
        else list.style.opacity = ".55";
      }
      try {
        const offset = reset ? 0 : state.posts?.items?.length || 0;
        const path = allTopics ? "/contents" : `/topics/${topic.id}/contents`;
        const dayBounds = state.postPeriod === "custom" ? calendarDayBounds(state.postDate) : {};
        const next = await api(path, { filter: state.postFilter, source: state.postSource, period: state.postPeriod, ...dayBounds, limit: reset ? 30 : 50, offset });
        if (reset) state.posts = next;
        else { state.posts.items.push(...next.items.filter((item) => !state.posts.items.some((known) => known.id === item.id))); state.posts.total = next.total; state.posts.facets = next.facets; }
        renderPosts();
        $("#contentList").style.opacity = "";
        if (reset && previousScroll > 0) window.scrollTo(0, previousScroll);
        setNotice(`已读取 ${allTopics ? "全部赛道" : esc(topic.name)} · ${state.postSource === "all" ? "全部来源" : esc(sourceLabel(state.postSource))} · ${signalLabel(state.postFilter)}，当前范围共 ${num(state.posts.total)} 条。`);
      } catch (error) { setNotice(`帖子加载失败：${error.message}`, true); }
      finally { $("#contentList").style.opacity = ""; state.loadingPosts = false; }
    }
    async function loadPostsPage() {
      $("#main").innerHTML = postFrame();
      try {
        state.topics = await api("/topics");
        if (state.postPeriod === "custom" && !/^\d{4}-\d{2}-\d{2}$/.test(state.postDate)) state.postDate = shiftDateKey(shanghaiToday(), -1);
        if (state.postTopic !== "all" && !state.topics.some((topic) => topic.slug === state.postTopic)) state.postTopic = "all";
        syncPostControls();
        $("#postTopic").addEventListener("change", (event) => { state.postTopic = event.target.value; updatePostUrl(); fetchPosts(true); });
        $("#postSource").addEventListener("change", (event) => { state.postSource = event.target.value; updatePostUrl(); fetchPosts(true); });
        $("#postPeriod").addEventListener("change", (event) => { state.postPeriod = event.target.value; if (state.postPeriod === "custom" && !state.postDate) state.postDate = shiftDateKey(shanghaiToday(), -1); syncPostControls(); updatePostUrl(); fetchPosts(true); });
        $("#postDate").addEventListener("change", (event) => { if (!event.target.value) { event.target.value = state.postDate || shiftDateKey(shanghaiToday(), -1); return; } state.postDate = event.target.value; state.postPeriod = "custom"; syncPostControls(); updatePostUrl(); fetchPosts(true); });
        $$("[data-filter]").forEach((button) => button.addEventListener("click", () => { state.postFilter = button.dataset.filter; syncPostControls(); updatePostUrl(); fetchPosts(true); }));
        $("#reloadPosts").addEventListener("click", () => fetchPosts(true));
        $("#loadMore").addEventListener("click", () => fetchPosts(false));
        await fetchPosts(true);
      } catch (error) { setNotice(`帖子页加载失败：${error.message}`, true); }
    }

    function sourceEvidence(row) {
      const evidence = row.evidence || {};
      const raw = Number(evidence.raw_observation_count || 0), content = Number(evidence.content_count || 0);
      const latest = evidence.last_observed_at ? `<br><span class="muted">最近观测 ${dateText(evidence.last_observed_at)}</span>` : "";
      if (row.name === "wikimedia-pageviews") return `趋势信号 ${num(evidence.trend_signal_count)} · 原始观测 ${num(raw)}${latest}`;
      if (row.name === "common-crawl") {
        const states = evidence.archive_status_counts || {};
        return `已检查 ${num(evidence.archive_checked_count)} 个 URL · 归档快照 ${num(evidence.archive_snapshot_count)}<br><span class="muted">无快照 ${num(states.no_capture || 0)} · 待重试 ${num(states.retry || 0)}</span>`;
      }
      if (row.name === "zhihu") return `可分析参考内容 ${num(content)} · 原始观测 ${num(raw)}${latest}`;
      return `历史内容 ${num(content)} · 原始观测 ${num(raw)}${latest}`;
    }
    const sourcePurpose = (name) => ({
      guba: "帖子与 LLM 分析", taoguba: "帖子与 LLM 分析", xiaohongshu: "帖子与 LLM 分析",
      zhihu: "高赞回答经交易日校验后进入实体识别与 LLM；页面标记参考交易日",
      "wikimedia-pageviews": "独立关注度信号；展示在趋势页，不计入帖子热度",
      "common-crawl": "补全已知帖子的归档正文；不生成独立帖子"
    }[name] || "来源证据");
    function renderResearchSources(config, rows) {
      const configured = Object.fromEntries((config?.sources || []).map((row) => [row.name, row]));
      const values = rows.length ? rows : Object.values(configured);
      return `<div class="table-wrap"><table><thead><tr><th>来源</th><th>采集状态</th><th>已落库证据</th><th>页面用途</th><th>配置</th><th>版本</th></tr></thead><tbody>${values.map((row) => { const setup = row.configuration || configured[row.name] || row; const health = setup.enabled === false ? "未启用" : row.health_status || "尚未采集"; return `<tr><td>${esc(sourceLabel(row.name))}<br><span class="muted">${esc(row.name)}</span></td><td>${esc(health)}</td><td>${sourceEvidence(row)}</td><td>${esc(sourcePurpose(row.name))}</td><td>${setup.configured ? "已就绪" : setup.required === false ? "可选未配置" : `缺少 ${esc((setup.missing || []).join(", "))}`}</td><td>${esc(row.collector_version || "—")}</td></tr>`; }).join("")}</tbody></table></div>`;
    }
    const eventLabels = {
      attention_spike: "关注度突增", buy_intent_spike: "买入意图突增", sell_intent_spike: "卖出意图突增",
      fomo_spike: "FOMO 突增", panic_spike: "恐慌突增", novice_spike: "新手参与突增",
      cross_platform_spike: "跨平台扩散"
    };
    const metricLabels = {
      post_count: "帖子量", unique_author_count: "独立作者数", buy_intent_ratio: "买入意图占比",
      sell_intent_ratio: "卖出意图占比", fomo_ratio: "FOMO 占比", panic_ratio: "恐慌占比", novice_ratio: "新手占比"
    };
    const eventLabel = (name) => eventLabels[name] || name || "尚无事件";
    const metricLabel = (name) => metricLabels[name] || name || "无单一指标";
    const fallbackMetric = {
      attention_spike: "post_count", buy_intent_spike: "buy_intent_ratio", sell_intent_spike: "sell_intent_ratio",
      fomo_spike: "fomo_ratio", panic_spike: "panic_ratio", novice_spike: "novice_ratio"
    };
    function readinessBox(readiness, title) {
      if (!readiness) return `<div class="research-status empty-state"><strong>${esc(title)}</strong>尚未生成研究状态。</div>`;
      const status = readiness.status || "unknown";
      const css = ["no_events", "no_signals"].includes(status) ? "empty-state" : status === "ready" ? "" : "waiting";
      const labels = {
        no_events: "尚无阈值事件", no_signals: "尚无指标事件", awaiting_entry: "等待下一交易日",
        awaiting_maturity: "等待收益成熟", partially_mature: "部分期限已成熟", benchmark_unavailable: "等待基准行情", ready: "已有可用样本"
      };
      return `<div class="research-status ${css}"><strong>${esc(labels[status] || title)}</strong></div>`;
    }
    function researchReplay(study, topic, eventName, horizon, coverage = null) {
      const period = horizon || "1d", periodLabel = `T+${period.replace("d", "")}`;
      const readiness = study?.readiness || {};
      const result = study?.horizons?.[period] || {};
      const raw = result.raw_return || {}, abnormal = result.market_abnormal_return || {};
      const sampleN = Number(raw.N || 0), eventN = Number(study?.events || 0);
      const observedDays = Number(coverage?.history_coverage?.observed_days || 0);
      const indexedDays = Number(coverage?.history_coverage?.index_days || 0);
      let lead = "";
      if (eventName && eventN && sampleN) {
        const move = Number(raw.mean || 0) >= 0 ? `平均上涨 ${pct(Math.abs(Number(raw.mean || 0)))}` : `平均下跌 ${pct(Math.abs(Number(raw.mean || 0)))}`;
        const relative = abnormal.mean == null ? "基准未对齐" : Number(abnormal.mean) >= 0 ? `超额 ${pct(Math.abs(Number(abnormal.mean)))}` : `落后 ${pct(Math.abs(Number(abnormal.mean)))}`;
        const positive = Math.round(Number(raw.hit_rate || 0) * sampleN);
        lead = `${eventLabel(eventName)} · ${periodLabel}：${move}，${relative}，正收益 ${positive}/${sampleN}`;
      }
      const confidence = sampleN >= 30 ? "可初步比较" : sampleN >= 10 ? "样本有限" : sampleN > 0 ? "案例级样本" : "尚无成熟样本";
      const cells = eventName ? `<div class="replay-cell"><strong>${num(eventN)}</strong><small>检测事件</small></div><div class="replay-cell"><strong>${num(sampleN)}</strong><small>${esc(periodLabel)} 成熟样本</small></div><div class="replay-cell"><strong>${pct(raw.mean)}</strong><small>代表标的平均收益</small></div><div class="replay-cell"><strong>${esc(confidence)}</strong><small>样本状态</small></div>` : `<div class="replay-cell"><strong>0</strong><small>异常事件</small></div><div class="replay-cell"><strong>${num(observedDays)}</strong><small>帖子样本日</small></div><div class="replay-cell"><strong>${num(indexedDays)}</strong><small>热度指数日</small></div><div class="replay-cell"><strong>${indexedDays ? "暂无事件" : observedDays ? "基线形成中" : "暂无样本"}</strong><small>研究状态</small></div>`;
      return `<div class="panel-head"><div><p class="eyebrow">RESEARCH SNAPSHOT</p><h2>研究概览</h2></div><div><a class="external" href="/trends?topic=${encodeURIComponent(topic?.slug || "")}">查看趋势 →</a> <a class="external" href="/posts?topic=${encodeURIComponent(topic?.slug || "")}&period=all">历史帖子 →</a></div></div>${lead ? `<p class="replay-lead">${esc(lead)}</p>` : ""}<div class="replay-grid">${cells}</div>`;
    }
    function studyTable(study) {
      if (!study || study.error) return `<div class="research-status empty-state"><strong>暂无事件研究数据</strong></div>`;
      const horizons = ["1d", "3d", "5d", "10d", "20d"];
      const readiness = study.readiness || {};
      const table = `<div class="table-wrap"><table><thead><tr><th>期限</th><th>原始收益均值</th><th>市场异常均值</th><th>成熟 / 待成熟</th></tr></thead><tbody>${horizons.map((horizon) => { const raw = study.horizons?.[horizon]?.raw_return || {}, abnormal = study.horizons?.[horizon]?.market_abnormal_return || {}, ready = readiness.horizons?.[horizon] || {}; return `<tr><td>T+${horizon.replace("d", "")}</td><td>${pct(raw.mean)}</td><td>${pct(abnormal.mean)}</td><td>${num(raw.N)} / ${num(ready.pending)}</td></tr>`; }).join("")}</tbody></table></div>`;
      return `${readinessBox(readiness, "事件研究状态")}${Number(readiness.return_rows || 0) ? table : ""}`;
    }
    function quantileTable(study, metricName) {
      if (!metricName) return `<div class="research-status empty-state"><strong>暂无分位数据</strong></div>`;
      if (!study || study.error) return `<div class="research-status empty-state"><strong>暂无分位研究数据</strong></div>`;
      const table = `<div class="table-wrap"><table><thead><tr><th>分位组</th><th>异常收益均值</th><th>命中率</th><th>N</th></tr></thead><tbody>${Object.entries(study.quantiles || {}).map(([bucket, row]) => `<tr><td>${esc(bucket)}</td><td>${pct(row.mean)}</td><td>${pct(row.hit_rate)}</td><td>${num(row.N)}</td></tr>`).join("")}</tbody></table></div>`;
      const enough = Number(study.N || 0) >= 20;
      const sampleNote = !enough && Number(study.N || 0) > 0 ? `<div class="research-status waiting"><strong>样本 ${num(study.N)} / 20</strong></div>` : "";
      return `${readinessBox(study.readiness, "分位研究状态")}${enough ? table : ""}${sampleNote}`;
    }
    async function showEvent(id) {
      const target = $("#eventDetail"); target.innerHTML = `<div class="empty">正在加载事件 #${id}…</div>`;
      try {
        const event = await api(`/events/${id}`);
        const rows = event.raw_drilldown || [];
        const capNote = rows.length >= Number(event.raw_drilldown_limit || 0) ? `（单次最多读取 ${num(event.raw_drilldown_limit)} 条）` : "";
        target.innerHTML = `<div class="raw-detail"><h3>事件 #${id} · ${esc(eventLabel(event.event_type))}</h3><p class="muted">${dateText(event.started_at)} → ${dateText(event.ended_at || event.peaked_at)} · 当前赛道关联原始内容 ${num(rows.length)} 条 ${capNote}</p>${rows.map((row) => { const body = String(row.content?.body || "").replace(/\s+/g, " ").trim(); const preview = body.length > 56 ? `${body.slice(0, 56)}…` : body; const label = row.content?.title || preview || row.content?.source_item_id || "原始内容"; const sourceLink = row.content?.url ? `<a class="external" href="${esc(row.content.url)}" target="_blank" rel="noopener">打开原帖 ↗</a>` : ""; return `<details><summary>${esc(label)} · ${dateText(row.content?.published_at)}</summary><p>${esc(row.content?.body || "")}</p>${sourceLink}<pre>${esc(JSON.stringify({ analysis: row.analysis, raw_observations: row.raw_observations }, null, 2))}</pre></details>`; }).join("") || `<div class="empty">没有关联原始内容。</div>`}</div>`;
      } catch (error) { target.innerHTML = `<div class="empty">事件详情加载失败：${esc(error.message)}</div>`; }
    }
    async function loadResearchData() {
      const topic = state.topics.find((row) => row.slug === state.researchTopic) || state.topics[0];
      if (!topic) return;
      state.researchTopic = topic.slug; $("#researchTopic").value = topic.slug;
      setNotice(`正在加载 ${topic.name} 的研究结果…`);
      const [config, sources, events, overview] = await Promise.all([
        optional("/config/status", {}, null), optional("/sources/status", {}, []),
        optional("/events", { topic_id: topic.id, limit: 5000 }, []),
        optional("/topics/overview", {}, null)
      ]);
      const coverage = overview?.topics?.find((row) => row.id === topic.id) || null;
      const eventTypes = [...new Set(events.map((event) => event.event_type))];
      if (!eventTypes.includes(state.researchEvent)) state.researchEvent = eventTypes[0] || "";
      const selectedEvents = events.filter((event) => event.event_type === state.researchEvent);
      const metricNames = [...new Set(selectedEvents.flatMap((event) => event.metrics || []).map((metric) => metric.metric_name))];
      const preferredMetric = fallbackMetric[state.researchEvent];
      if (!metricNames.includes(state.researchMetric)) state.researchMetric = metricNames.includes(preferredMetric) ? preferredMetric : metricNames[0] || preferredMetric || "";
      $("#researchEvent").innerHTML = eventTypes.length ? eventTypes.map((name) => `<option value="${esc(name)}"${name === state.researchEvent ? " selected" : ""}>${esc(eventLabel(name))} · ${num(events.filter((event) => event.event_type === name).length)}</option>`).join("") : `<option value="">尚无检测事件</option>`;
      $("#researchEvent").disabled = !eventTypes.length;
      $("#researchHorizon").value = state.researchHorizon;
      updateUrl({ topic: state.researchTopic, event: state.researchEvent, metric: state.researchMetric, horizon: state.researchHorizon });
      const [study, quantile] = await Promise.all([
        state.researchEvent ? optional("/research/event-study", { topic: topic.slug, event: state.researchEvent }, null) : null,
        state.researchMetric ? optional("/research/quantile-study", { topic: topic.slug, metric: state.researchMetric, horizon: state.researchHorizon }, null) : null
      ]);
      $("#eventStudyTitle").textContent = `${eventLabel(state.researchEvent)} · 事件后收益`;
      $("#eventStudyHint").textContent = state.researchEvent ? `${num(selectedEvents.length)} 个事件` : "暂无事件";
      $("#quantileStudyTitle").textContent = `${metricLabel(state.researchMetric)} · T+${state.researchHorizon.replace("d", "")} 分位研究`;
      $("#quantileStudyHint").textContent = state.researchMetric ? metricLabel(state.researchMetric) : "暂无触发指标";
      $("#researchSummary").innerHTML = researchReplay(study, topic, state.researchEvent, state.researchHorizon, coverage);
      $("#sourceStatus").innerHTML = renderResearchSources(config, sources);
      $("#eventStudy").innerHTML = state.researchEvent ? studyTable(study) : `<div class="research-status empty-state"><strong>暂无异常事件</strong></div>`;
      $("#quantileStudy").innerHTML = state.researchMetric ? quantileTable(quantile, state.researchMetric) : `<div class="research-status empty-state"><strong>暂无分位数据</strong></div>`;
      const visibleEvents = selectedEvents.slice(0, 200);
      $("#events").innerHTML = selectedEvents.length ? `<p class="hint">${esc(eventLabel(state.researchEvent))} · ${num(selectedEvents.length)} 个${selectedEvents.length > visibleEvents.length ? ` · 显示最近 ${num(visibleEvents.length)} 个` : ""}</p><div class="table-wrap"><table><thead><tr><th>ID</th><th>类型</th><th>开始</th><th>峰值</th><th>回报状态</th></tr></thead><tbody>${visibleEvents.map((event) => { const mature = (event.returns || []).filter((row) => row.raw_return != null).length; const returnStatus = !event.returns?.length ? "等待下一交易日" : mature ? `${mature} 个期限已成熟` : "等待 T+N"; return `<tr><td><button class="event-button" data-event="${event.id}">#${event.id}</button></td><td>${esc(eventLabel(event.event_type))}</td><td>${dateText(event.started_at)}</td><td>${num(event.peak_value, 3)}</td><td>${esc(returnStatus)}</td></tr>`; }).join("")}</tbody></table></div>` : `<div class="research-status empty-state"><strong>暂无异常事件</strong></div>`;
      $$("[data-event]").forEach((button) => button.addEventListener("click", () => showEvent(Number(button.dataset.event))));
      $("#eventDetail").innerHTML = "";
      const observedDays = Number(coverage?.history_coverage?.observed_days || 0), indexedDays = Number(coverage?.history_coverage?.index_days || 0);
      setNotice(events.length ? `${topic.name} · ${num(events.length)} 个检测事件` : `${topic.name} · 帖子样本 ${num(observedDays)} 天 · 热度指数 ${num(indexedDays)} 天`);
    }
    async function loadResearchPage() {
      $("#main").innerHTML = pageHead("RESEARCH & TRACE", "研究与溯源", "事件收益、指标分位与原始观测", `<label class="control">赛道<select id="researchTopic"></select></label><label class="control">事件<select id="researchEvent"><option>读取中…</option></select></label><label class="control">分位期限<select id="researchHorizon"><option value="1d">T+1</option><option value="3d">T+3</option><option value="5d">T+5</option><option value="10d">T+10</option><option value="20d">T+20</option></select></label>`)
        + `<article id="researchSummary" class="panel"></article><section class="research-grid"><article class="panel"><div class="panel-head"><div><h2 id="eventStudyTitle">事件后收益</h2><p id="eventStudyHint" class="hint">读取中</p></div></div><div id="eventStudy"></div></article><article class="panel"><div class="panel-head"><div><h2 id="quantileStudyTitle">指标强度分组</h2><p id="quantileStudyHint" class="hint">读取中</p></div></div><div id="quantileStudy"></div></article></section><article class="panel"><div class="panel-head"><div><h2>检测事件与 RawObservation</h2></div></div><div id="events"></div><div id="eventDetail"></div></article><article class="panel"><div class="panel-head"><div><h2>数据源与行情状态</h2></div></div><div id="sourceStatus"></div></article>`;
      try {
        state.topics = await api("/topics");
        if (!state.topics.some((topic) => topic.slug === state.researchTopic)) state.researchTopic = state.topics[0]?.slug || "";
        if (!["1d", "3d", "5d", "10d", "20d"].includes(state.researchHorizon)) state.researchHorizon = "1d";
        $("#researchTopic").innerHTML = topicOptions(state.researchTopic);
        $("#researchHorizon").value = state.researchHorizon;
        $("#researchTopic").addEventListener("change", (event) => { state.researchTopic = event.target.value; state.researchEvent = ""; state.researchMetric = ""; loadResearchData(); });
        $("#researchEvent").addEventListener("change", (event) => { state.researchEvent = event.target.value; state.researchMetric = ""; loadResearchData(); });
        $("#researchHorizon").addEventListener("change", (event) => { state.researchHorizon = event.target.value; loadResearchData(); });
        await loadResearchData();
      } catch (error) { setNotice(`研究页加载失败：${error.message}`, true); }
    }

    ({ overview: loadOverviewPage, trends: loadTrendsPage, posts: loadPostsPage, research: loadResearchPage }[PAGE] || loadOverviewPage)();
  </script>
</body>
</html>"""
