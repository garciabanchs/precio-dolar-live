let REPORT_DATA = null;
let currentMarket = "mercado_1";
let currentFx = "compuesto";

async function loadReportData() {
  const params = new URLSearchParams(window.location.search);
  const email = params.get("email");

  const reportUrl = email
    ? `/report/data?email=${encodeURIComponent(email)}`
    : "/report/data";

  const response = await fetch(reportUrl);
  REPORT_DATA = await response.json();
  bindToolbarEvents();
  renderReport();
}

function bindToolbarEvents() {
  document.querySelectorAll("#market-toolbar button").forEach(btn => {
    btn.addEventListener("click", () => {
      currentMarket = btn.dataset.market;
      setActiveButton("#market-toolbar", btn);
      renderReport();
    });
  });

  document.querySelectorAll("#fx-toolbar button").forEach(btn => {
    btn.addEventListener("click", () => {
      currentFx = btn.dataset.fx;
      setActiveButton("#fx-toolbar", btn);
      renderReport();
    });
  });
}

function setActiveButton(selector, activeBtn) {
  document.querySelectorAll(`${selector} button`).forEach(btn => btn.classList.remove("active"));
  activeBtn.classList.add("active");
}

function getCurrentMarketData() {
  return REPORT_DATA.markets.find(m => m.market_key === currentMarket);
}

function renderReport() {
  const market = getCurrentMarketData();
  if (!market) return;

  const fxData = market.fx_views[currentFx];
  if (!fxData) return;

  const title = document.getElementById("active-market-title");
  if (title) {
    title.textContent = `Mercado activo: ${market.city} · Referencia activa: ${currentFx}`;
  }

  renderTableRows(fxData.rows);
  renderMobileCards(fxData.rows);
}

function renderTableRows(rows) {
  const tbody = document.getElementById("pricing-table-body");
  if (!tbody) return;

  tbody.innerHTML = rows.map(row => `
    <tr>
      <td>${row.nombre_producto}</td>
      <td>${row.sku}</td>
      <td>${row.unidad}</td>
      <td>${row.precio_viejo_usd}</td>
      <td>${row.precio_nuevo_usd}</td>
      <td>${row.cambio_pct}%</td>
      <td>${row.competidor_lider}</td>
      <td>${row.competidor_intermedio}</td>
      <td>${row.competidor_economico}</td>
      <td>${Math.round(row.peso_competencia * 100)}%</td>
      <td>${Math.round(row.peso_riesgo * 100)}%</td>
      <td>${row.senal}</td>
    </tr>
  `).join("");
}

function renderMobileCards(rows) {
  const grid = document.getElementById("pricing-mobile-grid");
  if (!grid) return;

  grid.innerHTML = rows.map(row => `
    <article class="mobile-card">
      <h4>${row.nombre_producto}</h4>
      <div class="mobile-meta-row"><span>SKU</span><span>${row.sku}</span></div>
      <div class="mobile-meta-row"><span>Unidad</span><span>${row.unidad}</span></div>
      <div class="mobile-meta-row"><span>Precio viejo</span><span>${row.precio_viejo_usd}</span></div>
      <div class="mobile-meta-row"><span>Precio nuevo</span><span>${row.precio_nuevo_usd}</span></div>
      <div class="mobile-meta-row"><span>Cambio</span><span>${row.cambio_pct}%</span></div>
      <div class="mobile-meta-row"><span>Señal</span><span>${row.senal}</span></div>
    </article>
  `).join("");
}

document.addEventListener("DOMContentLoaded", loadReportData);