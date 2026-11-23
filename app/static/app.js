"use strict";

const state = {
  datasets: [],
  references: [],
  batches: [],
  selectedDataset: null,
  selectedBatch: null,
  rulesVersions: [],
  specDirty: false,
  channelSwitch: null,
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function toast(message, kind = "ok") {
  const node = $("#toast");
  node.textContent = message;
  node.className = `toast ${kind}`;
  node.hidden = false;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => { node.hidden = true; }, 5200);
}

async function api(path, options = {}) {
  const response = await fetch(path, options);
  const text = await response.text();
  let payload = null;
  if (text) {
    try { payload = JSON.parse(text); } catch { payload = { raw: text }; }
  }
  if (!response.ok) {
    const error = payload?.error || payload?.detail || {};
    const message = typeof error === "string" ? error : (error.message || `HTTP ${response.status}`);
    const failure = new Error(message);
    failure.code = error.code || `http_${response.status}`;
    failure.payload = payload;
    failure.status = response.status;
    throw failure;
  }
  return payload;
}

function jsonBody(value) {
  return {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(value),
  };
}

function statusBadge(status) {
  return `<span class="badge ${escapeHtml(status)}">${escapeHtml(status)}</span>`;
}

function shortId(id) {
  return id ? id.slice(0, 10) : "—";
}

function fmtTime(value) {
  if (!value) return "—";
  try {
    return new Date(value).toLocaleString("zh-CN", { hour12: false });
  } catch { return String(value); }
}

function updateTabCounts() {}

// ---------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------
function activateTab(name) {
  $$(".tab").forEach((tab) => tab.classList.toggle("active", tab.dataset.tab === name));
  $$(".panel").forEach((panel) => panel.classList.toggle("active", panel.id === `panel-${name}`));
  if (name === "rules") {
    loadRules();
    loadChannelSwitch();
  }
  if (name === "batches") loadBatches();
  if (name === "references") loadReferences();
  if (name === "datasets") loadDatasets();
}

$$(".tab").forEach((tab) => tab.addEventListener("click", () => activateTab(tab.dataset.tab)));

// ---------------------------------------------------------------------------
// Summary and datasets
// ---------------------------------------------------------------------------
async function loadSummary() {
  try {
    const data = await api("/api/summary");
    $("#stats").innerHTML = [
      ["数据集", data.datasets],
      ["已发布批次", data.published_batches],
      ["失败批次", data.failed_batches],
      ["隔离行", data.quarantined_rows],
      ["可信文件", data.trusted_files],
    ].map(([label, value]) => `<div class="stat">${escapeHtml(label)}<b>${value}</b></div>`).join("");
  } catch (error) {
    $("#stats").innerHTML = `<span class="badge failed">${escapeHtml(error.message)}</span>`;
  }
}

function datasetOptions(select, includeAll = false) {
  const current = select.value;
  select.innerHTML =
    (includeAll ? `<option value="">全部数据集</option>` : "") +
    state.datasets.map((item) =>
      `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name)}</option>`).join("");
  if (Array.from(select.options).some((option) => option.value === current)) {
    select.value = current;
  }
}

async function loadDatasets() {
  try {
    const data = await api("/api/datasets");
    state.datasets = data.datasets;
    if (!state.selectedDataset && state.datasets.length) {
      state.selectedDataset = state.datasets[0].name;
    }
    renderDatasets();
    datasetOptions($("#upload-dataset"));
    datasetOptions($("#batch-filter"), true);
    datasetOptions($("#rules-dataset"));
    renderPartitions();
    renderUploadChannelHint();
  } catch (error) {
    $("#dataset-list").innerHTML = `<div class="callout bad">${escapeHtml(error.message)}</div>`;
  }
}

function renderDatasets() {
  const container = $("#dataset-list");
  if (!state.datasets.length) {
    container.innerHTML = `<p class="muted">还没有数据集。请确认 config/rules.yaml 已加载，或通过 API 创建。</p>`;
    return;
  }
  container.innerHTML = state.datasets.map((item) => {
    const selected = item.name === state.selectedDataset ? "selected" : "";
    return `
      <div class="item clickable ${selected}" data-dataset="${escapeHtml(item.name)}">
        <div class="title">
          <span class="name">${escapeHtml(item.name)}</span>
          <span class="badge">规则 v${item.current_rules_version}</span>
        </div>
        <div class="meta">${escapeHtml(item.description || "无说明")}</div>
        <div class="meta">${item.batch_count} 个批次 · ${item.published_batches} 个已发布</div>
      </div>`;
  }).join("");
  $$("[data-dataset]", container).forEach((node) => {
    node.addEventListener("click", () => {
      state.selectedDataset = node.dataset.dataset;
      renderDatasets();
      renderPartitions();
    });
  });
}

async function renderPartitions() {
  const dataset = state.selectedDataset;
  const container = $("#partition-list");
  const preview = $("#preview-table");
  if (!dataset) {
    $("#partition-caption").textContent = "选择左侧数据集查看分区。";
    container.innerHTML = "";
    preview.innerHTML = `<p class="muted">暂无数据。</p>`;
    return;
  }
  $("#partition-caption").textContent = `${dataset} · 点击分区查看预览`;
  try {
    const [parts, detail] = await Promise.all([
      api(`/api/datasets/${encodeURIComponent(dataset)}/partitions`),
      api(`/api/datasets/${encodeURIComponent(dataset)}`),
    ]);
    state.datasets = state.datasets.map((item) =>
      item.name === dataset ? { ...item, ...detail } : item);
    if (!parts.partitions.length) {
      container.innerHTML = `<p class="muted">尚无已发布分区。</p>`;
    } else {
      container.innerHTML = parts.partitions.map((part) => `
        <div class="item clickable" data-date="${escapeHtml(part.date_key)}">
          <div class="title">
            <span class="name">date=${escapeHtml(part.date_key)}</span>
            <span class="badge">${part.row_count} 行</span>
          </div>
          <div class="meta">${part.batches} 个批次 · ${part.files} 个文件</div>
        </div>`).join("");
      $$("[data-date]", container).forEach((node) => {
        node.addEventListener("click", () => {
          $$("[data-date]", container).forEach((item) => item.classList.remove("selected"));
          node.classList.add("selected");
          loadPreview(dataset, node.dataset.date);
        });
      });
    }
    const latest = parts.partitions[0]?.date_key;
    if (latest) loadPreview(dataset, latest);
    else preview.innerHTML = `<p class="muted">暂无可信数据。</p>`;
  } catch (error) {
    container.innerHTML = `<div class="callout bad">${escapeHtml(error.message)}</div>`;
  }
}

async function loadPreview(dataset, dateKey) {
  const container = $("#preview-table");
  $("#preview-caption").textContent = `date=${dateKey}（最多 50 行）`;
  try {
    const data = await api(
      `/api/datasets/${encodeURIComponent(dataset)}/preview?date=${encodeURIComponent(dateKey)}&limit=50`
    );
    if (!data.rows.length) {
      container.innerHTML = `<p class="muted">该分区暂无可预览行。</p>`;
      return;
    }
    container.innerHTML = `
      <table>
        <thead><tr>${data.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr></thead>
        <tbody>${data.rows.map((row) => `<tr>${data.columns.map((column) =>
          `<td>${escapeHtml(row[column] ?? "")}</td>`).join("")}</tr>`).join("")}</tbody>
      </table>`;
    $("#preview-caption").textContent = `date=${dateKey} · ${data.files} 个文件 · 显示 ${data.rows.length} 行`;
  } catch (error) {
    container.innerHTML = `<div class="callout bad">${escapeHtml(error.message)}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------
function renderUploadChannelHint() {
  const hint = $("#upload-channel-hint");
  const name = $("#upload-dataset").value;
  const dataset = state.datasets.find((item) => item.name === name);
  if (!dataset) {
    hint.hidden = true;
    return;
  }
  if (dataset.channel_key_enabled) {
    hint.hidden = false;
    hint.className = "callout warn";
    hint.innerHTML = `该数据集已切换到渠道口径：每行必须包含 <code>channel</code>（仅允许 <code>direct</code> 或 <code>partner</code>），全表主键为 <code>(channel, order_id)</code>。`;
  } else {
    hint.hidden = false;
    hint.className = "callout";
    hint.innerHTML = `该数据集仍为旧口径：全表唯一键为字符串 <code>order_id</code>，不需要 <code>channel</code> 列；发布后自动归为 <code>direct</code>。`;
  }
}

$("#upload-dataset").addEventListener("change", renderUploadChannelHint);

$("#upload-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const dataset = $("#upload-dataset").value;
  const files = $("#upload-files").files;
  if (!dataset || !files.length) {
    toast("请选择数据集和至少一个 parquet 文件", "bad");
    return;
  }
  const body = new FormData();
  Array.from(files).forEach((file) => body.append("files", file));
  body.append("process", $("#upload-process").checked ? "true" : "false");
  const button = $("#upload-submit");
  button.disabled = true;
  button.textContent = "校验中…";
  const target = $("#upload-result");
  target.className = "result muted";
  target.textContent = "正在上传并执行字段、主键与引用校验…";
  try {
    const response = await api(`/api/datasets/${encodeURIComponent(dataset)}/batches`, {
      method: "POST",
      body,
    });
    const batch = response.batch;
    target.className = "result";
    target.innerHTML = renderRunResult(batch, response);
    toast(batch.status === "published" ? "批次已发布" :
      batch.status === "failed" ? "批次校验失败，已隔离问题行" : "批次已受理",
      batch.status === "failed" ? "bad" : "ok");
    await Promise.all([loadSummary(), loadDatasets(), loadBatches()]);
    if (batch.status === "failed" || batch.quarantine_count > 0) {
      state.selectedBatch = batch.id;
      await loadQuarantine(batch.id);
      activateTab("batches");
      await loadBatches();
    }
  } catch (error) {
    target.className = "result";
    target.innerHTML = `<div class="callout bad"><b>${escapeHtml(error.code)}</b><br>${escapeHtml(error.message)}</div>`;
    toast(error.message, "bad");
  } finally {
    button.disabled = false;
    button.textContent = "上传并校验";
    $("#upload-form").reset();
  }
});

function renderRunResult(batch, response) {
  const run = response?.batch?.run || null;
  const runError = response?.batch?.run_error || null;
  const published = batch.status === "published";
  const head = published
    ? `<div class="callout ok">批次已发布：${batch.valid_count} 行写入可信数据集，${batch.quarantine_count} 行进入隔离区。</div>`
    : `<div class="callout bad">批次状态：${escapeHtml(batch.status)}${runError ? ` · ${escapeHtml(runError.code)}` : ""}<br>${escapeHtml(batch.error_message || runError?.message || "")}</div>`;
  return `
    ${head}
    <dl class="kv">
      <dt>批次 ID</dt><dd>${escapeHtml(batch.id)}</dd>
      <dt>受理序号</dt><dd>${batch.acceptance_seq}</dd>
      <dt>固定规则版本</dt><dd>v${batch.rules_version}</dd>
      <dt>尝试次数</dt><dd>${batch.attempt}</dd>
      <dt>总行数 / 有效 / 隔离</dt><dd>${batch.row_count} / ${batch.valid_count} / ${batch.quarantine_count}</dd>
      <dt>固定参考表</dt><dd>${escapeHtml(
        Object.entries(batch.reference_versions || {}).map(([name, meta]) => `${name} v${meta.version}`).join(", ") || "无"
      )}</dd>
    </dl>`;
}

// ---------------------------------------------------------------------------
// Batches
// ---------------------------------------------------------------------------
async function loadBatches() {
  try {
    const dataset = $("#batch-filter").value;
    const query = dataset ? `?dataset=${encodeURIComponent(dataset)}` : "";
    const data = await api(`/api/batches${query}`);
    state.batches = data.batches;
    renderBatches();
  } catch (error) {
    $("#batch-list").innerHTML = `<div class="callout bad">${escapeHtml(error.message)}</div>`;
  }
}

$("#batch-filter").addEventListener("change", loadBatches);

function renderBatches() {
  const container = $("#batch-list");
  if (!state.batches.length) {
    container.innerHTML = `<p class="muted">暂无批次。</p>`;
    return;
  }
  container.innerHTML = state.batches.map((batch) => {
    const selected = batch.id === state.selectedBatch ? "selected" : "";
    return `
      <div class="item clickable ${selected}" data-batch="${escapeHtml(batch.id)}">
        <div class="title">
          <span class="name">${escapeHtml(batch.dataset)} · #${batch.acceptance_seq}</span>
          ${statusBadge(batch.status)}
        </div>
        <div class="meta"><code>${shortId(batch.id)}</code> · 规则 v${batch.rules_version} · 第 ${batch.attempt} 次尝试 · ${
          batch.channel_mode === "channel" ? "渠道口径 (channel, order_id)" : "旧口径 order_id（归 direct）"
        }</div>
        <div class="meta">${batch.row_count} 行 · 有效 ${batch.valid_count} · 隔离 ${batch.quarantine_count} · ${fmtTime(batch.accepted_at)}</div>
      </div>`;
  }).join("");
  $$("[data-batch]", container).forEach((node) => {
    node.addEventListener("click", () => selectBatch(node.dataset.batch));
  });
}

async function selectBatch(batchId) {
  state.selectedBatch = batchId;
  renderBatches();
  try {
    const batch = await api(`/api/batches/${encodeURIComponent(batchId)}`);
    renderBatchDetail(batch);
    if (Number(batch.quarantine_count) > 0 || batch.status === "failed") {
      await loadQuarantine(batchId);
    } else {
      $("#quarantine-card").hidden = true;
    }
  } catch (error) {
    $("#batch-detail").innerHTML = `<div class="callout bad">${escapeHtml(error.message)}</div>`;
  }
}

function renderBatchDetail(batch) {
  const refs = Object.entries(batch.reference_versions || {})
    .map(([name, meta]) => `${name} v${meta.version}`).join(", ") || "无";
  const errors = batch.errors
    ? `<h3>失败详情</h3><pre>${escapeHtml(JSON.stringify(batch.errors, null, 2))}</pre>` : "";
  const published = batch.status === "published";
  $("#batch-detail").innerHTML = `
    <div class="title">
      <span class="name">${escapeHtml(batch.dataset)} · 批次 ${escapeHtml(batch.id)}</span>
      ${statusBadge(batch.status)}
    </div>
    <dl class="kv">
      <dt>受理序号</dt><dd>${batch.acceptance_seq}</dd>
      <dt>主键口径</dt><dd>${
        batch.channel_mode === "channel"
          ? "渠道口径：全表主键 (channel, order_id)，channel ∈ direct / partner"
          : "旧口径：全表唯一键 order_id（字符串）；发布时归为 direct"
      }</dd>
      <dt>固定规则版本</dt><dd>v${batch.rules_version}（受理时固定）</dd>
      <dt>固定参考表</dt><dd>${escapeHtml(refs)}</dd>
      <dt>源文件</dt><dd>${(batch.source_files || []).map((f) => escapeHtml(f.original_name)).join(", ") || "—"}</dd>
      <dt>总行数 / 有效 / 隔离</dt><dd>${batch.row_count} / ${batch.valid_count} / ${batch.quarantine_count}</dd>
      <dt>尝试次数</dt><dd>${batch.attempt}</dd>
      <dt>受理时间</dt><dd>${fmtTime(batch.accepted_at)}</dd>
      <dt>完成时间</dt><dd>${fmtTime(batch.finished_at)}</dd>
      <dt>最后成功</dt><dd>${fmtTime(batch.last_success_at)}</dd>
    </dl>
    ${batch.error_message ? `<div class="callout bad">${escapeHtml(batch.error_message)}</div>` : ""}
    ${batch.previous_result_preserved
      ? `<div class="callout warn">本次重跑失败，但该批次此前已发布的可信分区仍然在线，旧数据未被覆盖。</div>`
      : ""}
    ${errors}
    <h3>操作</h3>
    <div class="actions">
      <button class="small" data-action="run" data-id="${escapeHtml(batch.id)}">
        ${batch.status === "accepted" ? "开始校验"
          : batch.status === "failed" ? "修复后重跑校验" : "重跑并替换本批次结果"}
      </button>
      ${batch.status === "published" ? "" : `
      <label class="row small">
        <input type="file" class="small" data-replace="${escapeHtml(batch.id)}" accept=".parquet" multiple />
        <span class="muted">选择修复后的 parquet 覆盖源文件并重跑</span>
      </label>`}
    </div>
    <p class="hint">重跑只替换该批次自身的结果，不会影响其他批次。规则与参考表保持受理时的固定版本。</p>
    <details>
      <summary>固定规则快照</summary>
      <pre>${escapeHtml(JSON.stringify(batch.rules_spec, null, 2))}</pre>
    </details>`;

  const runButton = $("[data-action='run']", $("#batch-detail"));
  if (runButton) {
    runButton.addEventListener("click", async () => {
      runButton.disabled = true;
      runButton.textContent = "校验中…";
      try {
        const response = await api(
          `/api/batches/${encodeURIComponent(batch.id)}/run?replace_published=${published}`,
          { method: "POST" }
        );
        const status = response?.batch?.status;
        if (status === "published") toast("批次重跑完成，已替换本批次结果", "ok");
        else toast(`重跑未通过：${response?.run_error?.code || status}`, "bad");
        await Promise.all([loadSummary(), loadBatches(), selectBatch(batch.id), renderPartitions()]);
      } catch (error) {
        toast(`${error.code}: ${error.message}`, "bad");
        $("#batch-detail").insertAdjacentHTML("beforeend",
          `<div class="callout bad">${escapeHtml(error.code)}<br>${escapeHtml(error.message)}</div>`);
        runButton.disabled = false;
        runButton.textContent = "重跑";
      }
    });
  }
  const fileInput = $("[data-replace]", $("#batch-detail"));
  if (fileInput) {
    fileInput.addEventListener("change", async () => {
      if (!fileInput.files.length) return;
      const body = new FormData();
      Array.from(fileInput.files).forEach((file) => body.append("files", file));
      body.append("process", "true");
      try {
        const response = await api(`/api/batches/${encodeURIComponent(batch.id)}/files`, {
          method: "POST",
          body,
        });
        if (response.batch.status === "published") toast("已替换源文件并重跑成功", "ok");
        else toast(`已替换源文件，但校验仍未通过：${response.batch.status}`, "bad");
        await Promise.all([loadSummary(), loadBatches(), selectBatch(response.batch.id), renderPartitions()]);
      } catch (error) {
        toast(`${error.code}: ${error.message}`, "bad");
      }
    });
  }
}

async function loadQuarantine(batchId) {
  const card = $("#quarantine-card");
  const container = $("#quarantine-table");
  try {
    const data = await api(`/api/batches/${encodeURIComponent(batchId)}/quarantine`);
    card.hidden = false;
    $("#quarantine-caption").textContent =
      `${data.rows.length} 行（当前尝试）· 失败行不会写入可信数据`;
    if (!data.rows.length) {
      container.innerHTML = `<p class="muted">当前尝试没有隔离行。</p>`;
      return;
    }
    container.innerHTML = `
      <table>
        <thead><tr><th>行号</th><th>源文件</th><th class="wrap">原因</th><th class="wrap">原始数据</th></tr></thead>
        <tbody>${data.rows.map((row) => `
          <tr>
            <td>${row.row_index}</td>
            <td>${escapeHtml(row.source_file || "—")}</td>
            <td class="wrap">${renderReasons(row.reasons)}</td>
            <td class="wrap"><code>${escapeHtml(JSON.stringify(row.raw_row))}</code></td>
          </tr>`).join("")}
        </tbody>
      </table>`;
  } catch (error) {
    card.hidden = false;
    container.innerHTML = `<div class="callout bad">${escapeHtml(error.message)}</div>`;
  }
}

function renderReasons(reasons) {
  if (!reasons || !reasons.length) return `<span class="muted">—</span>`;
  return `<ul class="reasons">${reasons.map((reason) => `
    <li>
      <span class="reason-code">${escapeHtml(reason.code)}</span>
      ${reason.field ? `<span class="reason-field">[${escapeHtml(reason.field)}]</span>` : ""}
      ${escapeHtml(reason.message || "")}
    </li>`).join("")}</ul>`;
}

// ---------------------------------------------------------------------------
// One-time channel-key switch
// ---------------------------------------------------------------------------
async function loadChannelSwitch() {
  const select = $("#rules-dataset");
  const status = $("#channel-switch-status");
  const form = $("#channel-switch-form");
  const note = $("#channel-switch-note");
  const submit = $("#channel-switch-submit");
  if (!select.value && state.datasets.length) select.value = state.datasets[0].name;
  const dataset = select.value;
  if (!dataset) {
    status.className = "callout";
    status.textContent = "没有可用数据集。";
    form.hidden = true;
    return;
  }
  try {
    const data = await api(`/api/datasets/${encodeURIComponent(dataset)}/channel-switch`);
    state.channelSwitch = data.switch;
    if (data.switch) {
      status.className = "callout ok";
      status.innerHTML = `已生效：复合主键 <code>(channel, order_id)</code> · 规则版本 v${escapeHtml(
        data.switch.rules_version
      )} · 生效时间 ${escapeHtml(fmtTime(data.switch.switched_at))}`;
      form.hidden = true;
    } else {
      status.className = "callout warn";
      status.innerHTML = `当前为旧口径：全表唯一键仅 <code>order_id</code>（字符串），数据中没有 <code>channel</code>。`;
      form.hidden = false;
      note.value = "";
      submit.disabled = false;
      submit.textContent = "执行一次性切换";
    }
  } catch (error) {
    status.className = "callout bad";
    status.textContent = error.message;
    form.hidden = false;
  }
}

$("#channel-switch-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const dataset = $("#rules-dataset").value;
  if (!dataset) return;
  const confirmed = window.confirm(
    `确认对 ${dataset} 执行一次性切换？\n\n` +
      "• 新受理批次的每行必须包含 channel（direct 或 partner），主键变为 (channel, order_id)\n" +
      "• 存量可信文件会补写 channel=direct\n" +
      "• 切换前受理的批次继续按旧规则校验，发布后归为 direct\n" +
      "• 切换失败时受理、查重和预览全部保持旧口径\n" +
      "• 该操作不可逆且只能执行一次，重启后保持生效"
  );
  if (!confirmed) return;
  const submit = $("#channel-switch-submit");
  submit.disabled = true;
  submit.textContent = "切换中…";
  try {
    const result = await api(
      `/api/datasets/${encodeURIComponent(dataset)}/channel-switch`,
      jsonBody({ note: $("#channel-switch-note").value.trim() || null })
    );
    if (result.already_effective) {
      toast(`切换此前已生效（规则 v${result.rules_version}），未重复执行`, "ok");
    } else {
      toast(
        `切换已生效：规则 v${result.rules_version}，${result.migrated_files} 个可信文件已补写 channel=direct`,
        "ok"
      );
    }
    state.specDirty = false;
    await Promise.all([loadChannelSwitch(), loadRules(), loadDatasets(), loadBatches()]);
  } catch (error) {
    toast(`${error.code}: ${error.message}`, "bad");
    submit.disabled = false;
    submit.textContent = "执行一次性切换";
  }
});

$("#rules-dataset").addEventListener("change", () => {
  state.specDirty = false;
  loadRules();
  loadChannelSwitch();
});

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------
async function loadRules() {
  const select = $("#rules-dataset");
  if (!select.value && state.datasets.length) select.value = state.datasets[0].name;
  const dataset = select.value;
  const container = $("#rules-list");
  if (!dataset) {
    container.innerHTML = `<p class="muted">没有可用数据集。</p>`;
    return;
  }
  try {
    const data = await api(`/api/datasets/${encodeURIComponent(dataset)}/rules`);
    state.rulesVersions = data.versions;
    const detail = await api(`/api/datasets/${encodeURIComponent(dataset)}`);
    const current = detail.current_rules_version;
    const latest = state.rulesVersions[state.rulesVersions.length - 1];
    $("#rules-expected").value = current;
    container.innerHTML = state.rulesVersions.slice().reverse().map((version) => `
      <div class="item">
        <div class="title">
          <span class="name">v${version.version} ${version.version === current
            ? `<span class="badge published">current</span>` : ""}</span>
          <button class="small" data-view="${version.version}">查看 JSON</button>
        </div>
        <div class="meta">${escapeHtml(version.note || "无说明")} · ${fmtTime(version.created_at)}</div>
        <pre class="hidden" data-spec="${version.version}"></pre>
      </div>`).join("");
    $$("[data-view]", container).forEach((button) => {
      button.addEventListener("click", () => {
        const target = $(`[data-spec="${button.dataset.view}"]`);
        const version = state.rulesVersions.find((item) => String(item.version) === button.dataset.view);
        target.textContent = JSON.stringify(version.spec, null, 2);
        target.classList.toggle("hidden");
      });
    });
    if (latest && !state.specDirty) {
      $("#rules-spec").value = JSON.stringify(latest.spec, null, 2);
    }
  } catch (error) {
    container.innerHTML = `<div class="callout bad">${escapeHtml(error.message)}</div>`;
  }
}

$("#rules-spec").addEventListener("input", () => { state.specDirty = true; });

$("#rules-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const dataset = $("#rules-dataset").value;
  let spec;
  try {
    spec = JSON.parse($("#rules-spec").value);
  } catch (error) {
    toast(`规则不是合法 JSON：${error.message}`, "bad");
    return;
  }
  try {
    const result = await api(`/api/datasets/${encodeURIComponent(dataset)}/rules`, jsonBody({
      spec,
      note: $("#rules-note").value || null,
      expected_version: Number($("#rules-expected").value),
    }));
    toast(`已发布规则 v${result.version}（仅影响之后受理的批次）`, "ok");
    state.specDirty = false;
    $("#rules-note").value = "";
    await Promise.all([loadRules(), loadDatasets()]);
  } catch (error) {
    toast(`${error.code}: ${error.message}`, "bad");
  }
});

// ---------------------------------------------------------------------------
// References
// ---------------------------------------------------------------------------
async function loadReferences() {
  try {
    const data = await api("/api/references");
    state.references = data.latest;
    const container = $("#reference-list");
    if (!data.versions.length) {
      container.innerHTML = `<p class="muted">尚无参考表。</p>`;
      return;
    }
    const grouped = {};
    data.versions.forEach((version) => {
      (grouped[version.name] ||= []).push(version);
    });
    container.innerHTML = Object.entries(grouped).map(([name, versions]) => {
      const latest = versions[0];
      return `
        <div class="item">
          <div class="title">
            <span class="name">${escapeHtml(name)} <span class="badge published">v${latest.version}</span></span>
            <span class="badge">${latest.row_count} 行</span>
          </div>
          <div class="meta">主键 <code>${escapeHtml(latest.key_field)}</code> · ${versions.length} 个版本 · ${fmtTime(latest.created_at)}</div>
          <div class="meta">列：${latest.columns.map((column) => escapeHtml(column.name)).join(", ")}</div>
        </div>`;
    }).join("");
  } catch (error) {
    $("#reference-list").innerHTML = `<div class="callout bad">${escapeHtml(error.message)}</div>`;
  }
}

$("#reference-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const file = $("#reference-file").files[0];
  if (!file) return;
  const body = new FormData();
  body.append("file", file);
  body.append("name", $("#reference-name").value.trim());
  body.append("key_field", $("#reference-key").value.trim());
  body.append("note", $("#reference-note").value.trim());
  try {
    const result = await api("/api/references", { method: "POST", body });
    toast(`已发布参考表 ${result.name} v${result.version}`, "ok");
    $("#reference-form").reset();
    await Promise.all([loadReferences(), loadSummary()]);
  } catch (error) {
    toast(`${error.code}: ${error.message}`, "bad");
  }
});

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
(async function init() {
  await loadSummary();
  await loadDatasets();
  await loadBatches();
})();
