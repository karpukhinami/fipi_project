/* =========================================================
   ФИПИ — клиентская логика
   ========================================================= */

'use strict';

// ── Состояние ──────────────────────────────────────────────
const state = {
  page: 1,
  pageSize: 50,
  totalPages: 1,
  currentTask: null,   // полные данные открытого задания
  currentWrapper: null, // обёртка группы открытого задания
  activeRowId: null,   // rowid активной строки в списке
  editMode: false,
};

// ── DOM-ссылки ─────────────────────────────────────────────
const $ = id => document.getElementById(id);

/** Парсит тело ответа как JSON. Если пришла HTML-страница (часто Flask debug при 500), даёт понятную ошибку вместо SyntaxError. */
async function readResponseJson(res) {
  const text = await res.text();
  const head = text.trimStart().slice(0, 64).toLowerCase();
  if (head.startsWith('<!doctype') || head.startsWith('<html') || head.startsWith('<head')) {
    throw new Error(
      'Сервер вернул HTML вместо JSON (HTTP ' + res.status + '). ' +
      'Обычно это страница ошибки Flask: на бэкенде было необработанное исключение — смотрите окно, где запущен python app.py. ' +
      'Либо открыта не та страница (нужен адрес http://localhost:5000/, а не файл с диска).'
    );
  }
  try {
    return text ? JSON.parse(text) : {};
  } catch (e) {
    throw new Error('Ответ не JSON (HTTP ' + res.status + '): ' + (e.message || e));
  }
}

const taskList      = $('task-list');
const taskCard      = $('task-card');
const totalCount    = $('total-count');
const pageInfo      = $('page-info');
const btnPrev       = $('btn-prev-page');
const btnNext       = $('btn-next-page');
const loadingOverlay = $('loading-overlay');
const loadingText    = $('loading-text');

// ── Тост ───────────────────────────────────────────────────
function showToast(msg, type = 'success') {
  const el = $('toast-main');
  $('toast-body').textContent = msg;
  el.className = `toast align-items-center text-bg-${type} border-0`;
  bootstrap.Toast.getOrCreateInstance(el, { delay: 3500 }).show();
}

// ── Оверлей загрузки ───────────────────────────────────────
function setLoading(on, text = 'Загрузка...') {
  loadingOverlay.style.display = on ? 'flex' : 'none';
  loadingText.textContent = text;
}

// ── Фильтры ────────────────────────────────────────────────
function getFilters() {
  return {
    exam_type:   $('f-exam-type').value,
    subject:     $('f-subject').value,
    answer_type: $('f-answer-type').value,
    exam_number: $('f-exam-number').value,
    kes:         $('f-kes').value.trim(),
    group_id:    $('f-group-id').value.trim(),
    task_number: $('f-task-number').value.trim(),
    search:      $('f-search').value.trim(),
  };
}

async function loadFilterOptions() {
  let data;
  try {
    const res = await fetch('/api/filters');
    data = await readResponseJson(res);
  } catch (e) {
    showToast(String(e.message || e), 'danger');
    return;
  }

  function fill(selectId, values) {
    const sel = $(selectId);
    const cur = sel.value;
    const first = sel.options[0].outerHTML;
    sel.innerHTML = first + values.map(v => `<option value="${esc(v)}">${esc(v)}</option>`).join('');
    sel.value = cur;
  }

  fill('f-exam-type',   data.exam_types   || []);
  fill('f-subject',     data.subjects     || []);
  fill('f-answer-type', data.answer_types || []);
  fill('f-exam-number', data.exam_numbers || []);
}

// ── Каталог КЭС ────────────────────────────────────────────
// Хранилище всех загруженных КЭС для клиентского поиска
let _kesCatalogAll = []; // [{subject, kes_text}, ...]

async function loadKesCatalog(subject = '') {
  const url = subject ? `/api/kes_catalog?subject=${encodeURIComponent(subject)}` : '/api/kes_catalog';
  try {
    const res = await fetch(url);
    const data = await readResponseJson(res);
    _kesCatalogAll = data.items || [];
  } catch (_) {
    _kesCatalogAll = [];
  }
  renderKesList('');
}

function renderKesList(filterText) {
  const sel = $('f-kes');
  const cur = sel.value;
  const q = filterText.trim().toLowerCase();
  const filtered = q
    ? _kesCatalogAll.filter(item => item.kes_text.toLowerCase().includes(q))
    : _kesCatalogAll;
  sel.innerHTML = '<option value="">— все КЭС —</option>' +
    filtered.map(item =>
      `<option value="${esc(item.kes_text)}" title="${esc(item.subject)}">${esc(item.kes_text)}</option>`
    ).join('');
  // Восстановить выбранное значение, если оно всё ещё в списке
  if (cur && filtered.some(i => i.kes_text === cur)) sel.value = cur;
}

// ── Список заданий ─────────────────────────────────────────
async function loadTasks(page = 1) {
  state.page = page;
  const params = new URLSearchParams({ ...getFilters(), page, page_size: state.pageSize });
  let data;
  try {
    const res = await fetch(`/api/tasks?${params}`);
    data = await readResponseJson(res);
  } catch (e) {
    showToast(String(e.message || e), 'danger');
    return;
  }

  state.totalPages = data.pages || 1;
  totalCount.textContent = data.total || 0;
  pageInfo.textContent = `стр. ${data.page} / ${state.totalPages}`;
  btnPrev.disabled = data.page <= 1;
  btnNext.disabled = data.page >= state.totalPages;
  const pageInput = $('page-input');
  if (pageInput) {
    pageInput.value = data.page;
    pageInput.max = Math.max(1, state.totalPages);
    pageInput.placeholder = `1–${state.totalPages || 1}`;
  }

  renderTaskList(data.tasks || []);
}

function renderTaskList(tasks) {
  if (!tasks.length) {
    taskList.innerHTML = '<div class="empty-state"><i class="bi bi-inbox"></i>Нет заданий</div>';
    return;
  }

  taskList.innerHTML = tasks.map(t => {
    const isWrapper = !t.task_number && t.group_position === '0';
    const num = t.task_number ? `<span class="badge bg-primary me-1">#${esc(t.task_number)}</span>` : '';
    const examNumBadge = t.exam_number ? `<span class="badge bg-info text-dark me-1">№${esc(t.exam_number)}</span>` : '';
    const examBadge = t.exam_type ? `<span class="badge bg-secondary me-1">${esc(t.exam_type)}</span>` : '';
    const subj = t.subject ? `<small class="text-muted">${esc(t.subject)}</small>` : '';
    const grp = t.group_id ? `<span class="badge bg-success-subtle text-success-emphasis ms-1" title="Группа">G</span>` : '';
    const editedBadge = t.manually_edited
      ? `<span class="badge bg-warning text-dark ms-1" title="Отредактировано вручную"><i class="bi bi-pencil-fill"></i></span>`
      : '';
    const kesTxt = t.kes ? `<div style="font-size:.72rem;color:#888;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:230px">${esc(t.kes)}</div>` : '';
    const previewSrc = (t.formatted_text || '').trim() || (t.text || '');
    const preview = previewSrc.replace(/\[img[^\]]*\]/g, '🖼').replace(/\[TABLE[^\]]*\]/g, '📋').slice(0, 100);

    return `<div class="task-row p-2 border-bottom${isWrapper ? ' bg-light' : ''}${t.manually_edited ? ' task-row-edited' : ''}" 
               data-id="${esc(t.id)}" data-group-id="${esc(t.group_id)}" data-group-pos="${esc(t.group_position)}"
               onclick="openTask('${esc(t.id)}','${esc(t.group_id)}','${esc(t.group_position)}',this)">
      <div class="d-flex align-items-center flex-wrap gap-1">
        ${examBadge}${examNumBadge}${num}${subj}${grp}${editedBadge}
        ${isWrapper ? '<span class="badge bg-warning text-dark ms-1">условие</span>' : ''}
      </div>
      ${kesTxt}
      <div style="font-size:.8rem;color:#444;margin-top:2px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis">${esc(preview)}</div>
    </div>`;
  }).join('');
}

// ── Открыть задание ────────────────────────────────────────
async function openTask(id, groupId, groupPos, rowEl) {
  // Снять выделение
  document.querySelectorAll('.task-row.active').forEach(el => el.classList.remove('active'));
  if (rowEl) rowEl.classList.add('active');

  setLoading(true, 'Загружаем задание...');
  try {
    const params = new URLSearchParams({ id, group_id: groupId, group_position: groupPos });
    const res = await fetch(`/api/tasks/detail?${params}`);
    const task = await readResponseJson(res);
    if (!res.ok) {
      showToast(task.error || 'Задание не найдено', 'danger');
      return;
    }
    state.currentTask = task;
    state.currentWrapper = null;
    state.editMode = false;

    // Если задание входит в группу и само не является обёрткой — подгружаем обёртку
    const isWrapper = !task.task_number && task.group_position === '0';
    let wrapper = null;
    if (groupId && !isWrapper) {
      try {
        const wRes = await fetch(`/api/tasks/group_wrapper?group_id=${encodeURIComponent(groupId)}`);
        const wData = await readResponseJson(wRes);
        // Не показываем обёртку саму на себя
        if (wData.wrapper && wData.wrapper.group_position === '0') {
          wrapper = wData.wrapper;
          state.currentWrapper = wrapper;
        }
      } catch (_) {}
    }

    renderTaskCard(task, wrapper);
  } catch (e) {
    showToast('Ошибка загрузки задания', 'danger');
  } finally {
    setLoading(false);
  }
}

// ── Блок разметки после анализа + учёт токенов/стоимости ───
function _analysisTokenLine(task) {
  let usage = null;
  try {
    usage = task.analysis_usage_json ? JSON.parse(task.analysis_usage_json) : null;
  } catch (_) {}
  const rub = usage && usage.cost_rub != null ? Number(usage.cost_rub).toFixed(2) : '—';
  const p = (usage && usage.primary) || {};
  const m = (usage && usage.merge) || {};
  const tin = (p.input_tokens || 0) + (m.input_tokens || 0);
  const tout = (p.output_tokens || 0) + (m.output_tokens || 0);
  return { rub, tin, tout };
}

/** Свернутый блок с полным JSON разметки для тех, кому нужна структура как у эталона task_markup. */
function buildCollapsedFullAnalysisJson(task, summaryText) {
  const raw = task && task.analysis_raw_json;
  if (!raw || !String(raw).trim()) return '';
  let pretty = String(raw);
  try {
    pretty = JSON.stringify(JSON.parse(pretty), null, 2);
  } catch (_) {}
  const label = summaryText || 'Полный JSON разметки (как сохранено в БД)';
  return `<details class="mt-2 border-top pt-2"><summary class="cursor-pointer small text-muted user-select-none">${esc(label)}</summary><pre class="small bg-body-secondary p-2 rounded overflow-auto border mt-1 mb-0" style="max-height:24rem;white-space:pre-wrap">${esc(pretty)}</pre></details>`;
}

function buildAnalysisMarkupSection(task) {
  let usage = null;
  try {
    usage = task.analysis_usage_json ? JSON.parse(task.analysis_usage_json) : null;
  } catch (_) {}
  let result = null;
  try {
    result = task.analysis_result_json ? JSON.parse(task.analysis_result_json) : null;
  } catch (_) {}
  if (!usage && !result && !task.analyzed_topic_id) {
    return '<div class="text-muted small mb-2">Разметка ещё не выполнялась — нажмите «Анализ и разметка».</div>';
  }
  const { rub, tin, tout } = _analysisTokenLine(task);
  const topicLinked = task.analysis_topic_linked !== false;

  if (!topicLinked) {
    let fb = null;
    try {
      fb = task.analysis_fallback_json ? JSON.parse(task.analysis_fallback_json) : null;
    } catch (_) {}
    let inner = '';
    if (fb) {
      const cur = fb['куррикулум_текстом'] || {};
      inner += `<div class="mb-2"><strong>Куррикулум (текстовый снимок):</strong> ${esc(cur['предмет_в_строке_справочника'] || '')} — ${esc(cur['раздел'] || '')} / ${esc(cur['подраздел'] || '')} — ${esc(cur['тема'] || '')} · класс ${esc(String(cur['класс'] || ''))}</div>`;
      if (cur['описание_темы']) {
        inner += `<div class="text-muted small mb-2">${esc(cur['описание_темы'])}</div>`;
      }
      if (fb['topic_id_на_момент_анализа'] != null) {
        inner += `<div class="small text-muted mb-2">topic_id на момент анализа: ${esc(String(fb['topic_id_на_момент_анализа']))}</div>`;
      }
      const fce = fb['элементы_содержания'] || [];
      inner += '<div class="section-label">Элементы содержания (снимок)</div>';
      inner += fce.length
        ? `<ul class="mb-2 ps-3">${fce.map(e =>
            `<li><span class="badge bg-secondary">${esc(e['важность'] || '')}</span> ${esc(e['название'] || '')} <span class="text-muted">(${esc(e['этап'] || '')})</span></li>`
          ).join('')}</ul>`
        : '<p class="text-muted small mb-2">—</p>';
      const fsk = fb['навыки_по_шагам_решения'] || [];
      inner += '<div class="section-label">Навыки по шагам (снимок)</div>';
      inner += fsk.length
        ? `<ol class="mb-2 ps-3">${fsk.map(s => {
            const pr = (s['пререквизиты_к_индексам_шагов'] && s['пререквизиты_к_индексам_шагов'].length)
              ? ` <span class="text-muted">← к шагам ${s['пререквизиты_к_индексам_шагов'].map(i => i + 1).join(', ')}</span>`
              : '';
            return `<li>${esc(s['действие'] || '')}${pr}</li>`;
          }).join('')}</ol>`
        : '<p class="text-muted small mb-2">—</p>';
      const fmt = fb['тип_вопроса_и_формат'];
      if (fmt && typeof fmt === 'object' && Object.keys(fmt).length) {
        inner += '<div class="section-label">Тип вопроса и формат (снимок)</div>';
        inner += `<ul class="small ps-3 mb-2">${Object.entries(fmt).map(([k, v]) =>
          `<li><code>${esc(k)}</code>: ${esc(typeof v === 'string' ? v : JSON.stringify(v))}</li>`
        ).join('')}</ul>`;
      }
      const steps = fb['шаги_решения_текстом'];
      if (Array.isArray(steps) && steps.length) {
        inner += '<div class="section-label">Шаги решения (снимок)</div><ol class="ps-3 mb-2 small">';
        inner += steps.map(s => `<li>${esc(String(s))}</li>`).join('');
        inner += '</ol>';
      }
      if (fb['ответ'] != null && fb['ответ'] !== '') {
        inner += `<div class="mb-0"><strong>Ответ (снимок):</strong> ${esc(String(fb['ответ']))}</div>`;
      }
      const tm = fb['разметка_task_markup'];
      if (tm && typeof tm === 'object' && Object.keys(tm).length) {
        inner += `<details class="mt-2"><summary class="cursor-pointer small text-muted">Срез разметки (ключи как в эталоне task_markup)</summary><pre class="small bg-body-secondary p-2 rounded border mt-1 mb-0 overflow-auto" style="max-height:20rem;white-space:pre-wrap">${esc(JSON.stringify(tm, null, 2))}</pre></details>`;
      }
      inner += buildCollapsedFullAnalysisJson(task);
    } else {
      inner = `<div class="mb-2"><strong>Куррикулум (поля задания):</strong> ${esc(task.analyzed_section || '')} / ${esc(task.analyzed_subsection || '')} — ${esc(task.analyzed_topic || '')} · класс ${esc(String(task.analyzed_grade_class || ''))}</div>`;
      inner += '<p class="text-muted small mb-2">JSON-снимок не сохранён (анализ до обновления приложения). Связи с навыками/элементами в таблицах могут указывать на устаревшие id.</p>';
      inner += buildCollapsedFullAnalysisJson(task);
    }
    return `
    <div class="card mb-3 border-warning" id="analysis-markup-card">
      <div class="card-header py-2 d-flex flex-wrap justify-content-between align-items-center gap-2">
        <span><i class="bi bi-bookmark-x me-1"></i>Разметка (не привязана к справочнику тем)</span>
        <span class="small text-muted">Токены: ${tin} вх. + ${tout} вых. · ~${rub} ₽ <span class="text-secondary">(оценка, 80&nbsp;₽/$)</span></span>
      </div>
      <div class="alert alert-warning mb-0 rounded-0 small border-0 border-bottom">
        <strong>Тема удалена из справочника</strong> или ссылка <code>analyzed_topic_id</code> больше не действует. Ниже показан <strong>текстовый снимок</strong> на момент анализа (не синхронизируется с базой). Связи задания с таблицами навыков/элементов могут не соответствовать актуальным справочникам.
      </div>
      <div class="card-body py-2 small">${inner}</div>
    </div>`;
  }

  const fullJsonLinked = buildCollapsedFullAnalysisJson(task);

  let curriculum = (result && result.curriculum) || {};
  if (!curriculum.topic && task.analyzed_topic) {
    curriculum = {
      section: task.analyzed_section,
      subsection: task.analyzed_subsection,
      topic: task.analyzed_topic,
      grade_class: task.analyzed_grade_class,
    };
  }
  const norm = result && result.normalized;
  const ce = (norm && norm.content_elements) || [];
  const st = (norm && norm.skill_steps) || [];
  const ceHtml = ce.length
    ? `<ul class="mb-1 ps-3">${ce.map(e =>
        `<li><span class="badge bg-${e.importance === 'key' ? 'primary' : 'secondary'}">${esc(e.importance || '')}</span> ${esc(e.label)} <span class="text-muted">(${esc(e.stage || '')})</span></li>`
      ).join('')}</ul>`
    : '<p class="text-muted small mb-1">Нет элементов содержания</p>';
  const skHtml = st.length
    ? `<ol class="mb-0 ps-3">${st.map(s => {
        const pr = (s.prereq_indices && s.prereq_indices.length)
          ? ` <span class="text-muted">← пререкв. к шагам ${s.prereq_indices.map(i => i + 1).join(', ')}</span>`
          : '';
        return `<li>${esc(s.label)}${pr}</li>`;
      }).join('')}</ol>`
    : '<p class="text-muted small mb-0">Нет шагов навыков</p>';

  return `
    <div class="card mb-3 border-info" id="analysis-markup-card">
      <div class="card-header py-2 d-flex flex-wrap justify-content-between align-items-center gap-2">
        <span><i class="bi bi-tags me-1"></i>Разметка (анализ)</span>
        <span class="small text-muted">Токены: ${tin} вх. + ${tout} вых. · ~${rub} ₽ <span class="text-secondary">(оценка, 80&nbsp;₽/$)</span></span>
      </div>
      <div class="card-body py-2 small">
        <div class="mb-2"><strong>Куррикулум:</strong> ${esc(curriculum.section || '')} / ${esc(curriculum.subsection || '')} — ${esc(curriculum.topic || '')} · класс ${esc(String(curriculum.grade_class || ''))}</div>
        <div class="section-label">Элементы содержания</div>${ceHtml}
        <div class="section-label mt-2">Мини-дерево навыков (решение)</div>${skHtml}
        ${result && result.task_markup_ref && typeof result.task_markup_ref === 'object' && Object.keys(result.task_markup_ref).length
          ? `<details class="mt-2"><summary class="cursor-pointer small text-muted">Срез разметки (ключи как в эталоне task_markup)</summary><pre class="small bg-body-secondary p-2 rounded border mt-1 mb-0 overflow-auto" style="max-height:20rem;white-space:pre-wrap">${esc(JSON.stringify(result.task_markup_ref, null, 2))}</pre></details>`
          : ''}
        ${fullJsonLinked}
      </div>
    </div>`;
}

async function runTaskAnalyze() {
  const apiKey = localStorage.getItem('claude_api_key') || '';
  if (!apiKey) {
    showToast('Укажите API-ключ в настройках Claude', 'warning');
    return;
  }
  const task = state.currentTask;
  if (!task) return;
  const model = localStorage.getItem('claude_model') || 'claude-sonnet-4-0';
  const proxyUrl = (localStorage.getItem('claude_proxy_url') || '').trim();
  setLoading(true, 'Анализ задания (Claude)…');
  try {
    const payload = {
      api_key: apiKey,
      model,
      id: task.id || '',
      group_id: task.group_id || '',
      group_position: task.group_position || '',
    };
    if (proxyUrl) payload.proxy_url = proxyUrl;
    const res = await fetch('/api/tasks/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await readResponseJson(res);
    if (!res.ok || !data.ok) {
      showToast(data.error || 'Ошибка анализа', 'danger');
      return;
    }
    state.currentTask = data.task;
    renderTaskCard(state.currentTask, state.currentWrapper);
    const rub = data.cost_rub != null ? Number(data.cost_rub).toFixed(2) : '?';
    showToast(`Анализ готов · ~${rub} ₽`);
  } catch (e) {
    showToast(String(e.message || e), 'danger');
  } finally {
    setLoading(false);
  }
}
window.runTaskAnalyze = runTaskAnalyze;

async function clearTaskAnalysis() {
  const task = state.currentTask;
  if (!task) return;
  if (!confirm('Удалить всю разметку и результат анализа для этого задания? Будут сброшены тема, JSON разметки, токены и связи с элементами содержания и навыками. Текст задания и решение в карточке не меняются.')) return;
  setLoading(true, 'Очистка разметки…');
  try {
    const res = await fetch('/api/tasks/clear-analysis', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: task.id || '',
        group_id: task.group_id || '',
        group_position: task.group_position || '',
      }),
    });
    const data = await readResponseJson(res);
    if (!res.ok || !data.ok) {
      showToast(data.error || 'Ошибка', 'danger');
      return;
    }
    state.currentTask = data.task;
    renderTaskCard(state.currentTask, state.currentWrapper);
    showToast('Разметка очищена');
  } catch (e) {
    showToast(String(e.message || e), 'danger');
  } finally {
    setLoading(false);
  }
}
window.clearTaskAnalysis = clearTaskAnalysis;

// ── Рендер карточки задания ────────────────────────────────
function renderTaskCard(task, wrapper) {
  if (!task) {
    taskCard.innerHTML = '<div class="empty-state"><i class="bi bi-hand-index-thumb"></i>Выберите задание из списка слева</div>';
    return;
  }
  const isWrapper = !task.task_number && task.group_position === '0';

  // Форматирование даты редактирования
  let editedLabel = '';
  if (task.manually_edited) {
    const dateStr = task.edited_at
      ? new Date(task.edited_at).toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' })
      : '';
    editedLabel = `<span class="badge bg-warning text-dark" title="Отредактировано вручную${dateStr ? ' · ' + dateStr : ''}"><i class="bi bi-pencil-fill me-1"></i>Ручная правка${dateStr ? ' · ' + dateStr : ''}</span>`;
  }

  const metaItems = [
    task.exam_type   ? `<span class="badge bg-primary">${esc(task.exam_type)}</span>` : '',
    task.subject     ? `<span class="badge bg-secondary">${esc(task.subject)}</span>` : '',
    task.task_number ? `<span class="badge bg-info text-dark">№${esc(task.task_number)}</span>` : '',
    task.group_id    ? `<span class="badge bg-success-subtle text-success-emphasis border">Группа: ${esc(task.group_id)}, поз. ${esc(task.group_position)}</span>` : '',
    task.answer_type ? `<span class="badge bg-light text-dark border">${esc(task.answer_type)}</span>` : '',
    isWrapper        ? `<span class="badge bg-warning text-dark">условие группы</span>` : '',
    editedLabel,
  ].filter(Boolean).join(' ');

  const kesBlock = task.kes
    ? `<div class="text-muted mb-2" style="font-size:.82rem"><i class="bi bi-tag me-1"></i>${esc(task.kes)}</div>`
    : '';

  const topicCategoryBlock = [task.topic_name, task.category_name].filter(Boolean).length
    ? `<div class="text-muted mb-2" style="font-size:.82rem">
         ${task.topic_name ? `<span><i class="bi bi-folder me-1"></i>${esc(task.topic_name)}</span>` : ''}
         ${task.topic_name && task.category_name ? ' · ' : ''}
         ${task.category_name ? `<span><i class="bi bi-bookmark me-1"></i>${esc(task.category_name)}</span>` : ''}
       </div>`
    : '';

  // Блок обёртки группы (условие)
  let wrapperBlock = '';
  if (wrapper && !isWrapper) {
    const wrapperText = renderTaskDisplayText(wrapper);
    wrapperBlock = `
      <div class="group-condition mb-3">
        <div class="label"><i class="bi bi-file-text me-1"></i>Условие группы заданий · ${esc(wrapper.group_id)}</div>
        <div class="task-text-rendered" id="wrapper-text-rendered">${wrapperText}</div>
      </div>`;
  }

  const renderedText = renderTaskDisplayText(task);
  const images = task.images || {};
  const audio = task.audio || {};
  const renderedAnswer = renderTaskText(task.answer || '', images, audio);
  const renderedSolution = renderTaskText(cleanSolutionText(task.solution || ''), images, audio);

  const answerBlock = !isWrapper ? `
    <div class="mt-3">
      <div class="section-label">Ответ</div>
      <div class="edit-field task-text-rendered" id="edit-answer" contenteditable="false">${renderedAnswer || '<span class="text-muted">—</span>'}</div>
    </div>
    <div class="mt-2">
      <div class="section-label">Решение</div>
      <div class="edit-field task-text-rendered" id="edit-solution" contenteditable="false">${renderedSolution || '<span class="text-muted">—</span>'}</div>
    </div>
  ` : '';

  const textEditBlock = `
    <div class="mt-3" id="text-edit-block" style="display:none">
      <div class="section-label">Форматированный текст</div>
      <textarea class="form-control font-monospace mb-2" id="edit-formatted-text" rows="6" style="font-size:.85rem">${esc(task.formatted_text || '')}</textarea>
      <div class="section-label">Необработанный текст</div>
      <textarea class="form-control font-monospace" id="edit-text" rows="10" style="font-size:.85rem">${esc(task.text || '')}</textarea>
    </div>`;

  const attachments = task.attachments || {};
  const attachmentNames = Object.keys(attachments).filter(k => attachments[k] && attachments[k].data);
  const attachmentBlock = !isWrapper && attachmentNames.length ? `
    <div class="mt-3">
      <div class="section-label">Вложения</div>
      <div class="d-flex flex-wrap gap-2" id="attachment-buttons">
        ${attachmentNames.map(fn => `
          <a href="/api/tasks/attachment?id=${encodeURIComponent(task.id || '')}&group_id=${encodeURIComponent(task.group_id || '')}&group_position=${encodeURIComponent(task.group_position || '')}&filename=${encodeURIComponent(fn)}" 
             class="btn btn-sm btn-outline-primary" download="${esc(fn)}" target="_blank">
            <i class="bi bi-download me-1"></i>Скачать ${esc(fn)}
          </a>
          ${fn.toLowerCase().endsWith('.zip') ? `
          <button type="button" class="btn btn-sm btn-outline-secondary" onclick="openZipPreview(${JSON.stringify(fn).replace(/"/g, '&quot;')})" title="Просмотр содержимого архива">
            <i class="bi bi-folder2-open me-1"></i>Открыть архив
          </button>` : ''}
        `).join('')}
      </div>
    </div>
  ` : '';

  taskCard.innerHTML = `
    <div class="d-flex justify-content-between align-items-start mb-2 flex-wrap gap-2">
      <div>${metaItems}</div>
      <div class="d-flex gap-2">
        <button class="btn btn-sm btn-outline-secondary" onclick="toggleEditMode()" id="btn-edit-mode" title="Редактировать">
          <i class="bi bi-pencil"></i> Редактировать
        </button>
        <button class="btn btn-sm btn-success" id="btn-save" style="display:none" onclick="saveTask()">
          <i class="bi bi-floppy"></i> Сохранить
        </button>
      </div>
    </div>

    ${wrapperBlock}

    ${kesBlock}
    ${topicCategoryBlock}

    <div class="task-text-rendered mb-3" id="task-text-rendered">${renderedText}</div>
    ${textEditBlock}
    ${answerBlock}

    ${attachmentBlock}

    <!-- Кнопка отправки в Claude -->
    ${!isWrapper ? `
    <hr/>
    <div>
      <div class="d-flex align-items-center gap-2 mb-2 flex-wrap">
        <button class="btn btn-sm btn-outline-primary" onclick="toggleClaudePanel()">
          <i class="bi bi-robot me-1"></i>Отправить в Claude
        </button>
        <button class="btn btn-sm btn-primary" onclick="runTaskAnalyze()" id="btn-analyze-task" title="Решение + разметка + справочники">
          <i class="bi bi-diagram-2 me-1"></i>Анализ и разметка
        </button>
        <button class="btn btn-sm btn-outline-danger" onclick="clearTaskAnalysis()" id="btn-clear-analysis" title="Удалить сохранённый анализ, JSON и связи с элементами/навыками">
          <i class="bi bi-eraser me-1"></i>Очистить разметку
        </button>
      </div>
      ${buildAnalysisMarkupSection(task)}
      <div id="claude-panel" style="display:none">
        <div class="mb-2">
          <label class="section-label">Промпт (инструкция для Claude)</label>
          <textarea class="form-control form-control-sm" id="claude-prompt" rows="3"></textarea>
        </div>
        <button class="btn btn-sm btn-primary" onclick="sendToClaude()">
          <i class="bi bi-send me-1"></i>Отправить
        </button>
        <div id="claude-result" class="mt-3" style="display:none">
          <div class="section-label">Ответ Claude</div>
          <div class="claude-response" id="claude-response-text"></div>
        </div>
      </div>
    </div>
    ` : ''}
  `;

  // Подставляем промпт по умолчанию
  const promptEl = $('claude-prompt');
  if (promptEl) {
    promptEl.value = localStorage.getItem('claude_default_prompt') ||
      'Проверь и распознай формулы в задании. Реши задание, показав ход решения.';
  }

  // Рендерим формулы через MathJax (включая ответ и решение)
  if (window.MathJax && window.MathJax.typesetPromise) {
    const nodes = [$('task-text-rendered'), $('wrapper-text-rendered'), $('edit-answer'), $('edit-solution')].filter(Boolean);
    window.MathJax.typesetPromise(nodes).catch(() => {});
  }
}

// ── Просмотр содержимого zip-архива ─────────────────────────
async function openZipPreview(filename) {
  const task = state.currentTask;
  if (!task || !window.JSZip) return;
  const url = `/api/tasks/attachment?id=${encodeURIComponent(task.id || '')}&group_id=${encodeURIComponent(task.group_id || '')}&group_position=${encodeURIComponent(task.group_position || '')}&filename=${encodeURIComponent(filename)}`;
  const listEl = $('zip-preview-list');
  const loadingEl = $('zip-preview-loading');
  if (!listEl || !loadingEl) return;
  listEl.innerHTML = '';
  loadingEl.style.display = '';
  const modal = new bootstrap.Modal($('modal-zip-preview'));
  modal.show();
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error('Не удалось загрузить архив');
    const blob = await res.blob();
    const zip = await JSZip.loadAsync(blob);
    const files = [];
    zip.forEach((path, entry) => { if (!entry.dir) files.push({ path, entry }); });
    loadingEl.style.display = 'none';
    listEl.innerHTML = files.map(f => `
      <a href="#" class="list-group-item list-group-item-action d-flex align-items-center" data-path="${esc(f.path)}">
        <i class="bi bi-file-earmark me-2"></i>${esc(f.path)}
      </a>
    `).join('');
    listEl.querySelectorAll('a').forEach(a => {
      a.addEventListener('click', async (e) => {
        e.preventDefault();
        const path = a.dataset.path;
        const entry = zip.file(path);
        if (!entry) return;
        const blob = await entry.async('blob');
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = path.split('/').pop() || path;
        link.click();
        URL.revokeObjectURL(url);
      });
    });
  } catch (err) {
    loadingEl.style.display = 'none';
    listEl.innerHTML = `<div class="text-danger">${esc(err.message)}</div>`;
  }
}
window.openZipPreview = openZipPreview;

// ── Отображение текста задания (formatted_text первым с картинками, затем text с разделителем) ──
function renderTaskDisplayText(task) {
  const formatted = (task.formatted_text || '').trim();
  const rawText = (task.text || '').trim();
  const textPart = renderTaskText(task.text || '', task.images || {}, task.audio || {}, task);
  if (formatted) {
    const formattedPart = renderTaskText(formatted, task.images || {}, task.audio || {}, task);
    // Если text и formatted_text совпадают — выводим только один раз
    if (rawText && formatted !== rawText) {
      return formattedPart +
        '<hr class="my-3"/><div class="section-label">Необработанный текст</div>' +
        '<div class="mt-1">' + textPart + '</div>';
    }
    return formattedPart;
  }
  return textPart;
}

// ── Очистка текста решения от артефактов ────────────────────
function cleanSolutionText(text) {
  if (!text) return '';
  return text
    .replace(/Решениеrule_info\.\s*/g, '')  // убираем артефакт целиком
    .replace(/npnp/g, '');                   // убираем артефакт
}

// ── Рендер текста с маркерами ──────────────────────────────
function renderTaskText(text, images, audio, task) {
  if (!text) return '<em class="text-muted">Текст отсутствует</em>';

  // Разбиваем по маркерам
  const parts = splitByMarkers(text);
  let html = '';

  for (const part of parts) {
    if (part.type === 'text') {
      html += renderTextSegment(part.value);
    } else if (part.type === 'img') {
      html += renderImage(part.filename, images, false);
    } else if (part.type === 'img_inline') {
      // inline-изображение — вставляем прямо в поток текста
      html += renderImage(part.filename, images, true);
    } else if (part.type === 'audio') {
      html += renderAudio(part.filename, audio);
    } else if (part.type === 'attachment') {
      html += renderAttachment(part.value, task);
    } else if (part.type === 'table') {
      html += renderTable(part.value);
    }
  }

  return html;
}

function splitByMarkers(text) {
  const re = /(\[img_inline:[^\]]+\]|\[img:[^\]]+\]|\[audio:[^\]]+\]|\[attachment:[^\]]+\]|\[TABLE>>>[\s\S]*?<<<TABLE\])/g;
  const parts = [];
  let last = 0;
  let m;

  while ((m = re.exec(text)) !== null) {
    if (m.index > last) {
      parts.push({ type: 'text', value: text.slice(last, m.index) });
    }
    const token = m[1];
    if (token.startsWith('[img_inline:')) {
      parts.push({ type: 'img_inline', filename: token.slice(12, -1) });
    } else if (token.startsWith('[img:')) {
      parts.push({ type: 'img', filename: token.slice(5, -1) });
    } else if (token.startsWith('[audio:')) {
      parts.push({ type: 'audio', filename: token.slice(7, -1) });
    } else if (token.startsWith('[attachment:')) {
      parts.push({ type: 'attachment', value: token.slice(12, -1) });
    } else if (token.startsWith('[TABLE>>>')) {
      parts.push({ type: 'table', value: token.slice(9, -9) }); // убираем [TABLE>>> и <<<TABLE]
    }
    last = m.index + m[0].length;
  }

  if (last < text.length) {
    parts.push({ type: 'text', value: text.slice(last) });
  }
  return parts;
}

function renderTextSegment(text) {
  // Параграфы
  const lines = text.split('\n');
  let out = '';
  let inPara = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();
    if (!trimmed) {
      if (inPara) { out += '</p>'; inPara = false; }
      continue;
    }
    if (!inPara) { out += '<p class="mb-1">'; inPara = true; }
    else { out += '<br/>'; }
    out += escHtml(trimmed);
  }
  if (inPara) out += '</p>';
  return out;
}

function renderImage(filename, images, inline) {
  const imgData = images[filename];
  if (!imgData || !imgData.data) {
    return `<span class="text-danger">[изображение не найдено: ${esc(filename)}]</span>`;
  }
  const src = `data:${imgData.mime || 'image/png'};base64,${imgData.data}`;
  if (inline) {
    return `<img src="${src}" class="inline-img" alt="${esc(filename)}" title="${esc(filename)}"/>`;
  } else {
    return `<img src="${src}" class="block-img" alt="${esc(filename)}" title="${esc(filename)}"/>`;
  }
}

function isUrl(s) {
  return typeof s === 'string' && /^https?:\/\//i.test(s.trim());
}

function renderAudio(filename, audio) {
  // Ссылка на аудио по URL — показываем ссылку для прослушивания
  if (isUrl(filename)) {
    const url = filename.trim();
    return `<div class="my-2"><i class="bi bi-volume-up me-1"></i><a href="${esc(url)}" target="_blank" rel="noopener">Прослушать аудио по ссылке</a></div>`;
  }
  const audioData = audio[filename];
  if (!audioData) {
    return `<div class="text-warning"><i class="bi bi-volume-up me-1"></i>Аудио: ${esc(filename)}</div>`;
  }
  if (audioData.data) {
    const src = `data:${audioData.mime || 'audio/mpeg'};base64,${audioData.data}`;
    return `<audio class="audio-player" controls src="${src}"></audio>`;
  } else if (audioData.url) {
    return `<audio class="audio-player" controls src="${esc(audioData.url)}"></audio>`;
  }
  return `<div class="text-warning"><i class="bi bi-volume-up me-1"></i>Аудио: ${esc(filename)}</div>`;
}

function renderAttachment(value, task) {
  const trimmed = (value || '').trim();
  // Ссылка на файл по URL — показываем ссылку для скачивания
  if (isUrl(trimmed)) {
    return `<div class="my-2"><i class="bi bi-download me-1"></i><a href="${esc(trimmed)}" target="_blank" rel="noopener" download>Скачать файл по ссылке</a></div>`;
  }
  // Локальное вложение — если есть task и файл в attachments, ссылка на API
  if (task && task.attachments && task.attachments[trimmed] && task.attachments[trimmed].data) {
    const url = `/api/tasks/attachment?id=${encodeURIComponent(task.id || '')}&group_id=${encodeURIComponent(task.group_id || '')}&group_position=${encodeURIComponent(task.group_position || '')}&filename=${encodeURIComponent(trimmed)}`;
    return `<div class="my-2"><i class="bi bi-download me-1"></i><a href="${url}" download="${esc(trimmed)}" target="_blank">Скачать ${esc(trimmed)}</a></div>`;
  }
  return `<span class="text-muted">[вложение: ${esc(trimmed)}]</span>`;
}

function renderTable(jsonStr) {
  try {
    const rows = JSON.parse(jsonStr);
    if (!rows || !rows.length) return '';
    const colCount = Math.max(...rows.map(r => r.length));

    let html = '<div class="table-responsive my-2"><table class="table table-bordered table-sm">';
    rows.forEach((row, ri) => {
      html += '<tr>';
      row.forEach((cell, ci) => {
        const tag = ri === 0 ? 'th' : 'td';
        // Вычислим rowspan/colspan если надо — упрощённо, просто выводим текст
        html += `<${tag}>${escHtml(cell || '')}</${tag}>`;
      });
      // Дополняем пустыми ячейками до colCount
      for (let ci = row.length; ci < colCount; ci++) {
        const tag = ri === 0 ? 'th' : 'td';
        html += `<${tag}></${tag}>`;
      }
      html += '</tr>';
    });
    html += '</table></div>';
    return html;
  } catch (e) {
    return `<div class="text-danger">[ошибка рендера таблицы: ${esc(String(e))}]</div>`;
  }
}

// ── Редактирование ─────────────────────────────────────────
function toggleEditMode() {
  state.editMode = !state.editMode;
  const btnEdit = $('btn-edit-mode');
  const btnSave = $('btn-save');
  const textEditBlock = $('text-edit-block');
  const rendered = $('task-text-rendered');

  if (state.editMode) {
    btnEdit.innerHTML = '<i class="bi bi-x-lg"></i> Отмена';
    btnEdit.className = 'btn btn-sm btn-outline-danger';
    btnSave.style.display = '';
    // Показываем текстовый редактор
    if (textEditBlock) textEditBlock.style.display = '';
    if (rendered) rendered.style.display = 'none';
    // Разрешаем редактирование полей, подставляем сырой текст
    const task = state.currentTask;
    const answerEl = $('edit-answer');
    const solutionEl = $('edit-solution');
    if (answerEl && task) { answerEl.innerText = task.answer || ''; answerEl.contentEditable = 'true'; }
    if (solutionEl && task) { solutionEl.innerText = task.solution || ''; solutionEl.contentEditable = 'true'; }
  } else {
    btnEdit.innerHTML = '<i class="bi bi-pencil"></i> Редактировать';
    btnEdit.className = 'btn btn-sm btn-outline-secondary';
    btnSave.style.display = 'none';
    if (textEditBlock) textEditBlock.style.display = 'none';
    if (rendered) rendered.style.display = '';
    // Восстанавливаем рендер с картинками для ответа и решения
    const task = state.currentTask;
    const answerEl = $('edit-answer');
    const solutionEl = $('edit-solution');
    if (task) {
      const images = task.images || {};
      const audio = task.audio || {};
      if (answerEl) {
        answerEl.contentEditable = 'false';
        const html = renderTaskText(task.answer || '', images, audio, task);
        answerEl.innerHTML = html || '<span class="text-muted">—</span>';
      }
      if (solutionEl) {
        solutionEl.contentEditable = 'false';
        const html = renderTaskText(cleanSolutionText(task.solution || ''), images, audio, task);
        solutionEl.innerHTML = html || '<span class="text-muted">—</span>';
      }
      if (window.MathJax && window.MathJax.typesetPromise) {
        window.MathJax.typesetPromise([answerEl, solutionEl].filter(Boolean)).catch(() => {});
      }
    }
  }
}

async function saveTask() {
  const task = state.currentTask;
  if (!task) return;

  const fields = {};
  const answerEl   = $('edit-answer');
  const solutionEl = $('edit-solution');
  const textEl     = $('edit-text');
  const formattedEl = $('edit-formatted-text');

  if (answerEl)   fields.answer   = answerEl.innerText;
  if (solutionEl) fields.solution = solutionEl.innerText;
  if (textEl)     fields.text     = textEl.value;
  if (formattedEl) fields.formatted_text = formattedEl.value;

  setLoading(true, 'Сохранение...');
  try {
    const res = await fetch('/api/tasks/update', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: task.id,
        group_id: task.group_id,
        group_position: task.group_position,
        fields,
      }),
    });
    const data = await readResponseJson(res);
    if (data.ok) {
      showToast('Сохранено');
      // Обновляем state
      Object.assign(state.currentTask, fields);
      // Выходим из режима редактирования и перерисовываем
      state.editMode = true; // чтобы toggleEditMode сбросил
      toggleEditMode();
      renderTaskCard(state.currentTask, state.currentWrapper);
    } else {
      showToast(data.error || 'Ошибка сохранения', 'danger');
    }
  } catch (e) {
    showToast('Ошибка сети', 'danger');
  } finally {
    setLoading(false);
  }
}

// ── Claude ─────────────────────────────────────────────────
function toggleClaudePanel() {
  const panel = $('claude-panel');
  if (!panel) return;
  panel.style.display = panel.style.display === 'none' ? '' : 'none';
}

async function sendToClaude() {
  const apiKey = localStorage.getItem('claude_api_key') || '';
  if (!apiKey) {
    showToast('Укажите API-ключ Claude в настройках (кнопка 🤖 в шапке)', 'warning');
    return;
  }
  const task = state.currentTask;
  if (!task) return;

  const prompt = ($('claude-prompt') || {}).value || '';
  const model  = localStorage.getItem('claude_model') || 'claude-sonnet-4-0';
  const proxyUrl = (localStorage.getItem('claude_proxy_url') || '').trim();

  // Строим текстовое представление задания (без base64 изображений)
  const taskText = task.text || '';

  const resultEl = $('claude-result');
  const responseEl = $('claude-response-text');
  if (resultEl) resultEl.style.display = '';
  if (responseEl) responseEl.innerHTML = '<div class="spinner-border spinner-border-sm text-primary"></div> Отправляем запрос...';

  try {
    const payload = { api_key: apiKey, model, prompt, task_text: taskText };
    if (proxyUrl) payload.proxy_url = proxyUrl;
    const res = await fetch('/api/claude', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await readResponseJson(res);
    if (data.response) {
      if (responseEl) {
        responseEl.innerHTML = window.marked ? marked.parse(data.response) : escHtml(data.response).replace(/\n/g, '<br/>');
        // Рендерим формулы в ответе
        if (window.MathJax && window.MathJax.typesetPromise) {
          window.MathJax.typesetPromise([responseEl]).catch(() => {});
        }
      }
    } else {
      const errMsg = data.error || 'Неизвестная ошибка';
      if (responseEl) responseEl.innerHTML = `<div class="text-danger">${esc(errMsg)}</div>`;
    }
  } catch (e) {
    if (responseEl) responseEl.innerHTML = `<div class="text-danger">Ошибка сети: ${esc(String(e))}</div>`;
  }
}

// ── Экспорт ────────────────────────────────────────────────
function buildExportUrl() {
  const filters = getFilters();
  const include = [];
  if ($('exp-include-images') && $('exp-include-images').checked) include.push('images');
  if ($('exp-include-audio')  && $('exp-include-audio').checked)  include.push('audio');
  if ($('exp-include-html')   && $('exp-include-html').checked)   include.push('html');
  if ($('exp-include-attachments') && $('exp-include-attachments').checked) include.push('attachments');
  const params = new URLSearchParams({ ...filters, include: include.join(',') });
  return `/api/export?${params}`;
}

// ── Загрузка JSON-файлов ───────────────────────────────────
async function importFiles(files) {
  let totalAdded = 0, totalUpdated = 0, totalSkipped = 0;
  setLoading(true, 'Загрузка файлов...');
  for (const file of files) {
    loadingText.textContent = `Загружаем ${file.name}...`;
    const formData = new FormData();
    formData.append('file', file);
    try {
      const res = await fetch('/api/import', { method: 'POST', body: formData });
      const data = await readResponseJson(res);
      if (data.error) {
        showToast(`${file.name}: ${data.error}`, 'danger');
      } else {
        totalAdded   += data.added   || 0;
        totalUpdated += data.updated || 0;
        totalSkipped += data.skipped || 0;
      }
    } catch (e) {
      showToast(`Ошибка при загрузке ${file.name}`, 'danger');
    }
  }
  setLoading(false);
  const parts = [`Добавлено: ${totalAdded}`];
  if (totalUpdated) parts.push(`обновлено: ${totalUpdated}`);
  parts.push(`пропущено: ${totalSkipped}`);
  showToast(parts.join(', '));
  await loadFilterOptions();
  await loadKesCatalog($('f-subject').value);
  await loadTasks(1);
}

// ── Настройки Claude ───────────────────────────────────────
const CLAUDE_LEGACY_MODELS = ['claude-3-5-haiku-20241022', 'claude-3-5-sonnet-20241022'];

function loadClaudeSettings() {
  const key    = localStorage.getItem('claude_api_key') || '';
  let model    = localStorage.getItem('claude_model')   || 'claude-sonnet-4-0';
  if (CLAUDE_LEGACY_MODELS.includes(model)) {
    model = 'claude-sonnet-4-0';
    localStorage.setItem('claude_model', model);
  }
  const proxy  = localStorage.getItem('claude_proxy_url') || '';
  const prompt = localStorage.getItem('claude_default_prompt') ||
    'Проверь и распознай формулы в задании. Реши задание, показав ход решения.';
  if ($('claude-api-key'))       $('claude-api-key').value = key;
  if ($('claude-model'))         $('claude-model').value = model;
  if ($('claude-proxy-url'))     $('claude-proxy-url').value = proxy;
  if ($('claude-default-prompt')) $('claude-default-prompt').value = prompt;
}

function saveClaudeSettings() {
  localStorage.setItem('claude_api_key',       ($('claude-api-key') || {}).value || '');
  localStorage.setItem('claude_model',         ($('claude-model') || {}).value || 'claude-sonnet-4-0');
  localStorage.setItem('claude_proxy_url',     (($('claude-proxy-url') || {}).value || '').trim());
  localStorage.setItem('claude_default_prompt', ($('claude-default-prompt') || {}).value || '');
  showToast('Настройки Claude сохранены');
}

// ── Вспомогательные ────────────────────────────────────────
function esc(str) {
  return String(str || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escHtml(str) {
  return String(str || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// ── Инициализация ──────────────────────────────────────────
async function init() {
  await loadFilterOptions();
  await loadKesCatalog();
  await loadTasks(1);
  loadClaudeSettings();

  // Фильтры
  $('btn-apply-filters').addEventListener('click', () => loadTasks(1));
  $('f-search').addEventListener('keydown', e => { if (e.key === 'Enter') loadTasks(1); });
  $('f-task-number').addEventListener('keydown', e => { if (e.key === 'Enter') loadTasks(1); });
  ['f-exam-type','f-answer-type','f-exam-number'].forEach(id => {
    const el = $(id);
    if (el) el.addEventListener('change', () => loadTasks(1));
  });
  const fGroupId = $('f-group-id');
  if (fGroupId) {
    fGroupId.addEventListener('change', () => loadTasks(1));
    fGroupId.addEventListener('keydown', e => { if (e.key === 'Enter') loadTasks(1); });
  }
  // При смене предмета — перезагружаем список КЭС для этого предмета
  $('f-subject').addEventListener('change', () => {
    const subj = $('f-subject').value;
    $('f-kes').value = '';
    $('f-kes-search').value = '';
    loadKesCatalog(subj);
    loadTasks(1);
  });
  // Применить фильтр при выборе КЭС из списка
  $('f-kes').addEventListener('change', () => loadTasks(1));
  // Поиск внутри списка КЭС
  $('f-kes-search').addEventListener('input', e => {
    renderKesList(e.target.value);
  });
  $('f-kes-search').addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      // Если только одна опция подходит — выбрать её автоматически
      const sel = $('f-kes');
      const opts = Array.from(sel.options).filter(o => o.value !== '');
      if (opts.length === 1) { sel.value = opts[0].value; }
      loadTasks(1);
    }
  });
  $('btn-clear-filters').addEventListener('click', () => {
    $('f-exam-type').value   = '';
    $('f-subject').value     = '';
    $('f-answer-type').value = '';
    $('f-exam-number').value  = '';
    $('f-kes').value         = '';
    $('f-kes-search').value  = '';
    $('f-group-id').value    = '';
    $('f-task-number').value = '';
    $('f-search').value      = '';
    loadKesCatalog('');
    loadTasks(1);
  });

  // Пагинация
  btnPrev.addEventListener('click', () => loadTasks(state.page - 1));
  btnNext.addEventListener('click', () => loadTasks(state.page + 1));
  const pageInputEl = $('page-input');
  if (pageInputEl) {
    pageInputEl.addEventListener('keydown', e => {
      if (e.key === 'Enter') {
        const n = parseInt(e.target.value, 10);
        if (n >= 1 && n <= state.totalPages) loadTasks(n);
      }
    });
  }

  // Импорт файлов
  $('import-file-input').addEventListener('change', e => {
    if (e.target.files.length) {
      importFiles(Array.from(e.target.files));
      e.target.value = '';
    }
  });

  // Экспорт
  $('btn-export').addEventListener('click', () => {
    new bootstrap.Modal($('modal-export')).show();
  });
  $('btn-do-export').addEventListener('click', () => {
    const url = buildExportUrl();
    const a = document.createElement('a');
    a.href = url;
    a.download = 'fipi_export.json';
    a.click();
  });

  // Настройки Claude
  $('btn-save-claude-settings').addEventListener('click', saveClaudeSettings);

  // Удаление из базы
  let _pendingDelete = { mode: null, count: 0 };
  const modalChoice = new bootstrap.Modal($('modal-delete-choice'));
  const modalConfirm = new bootstrap.Modal($('modal-delete-confirm'));

  $('btn-remove-from-db').addEventListener('click', () => {
    $('btn-delete-current').disabled = !state.currentTask;
    modalChoice.show();
  });

  $('btn-delete-current').addEventListener('click', async () => {
    if (!state.currentTask) return;
    modalChoice.hide();
    _pendingDelete = { mode: 'current', count: 1 };
    $('delete-count').textContent = '1';
    modalConfirm.show();
  });

  $('btn-delete-filtered').addEventListener('click', async () => {
    modalChoice.hide();
    setLoading(true, 'Подсчёт...');
    try {
      const params = new URLSearchParams(getFilters());
      const res = await fetch(`/api/tasks/count?${params}`);
      const data = await readResponseJson(res);
      const count = data.count || 0;
      _pendingDelete = { mode: 'filtered', count };
      $('delete-count').textContent = String(count);
      modalConfirm.show();
    } catch (e) {
      showToast('Ошибка при подсчёте', 'danger');
    } finally {
      setLoading(false);
    }
  });

  $('modal-delete-choice').addEventListener('hidden.bs.modal', () => {
    _pendingDelete = { mode: null, count: 0 };
  });

  $('modal-delete-confirm').addEventListener('hidden.bs.modal', () => {
    _pendingDelete = { mode: null, count: 0 };
  });

  $('btn-delete-confirm').addEventListener('click', async () => {
    const { mode, count } = _pendingDelete;
    if (!mode || count === 0) {
      modalConfirm.hide();
      return;
    }
    setLoading(true, 'Удаление...');
    try {
      let deleted = 0;
      if (mode === 'current' && state.currentTask) {
        const p = new URLSearchParams({
          id: state.currentTask.id || '',
          group_id: state.currentTask.group_id || '',
          group_position: state.currentTask.group_position || '',
        });
        const res = await fetch(`/api/tasks/delete?${p}`, { method: 'DELETE' });
        const data = await readResponseJson(res);
        deleted = data.deleted || 0;
      } else if (mode === 'filtered') {
        const params = new URLSearchParams({ ...getFilters(), mode: 'filtered' });
        const res = await fetch(`/api/tasks/delete?${params}`, { method: 'DELETE' });
        const data = await readResponseJson(res);
        deleted = data.deleted || 0;
      }
      modalConfirm.hide();
      modalChoice.hide();
      showToast(`Удалено заданий: ${deleted}`);
      state.currentTask = null;
      state.currentWrapper = null;
      loadTasks(1);
      renderTaskCard(null, null);
    } catch (e) {
      showToast('Ошибка удаления', 'danger');
    } finally {
      setLoading(false);
    }
  });
}

document.addEventListener('DOMContentLoaded', init);

