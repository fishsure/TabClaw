/**
 * TabClaw Frontend Application
 * Manages state, API calls, streaming, and UI rendering.
 */

// ---------------------------------------------------------------------------
// Demo scenario definitions
// ---------------------------------------------------------------------------
const DEMO_SCENARIOS = [
  {
    id: 'sales_analysis',
    icon: '📈',
    title: '销售业绩全景分析',
    description: '分析 2023 年全年销售数据，找出最佳区域与产品，拆解季度趋势，输出品类透视表。',
    files: ['sales_2023.csv'],
    queries: [
      '请先介绍这份销售数据的基本情况：数据规模、包含哪些维度、整体销售总额与总利润。',
      '按区域（region）统计全年总收入和总利润，从高到低排名，找出表现最好和最差的区域。',
      '按季度汇总收入和利润：Q1–Q4 各季度表现如何？哪个季度收入最高、利润率最好？',
      '做一张以 region 为行、category 为列的收入透视表，看看哪个区域哪个品类贡献最大。',
    ],
  },
  {
    id: 'hr_insights',
    icon: '👥',
    title: 'HR 人才数据洞察',
    description: '深入分析员工薪资、绩效与部门分布，识别高潜力人才与薪资结构问题。',
    files: ['employees.csv'],
    queries: [
      '请介绍公司整体人才结构：各部门人数分布、平均薪资水平、整体绩效分布情况。',
      '统计各部门的平均薪资和最高薪资，按平均薪资从高到低排序，哪个部门薪资水平最高？',
      '找出高绩效员工（performance_score ≥ 4.5），统计他们的部门分布和平均薪资，与全员平均水平对比。',
      '分析薪资与绩效分数的相关性，并找出绩效最高（performance_score ≥ 4.7）的员工名单及其薪资。',
    ],
  },
  {
    id: 'order_product',
    icon: '🔗',
    title: '订单与产品关联分析',
    description: '将订单流水与产品目录跨表关联，分析品类收入、退货情况与渠道价值。',
    files: ['products.csv', 'orders.csv'],
    queries: [
      '分别查看 products 和 orders 两张表的结构与基本信息，说明如何通过 product_id 关联它们。',
      '将两张表通过 product_id 合并，统计每个产品类别（category）的总销售额和订单量，哪个品类最畅销？',
      '筛选出所有退货订单（status=\'Returned\'），统计退货量最多的 Top 5 产品，并关联 products 表查看这些产品的评分（rating）分析原因。',
      '按销售渠道（channel）统计订单总量和总销售额，哪个渠道最有价值？',
    ],
  },
  {
    id: 'nps_survey',
    icon: '📊',
    title: '用户 NPS 满意度分析',
    description: '解析用户调研数据，对比各国满意度差距，按使用频率细分，挖掘产品改进优先级。',
    files: ['survey_nps.csv'],
    queries: [
      '介绍调研基本情况：样本量、受访者国家与角色分布、平均 NPS 分和满意度得分。',
      '按国家统计平均 NPS 分和平均满意度，从低到高排名，哪个市场用户体验最差、最需要改进？',
      '按使用频率（use_frequency）分组统计平均 NPS 和满意度，高频用户和低频用户的体验差距有多大？',
      '找出 NPS 低分用户（nps_score ≤ 4），统计他们最常提到的痛点（main_pain_point）和用户角色分布，给出改进优先级建议。',
    ],
  },
];

class TabClawApp {
  constructor() {
    this.state = {
      tables: [],
      skills: { builtin: [], packages: [] },
      memory: {},
      planMode: true,
      codeToolEnabled: false,
      skillLearnEnabled: false,
      streaming: false,
      currentPlan: null,
      currentPlanMessage: '',
      tableModal: { tableId: null, page: 1, totalPages: 1 },
      skillEdit: null,
      memoryEdit: null,
      demoRunning: false,
      clarifying: false,
      // Workflow tracking: maps msgId → session_id from backend
      currentWorkflowId: null,
    };

    this._streamMsgId = null;
    this._streamBuffer = '';

    this._init();
  }

  // -----------------------------------------------------------------------
  // Initialisation
  // -----------------------------------------------------------------------

  _init() {
    // Configure marked (GitHub-flavoured MD, single-newline → <br>)
    marked.use({ gfm: true, breaks: true });
    this._lang = localStorage.getItem('lang') || 'en';
    this._applyTheme(localStorage.getItem('theme') || 'dark');
    this._bindEvents();
    this._loadTables();
    this._loadSkills();
    this._loadMemory();
    this._autoresize(document.getElementById('message-input'));
    if (this._lang === 'zh') this._applyLangLabels();
  }

  _bindEvents() {
    // Sidebar tabs
    document.querySelectorAll('.sidebar-tab').forEach(btn => {
      btn.addEventListener('click', () => this._switchTab(btn.dataset.tab));
    });

    // File upload
    const uploadArea = document.getElementById('upload-area');
    const fileInput = document.getElementById('file-input');
    uploadArea.addEventListener('click', () => fileInput.click());
    uploadArea.addEventListener('dragover', e => { e.preventDefault(); uploadArea.classList.add('drag-over'); });
    uploadArea.addEventListener('dragleave', () => uploadArea.classList.remove('drag-over'));
    uploadArea.addEventListener('drop', e => {
      e.preventDefault();
      uploadArea.classList.remove('drag-over');
      [...(e.dataTransfer.files || [])].forEach(f => this._uploadFile(f));
    });
    fileInput.addEventListener('change', e => {
      [...(e.target.files || [])].forEach(f => this._uploadFile(f));
      fileInput.value = '';
    });

    // Chat input
    const input = document.getElementById('message-input');
    const sendBtn = document.getElementById('send-btn');
    input.addEventListener('keydown', e => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); this._send(); }
    });
    sendBtn.addEventListener('click', () => this._send());

    // Plan mode toggle
    document.getElementById('plan-mode-check').addEventListener('change', e => {
      this.state.planMode = e.target.checked;
    });
    document.getElementById('code-tool-check').addEventListener('change', e => {
      this.state.codeToolEnabled = e.target.checked;
    });
    document.getElementById('skill-learn-check').addEventListener('change', e => {
      this.state.skillLearnEnabled = e.target.checked;
    });

    // Theme toggle
    document.getElementById('theme-btn').addEventListener('click', () => this._toggleTheme());
    // Lang toggle
    document.getElementById('lang-btn').addEventListener('click', () => this._toggleLang());

    // Clear / Compact chat
    document.getElementById('clear-chat-btn').addEventListener('click', () => this._clearChat());
    document.getElementById('compact-chat-btn').addEventListener('click', () => this._compactChat());

    // Demo
    document.getElementById('demo-btn').addEventListener('click', () => this.showDemoModal());
    document.getElementById('demo-stop-btn').addEventListener('click', () => this.stopDemo());

    // Plan modal
    document.getElementById('add-plan-step-btn').addEventListener('click', () => this._addPlanStep());
    document.getElementById('execute-plan-btn').addEventListener('click', () => this._executePlan());

    // Skills
    document.getElementById('add-skill-btn').addEventListener('click', () => this.showSkillModal());
    document.getElementById('skill-save-btn').addEventListener('click', () => this._saveSkill());
    document.getElementById('clear-skills-btn').addEventListener('click', () => this._clearAllSkills());
    document.getElementById('import-skill-btn').addEventListener('click', () => {
      document.getElementById('skill-zip-input').click();
    });
    document.getElementById('skill-zip-input').addEventListener('change', e => {
      if (e.target.files.length) this._importSkillZip(e.target.files[0]);
      e.target.value = '';
    });

    // Memory
    document.getElementById('add-memory-btn').addEventListener('click', () => this.showMemoryModal());
    document.getElementById('memory-overview-btn').addEventListener('click', () => this._summarizeMemory());
    document.getElementById('memory-save-btn').addEventListener('click', () => this._saveMemory());
    document.getElementById('forget-btn').addEventListener('click', () => this._forgetMemory());
    document.getElementById('forget-input').addEventListener('keydown', e => {
      if (e.key === 'Enter') this._forgetMemory();
    });
    document.getElementById('clear-memory-btn').addEventListener('click', () => this._clearAllMemory());

    // Table modal pagination
    document.getElementById('table-modal-prev').addEventListener('click', () => this._tableModalPage(-1));
    document.getElementById('table-modal-next').addEventListener('click', () => this._tableModalPage(+1));
    document.getElementById('table-modal-download').addEventListener('click', () => {
      const tid = this.state.tableModal.tableId;
      if (tid) window.location.href = `/api/tables/${tid}/download`;
    });

    // Close modals on overlay click
    ['plan-modal', 'table-modal', 'skill-modal', 'memory-modal', 'demo-modal', 'growth-modal'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('click', e => {
        if (e.target.id === id) this._closeModalById(id);
      });
    });

    // Escape key closes modals
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') {
        ['plan-modal', 'table-modal', 'skill-modal', 'memory-modal', 'demo-modal', 'growth-modal'].forEach(id => {
          const el = document.getElementById(id);
          if (el && !el.classList.contains('hidden')) this._closeModalById(id);
        });
      }
    });
  }

  _closeModalById(id) {
    if (id === 'plan-modal') this.hidePlanModal();
    else if (id === 'table-modal') this.hideTableModal();
    else if (id === 'skill-modal') this.hideSkillModal();
    else if (id === 'skill-detail-modal') this.hideSkillDetailModal();
    else if (id === 'memory-modal') this.hideMemoryModal();
    else if (id === 'memory-summary-modal') this.hideMemorySummaryModal();
    else if (id === 'demo-modal') this.hideDemoModal();
    else if (id === 'growth-modal') this.hideGrowthModal();
  }

  _autoresize(textarea) {
    const resize = () => {
      textarea.style.height = 'auto';
      textarea.style.height = Math.min(textarea.scrollHeight, 160) + 'px';
    };
    textarea.addEventListener('input', resize);
  }

  _switchTab(tab) {
    document.querySelectorAll('.sidebar-tab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    document.querySelectorAll('.sidebar-panel').forEach(p => p.classList.toggle('active', p.id === `panel-${tab}`));
  }

  _applyTheme(theme) {
    const isLight = theme === 'light';
    document.documentElement.classList.toggle('light', isLight);
    const btn = document.getElementById('theme-btn');
    if (btn) {
      btn.innerHTML = isLight
        ? `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z"/></svg>`
        : `<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>`;
      btn.title = isLight ? '切换夜间模式' : '切换日间模式';
    }
    localStorage.setItem('theme', theme);
  }

  _toggleTheme() {
    const current = document.documentElement.classList.contains('light') ? 'light' : 'dark';
    this._applyTheme(current === 'light' ? 'dark' : 'light');
  }

  _toggleLang() {
    this._lang = this._lang === 'en' ? 'zh' : 'en';
    localStorage.setItem('lang', this._lang);
    this._applyLangLabels();
  }

  _applyLangLabels() {
    const zh = this._lang === 'zh';
    // Lang button shows the language you'll switch TO
    document.getElementById('lang-btn').textContent = zh ? 'EN' : '中';
    // Header buttons
    const compactLabel = document.querySelector('#compact-chat-btn .btn-label');
    if (compactLabel) compactLabel.textContent = zh ? '压缩' : 'Compact';
    const clearLabel = document.querySelector('#clear-chat-btn .btn-label');
    if (clearLabel) clearLabel.textContent = zh ? '清空对话' : 'Clear Chat';
    // Sidebar tabs
    const tabMap = { tables: ['Tables', '数据表'], skills: ['Skills', '技能'], memory: ['Memory', '记忆'] };
    document.querySelectorAll('.sidebar-tab').forEach(tab => {
      const [en, zh_] = tabMap[tab.dataset.tab] || [];
      if (en) tab.textContent = zh ? zh_ : en;
    });
    // Toolbar
    const planLabel = document.getElementById('plan-mode-label-text');
    if (planLabel) planLabel.textContent = zh ? '规划模式' : 'Plan Mode';
    const planHint = document.getElementById('plan-mode-hint-span');
    if (planHint) planHint.textContent = zh ? '— 执行前可审阅步骤' : '— review steps before execution';
    const codeLabel = document.getElementById('code-tool-label-text');
    if (codeLabel) codeLabel.textContent = zh ? '代码工具' : 'Code Tool';
    const codeHint = document.getElementById('code-tool-hint-span');
    if (codeHint) codeHint.textContent = zh ? '— Python 沙箱' : '— Python sandbox';
    const skillLearnLabel = document.getElementById('skill-learn-label-text');
    if (skillLearnLabel) skillLearnLabel.textContent = zh ? '技能学习' : 'Skill Learning';
    const skillLearnHint = document.getElementById('skill-learn-hint-span');
    if (skillLearnHint) skillLearnHint.textContent = zh ? '— 默认关闭' : '— auto off';
    // Plan modal buttons
    const planCancel = document.getElementById('plan-cancel-btn');
    if (planCancel) planCancel.textContent = zh ? '取消' : 'Cancel';
    const execLabel = document.querySelector('#execute-plan-btn .btn-label');
    if (execLabel) execLabel.textContent = zh ? '执行计划' : 'Execute Plan';
    // Input placeholder
    const msgInput = document.getElementById('message-input');
    if (msgInput) msgInput.placeholder = zh
      ? '提问或对数据表发出操作指令…'
      : 'Ask a question or give an instruction about your tables…';
    // Upload hints
    const uploadMain = document.getElementById('upload-hint-main');
    if (uploadMain) uploadMain.textContent = zh ? '点击或拖拽 CSV / Excel 文件至此' : 'Click or drop CSV / Excel files';
    const uploadSub = document.getElementById('upload-hint-sub');
    if (uploadSub) uploadSub.textContent = zh ? '支持多文件同时上传' : 'Multiple files supported';
    // Lab credit
    const labCredit = document.getElementById('lab-credit');
    if (labCredit) labCredit.textContent = zh
      ? '中国科学技术大学认知智能全国重点实验室 AGI 组'
      : 'State Key Laboratory of Cognitive Intelligence, USTC · AGI Group';
  }

  // -----------------------------------------------------------------------
  // API helpers
  // -----------------------------------------------------------------------

  async _api(method, path, body) {
    const opts = { method, headers: { 'Content-Type': 'application/json' } };
    if (body !== undefined) opts.body = JSON.stringify(body);
    const res = await fetch(path, opts);
    if (!res.ok) {
      const err = await res.text();
      throw new Error(err || `HTTP ${res.status}`);
    }
    return res.json();
  }

  // -----------------------------------------------------------------------
  // Tables
  // -----------------------------------------------------------------------

  async _loadTables() {
    try {
      this.state.tables = await this._api('GET', '/api/tables');
      this._renderTables();
    } catch (e) { console.error('loadTables', e); }
  }

  async _uploadFile(file) {
    const name = file.name;
    this._notify(`Uploading ${name}…`, 'info');
    const formData = new FormData();
    formData.append('file', file);
    try {
      const res = await fetch('/api/upload', { method: 'POST', body: formData });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      this.state.tables = await this._api('GET', '/api/tables');
      this._renderTables();
      this._notify(`Uploaded: ${data.name} (${data.rows} rows × ${data.cols} cols)`, 'success');
      this._hideChatEmpty();
    } catch (e) { this._notify(`Upload failed: ${e.message}`, 'error'); }
  }

  async _deleteTable(tableId) {
    try {
      await this._api('DELETE', `/api/tables/${tableId}`);
      this.state.tables = this.state.tables.filter(t => t.table_id !== tableId);
      this._renderTables();
      this._notify('Table removed', 'success');
    } catch (e) { this._notify(`Error: ${e.message}`, 'error'); }
  }

  _renderTables() {
    const list = document.getElementById('tables-list');
    const count = document.getElementById('table-count');
    count.textContent = this.state.tables.length;
    if (!this.state.tables.length) {
      list.innerHTML = '<div class="empty-state">No tables yet.<br>Upload CSV or Excel files below.</div>';
      return;
    }
    list.innerHTML = this.state.tables.map(t => `
      <div class="table-item">
        <span class="table-item-icon">📊</span>
        <div class="table-item-info" onclick="app.showTableModal('${t.table_id}')">
          <div class="table-item-name">${this._esc(t.name)}</div>
          <div class="table-item-meta">${t.rows.toLocaleString()} rows × ${t.cols} cols</div>
        </div>
        <span class="table-item-badge ${t.source === 'computed' ? 'purple' : ''}">${t.source === 'computed' ? 'result' : 'csv'}</span>
        <div class="table-item-actions">
          <button class="btn icon-only sm" title="View" onclick="app.showTableModal('${t.table_id}')">👁</button>
          <button class="btn icon-only sm danger" title="Delete" onclick="app._deleteTable('${t.table_id}')">🗑</button>
        </div>
      </div>
    `).join('');
    this._hideChatEmpty();
  }

  // -----------------------------------------------------------------------
  // Table modal
  // -----------------------------------------------------------------------

  async showTableModal(tableId, page = 1) {
    this.state.tableModal = { tableId, page, totalPages: 1 };
    document.getElementById('table-modal').classList.remove('hidden');
    await this._loadTablePage(tableId, page);
  }

  hideTableModal() {
    document.getElementById('table-modal').classList.add('hidden');
  }

  async _loadTablePage(tableId, page) {
    const content = document.getElementById('table-modal-content');
    content.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted)">Loading…</div>';
    try {
      const data = await this._api('GET', `/api/tables/${tableId}?page=${page}&page_size=50`);
      this.state.tableModal.totalPages = data.total_pages;
      this.state.tableModal.page = data.page;
      document.getElementById('table-modal-title').textContent = data.name;
      document.getElementById('table-modal-meta').textContent =
        `${data.total_rows.toLocaleString()} rows × ${data.columns.length} columns`;
      document.getElementById('table-modal-page').textContent =
        `Page ${data.page} / ${data.total_pages}`;
      document.getElementById('table-modal-prev').disabled = data.page <= 1;
      document.getElementById('table-modal-next').disabled = data.page >= data.total_pages;
      content.innerHTML = this._buildDataTable(data.columns, data.rows, data.total_rows);
    } catch (e) {
      content.innerHTML = `<div style="padding:20px;color:var(--red)">${e.message}</div>`;
    }
  }

  async _tableModalPage(delta) {
    const { tableId, page, totalPages } = this.state.tableModal;
    const newPage = Math.min(Math.max(1, page + delta), totalPages);
    if (newPage !== page) await this._loadTablePage(tableId, newPage);
  }

  _buildDataTable(columns, rows, totalRows, maxInline = 0) {
    const limit = maxInline || rows.length;
    const shown = rows.slice(0, limit);
    const extra = rows.length > limit ? rows.length - limit : 0;
    const headers = columns.map(c => `<th>${this._esc(String(c))}</th>`).join('');
    const bodyRows = shown.map(row =>
      `<tr>${columns.map(c => `<td title="${this._esc(String(row[c] ?? ''))}">${this._esc(String(row[c] ?? ''))}</td>`).join('')}</tr>`
    ).join('');
    let html = `<div class="table-scroll"><table class="data-table"><thead><tr>${headers}</tr></thead><tbody>${bodyRows}</tbody></table></div>`;
    if (extra > 0) html += `<div class="table-more-rows">… ${extra} more rows (showing ${shown.length} of ${rows.length})</div>`;
    if (maxInline > 0 && totalRows > maxInline) {
      html += `<div class="table-more-rows">${totalRows.toLocaleString()} total rows — <a style="color:var(--primary);cursor:pointer" onclick="app.showTableModal('_tid_')">View full table</a></div>`;
    }
    return html;
  }

  // -----------------------------------------------------------------------
  // Chat / Send
  // -----------------------------------------------------------------------

  async _send() {
    const input = document.getElementById('message-input');
    const msg = input.value.trim();
    if (!msg || this.state.streaming || this.state.clarifying) return;
    input.value = '';
    input.style.height = 'auto';
    this._hideChatEmpty();
    this._appendUserMessage(msg);
    this._scrollChatForce();

    // Intent clarification check
    this.state.clarifying = true;
    this._setInputEnabled(false);
    let clarify = null;
    try {
      clarify = await this._api('POST', '/api/clarify', { message: msg });
    } catch {}
    this.state.clarifying = false;
    this._setInputEnabled(true);

    if (clarify && clarify.needs_clarification) {
      this._showClarificationCard(msg, clarify.question, clarify.options);
      return;
    }

    if (this.state.planMode) {
      await this._generateAndShowPlan(msg);
    } else {
      await this._streamChat(msg);
    }
  }

  insertSuggestion(chipEl) {
    document.getElementById('message-input').value = chipEl.textContent;
    document.getElementById('message-input').focus();
  }

  async _generateAndShowPlan(msg) {
    const thinkId = this._appendThinking('Generating plan…');
    try {
      const plan = await this._api('POST', '/api/generate-plan', { message: msg });
      this._removeMessage(thinkId);
      this.state.currentPlan = plan;
      this.state.currentPlanMessage = msg;
      this._showPlanModal(plan);
    } catch (e) {
      this._removeMessage(thinkId);
      this._appendErrorMessage(`Failed to generate plan: ${e.message}`);
    }
  }

  async _streamChat(message, executePlan = false, steps = null) {
    this.state.streaming = true;
    this._setInputEnabled(false);
    this._currentMsgTables = [];  // tables created during this message
    this._agentState = {};        // per-agent state for multi-agent mode

    const msgId = this._appendAssistantMessage('');
    this._streamMsgId = msgId;
    this._streamBuffer = '';

    try {
      const endpoint = executePlan ? '/api/execute-plan' : '/api/chat';
      const codeTool = this.state.codeToolEnabled;
      const skillLearn = this.state.skillLearnEnabled;
      const body = executePlan
        ? { message, steps, code_tool: codeTool, skill_learn: skillLearn }
        : { message, code_tool: codeTool, skill_learn: skillLearn };

      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const parts = buf.split('\n');
        buf = parts.pop(); // keep partial line
        for (const line of parts) {
          const trimmed = line.trim();
          if (!trimmed || trimmed === 'data: [DONE]') continue;
          if (trimmed.startsWith('data: ')) {
            try {
              const event = JSON.parse(trimmed.slice(6));
              this._handleStreamEvent(event, msgId);
            } catch { /* ignore parse errors */ }
          }
        }
      }
    } catch (e) {
      this._appendToMessage(msgId, `\n\n*Stream error: ${e.message}*`);
    } finally {
      this._finalizeStreamMessage(msgId);
      this._highlightFinalAnswer(msgId);
      if (this._currentMsgTables && this._currentMsgTables.length > 0) {
        this._appendResultDownloadPanel(msgId, this._currentMsgTables);
      }
      if (this.state.currentWorkflowId) {
        this._appendFeedbackButtons(msgId, this.state.currentWorkflowId);
      }
      this.state.streaming = false;
      this._setInputEnabled(true);
      this._streamMsgId = null;
      this._streamBuffer = '';
      this._currentMsgTables = [];
      this.state.currentWorkflowId = null;
      await this._loadTables();
      await this._loadMemory();
    }
  }

  _handleStreamEvent(event, msgId) {
    switch (event.type) {
      case 'text_chunk':
        if (event.agent_id) {
          this._updateAgentCardText(msgId, event.agent_id, event.content);
        } else {
          // First aggregator chunk — hide the "synthesizing" header
          const aggHdr = document.getElementById(`aggregate-header-${msgId}`);
          if (aggHdr) aggHdr.classList.add('hidden');
          this._streamBuffer += event.content;
          this._updateStreamBubble(msgId, this._streamBuffer);
        }
        break;

      case 'tool_call':
        if (event.agent_id) {
          this._addAgentToolBadge(msgId, event.agent_id, event.skill);
        } else {
          this._appendToolBlock(msgId, event.skill, event.params, null);
        }
        break;

      case 'tool_result':
        if (!event.agent_id) this._updateLastToolBlock(msgId, event.text);
        break;

      case 'table':
        this._appendTableResult(msgId, event.data);
        if (this._currentMsgTables) this._currentMsgTables.push(event.data);
        break;

      case 'step_start':
        this._appendStepIndicator(msgId, event.step_num, event.total, event.description, false);
        break;

      case 'step_done':
        this._markStepDone(msgId, event.step_num);
        break;

      case 'reflect_start':
        this._appendReflectIndicator(msgId);
        break;

      case 'reflect_done':
        this._markReflectDone(msgId);
        break;

      case 'compacted':
        this._appendCompactedNotice(event.old_count, event.summary);
        break;

      case 'skill_learned':
        this._appendSkillLearnedBadge(msgId, event.skill);
        this._loadSkills();
        break;

      case 'workflow_id':
        this.state.currentWorkflowId = event.session_id;
        break;

      case 'skill_reused':
        this._appendSkillReusedHint(msgId, event.skill_name, event.message);
        break;

      case 'agent_pool_start':
        this._createAgentPool(msgId, event.agents);
        break;

      case 'agent_start':
        this._activateAgentCard(msgId, event.agent_id);
        break;

      case 'agent_done':
        this._finishAgentCard(msgId, event.agent_id, event.conclusion);
        break;

      case 'aggregate_start':
        this._appendAggregateHeader(msgId);
        break;

      case 'final_text':
        if (!event.agent_id && event.content && !this._streamBuffer) {
          this._updateStreamBubble(msgId, event.content);
          this._streamBuffer = event.content;
        }
        break;

      case 'error':
        if (!event.agent_id) {
          this._appendToMessage(msgId, `\n\n⚠️ **Error:** ${event.content}`);
        }
        break;
    }
  }

  // -----------------------------------------------------------------------
  // DOM message building
  // -----------------------------------------------------------------------

  _appendUserMessage(text) {
    const id = 'msg-' + Date.now();
    const el = document.createElement('div');
    el.className = 'message user';
    el.id = id;
    el.innerHTML = `
      <div class="msg-avatar">U</div>
      <div class="msg-body">
        <div class="msg-bubble">${this._esc(text)}</div>
      </div>`;
    this._chatContainer().appendChild(el);
    this._scrollChat();
    return id;
  }

  _appendAssistantMessage(initialContent) {
    const id = 'msg-' + Date.now() + '-ai';
    const el = document.createElement('div');
    el.className = 'message assistant';
    el.id = id;
    el.innerHTML = `
      <div class="msg-avatar">⚡</div>
      <div class="msg-body" id="${id}-body">
        <div class="msg-bubble" id="${id}-bubble">
          <span class="typing-cursor"></span>
        </div>
      </div>`;
    this._chatContainer().appendChild(el);
    this._scrollChat();
    return id;
  }

  _appendThinking(label) {
    const id = 'think-' + Date.now();
    const el = document.createElement('div');
    el.className = 'message assistant';
    el.id = id;
    el.innerHTML = `
      <div class="msg-avatar">⚡</div>
      <div class="msg-body">
        <div class="msg-bubble">
          <div class="thinking-indicator">
            <div class="thinking-dots"><span></span><span></span><span></span></div>
            ${this._esc(label)}
          </div>
        </div>
      </div>`;
    this._chatContainer().appendChild(el);
    this._scrollChat();
    return id;
  }

  _appendErrorMessage(text) {
    const id = 'err-' + Date.now();
    const el = document.createElement('div');
    el.className = 'message assistant';
    el.id = id;
    el.innerHTML = `
      <div class="msg-avatar" style="color:var(--red)">⚠</div>
      <div class="msg-body">
        <div class="msg-bubble" style="border-color:var(--red)22">
          <span style="color:var(--red)">${this._esc(text)}</span>
        </div>
      </div>`;
    this._chatContainer().appendChild(el);
    this._scrollChat();
    return id;
  }

  _removeMessage(id) {
    document.getElementById(id)?.remove();
  }

  // Strip raw tool-call syntax that DeepSeek V3 sometimes leaks into delta.content
  _stripLLMMarkers(text) {
    if (!text) return text;
    // Remove everything from <｜tool▁call▁begin｜> to <｜tool▁call▁end｜> (or end of string)
    return text.replace(/<｜tool[\s\S]*?(?:<｜tool\u2581call\u2581end｜>|$)/g, '').trim();
  }

  _updateStreamBubble(msgId, text) {
    const bubble = document.getElementById(`${msgId}-bubble`);
    if (!bubble) return;
    bubble.innerHTML = this._renderMarkdown(this._stripLLMMarkers(text)) + '<span class="typing-cursor"></span>';
    this._scrollChat();
  }

  _appendToMessage(msgId, extra) {
    const bubble = document.getElementById(`${msgId}-bubble`);
    if (bubble) bubble.innerHTML += this._renderMarkdown(extra);
    this._scrollChat();
  }

  _finalizeStreamMessage(msgId) {
    const bubble = document.getElementById(`${msgId}-bubble`);
    if (!bubble) return;
    // Remove typing cursor
    bubble.querySelectorAll('.typing-cursor').forEach(c => c.remove());
    // Re-render final content cleanly (strip any leaked markers)
    if (this._streamBuffer) {
      bubble.innerHTML = this._renderMarkdown(this._stripLLMMarkers(this._streamBuffer));
      // Replace [CONSENSUS] / [UNCERTAIN] text markers with styled badges
      this._renderUncertaintyMarkers(bubble);
    }
  }

  _renderUncertaintyMarkers(el) {
    el.innerHTML = el.innerHTML
      .replace(/\[CONSENSUS\]/g,
        '<span class="uncertainty-badge consensus">✓ CONSENSUS</span>')
      .replace(/\[UNCERTAIN\]/g,
        '<span class="uncertainty-badge uncertain">⚠ UNCERTAIN</span>');
  }

  // Wrap the "## ✅ 最终结论 / 操作结果" section in a highlighted box
  _highlightFinalAnswer(msgId) {
    const bubble = document.getElementById(`${msgId}-bubble`);
    if (!bubble) return;

    // Find the last heading that contains the conclusion/result marker
    let targetHeading = null;
    bubble.querySelectorAll('h1, h2, h3').forEach(h => {
      const text = h.textContent;
      if (text.includes('✅') || text.includes('最终结论') || text.includes('操作结果')) {
        targetHeading = h;
      }
    });
    if (!targetHeading) return;

    // Create the highlight box and insert it before the heading
    const box = document.createElement('div');
    box.className = 'final-answer-box';
    bubble.insertBefore(box, targetHeading);

    // Move the heading and all following siblings into the box
    let el = targetHeading;
    while (el) {
      const next = el.nextSibling;
      box.appendChild(el);
      el = next;
    }
  }

  // Append a sticky download panel below the message for result tables
  _appendResultDownloadPanel(msgId, tables) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;

    const panel = document.createElement('div');
    panel.className = 'result-download-panel';

    const header = document.createElement('div');
    header.className = 'rdp-header';
    header.innerHTML = `<span class="rdp-title">📥 结果表格</span><span class="rdp-hint">点击下载或在聊天中预览</span>`;
    panel.appendChild(header);

    const list = document.createElement('div');
    list.className = 'rdp-list';
    tables.forEach(t => {
      const item = document.createElement('div');
      item.className = 'rdp-item';
      const rows = (t.total_rows || 0).toLocaleString();
      const cols = (t.columns || []).length;
      item.innerHTML = `
        <span class="rdp-table-name">📊 ${this._esc(t.name)}</span>
        <span class="rdp-table-meta">${rows} 行 × ${cols} 列</span>
        <div class="rdp-actions">
          <button class="btn sm" onclick="app.showTableModal('${t.table_id}')">预览</button>
          <button class="btn sm primary" onclick="window.location.href='/api/tables/${t.table_id}/download'">⬇ CSV</button>
        </div>`;
      list.appendChild(item);
    });
    panel.appendChild(list);
    body.appendChild(panel);
    this._scrollChat();
  }

  _appendToolBlock(msgId, skillName, params, resultText) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const blockId = `tool-${Date.now()}`;

    // Special rendering for execute_python: show code in a proper code block
    let paramsHtml;
    const isCode = skillName === 'execute_python';
    if (isCode && params.code) {
      const rn = params.result_name ? `<span class="tool-code-result-name">→ result: <code>${this._esc(params.result_name)}</code></span>` : '';
      paramsHtml = `<pre class="tool-code-block"><code>${this._esc(params.code)}</code></pre>${rn}`;
    } else {
      paramsHtml = `<pre class="tool-params-pre">${this._esc(JSON.stringify(params, null, 2))}</pre>`;
    }

    const icon = isCode
      ? `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>`
      : `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z"/></svg>`;

    const div = document.createElement('div');
    div.className = `tool-block${isCode ? ' tool-block-code' : ''}`;
    div.id = blockId;
    div.innerHTML = `
      <div class="tool-block-header" onclick="this.nextElementSibling.style.display = this.nextElementSibling.style.display === 'none' ? 'block' : 'none'">
        ${icon}
        <span class="tool-name">${this._esc(skillName)}</span>
        <span class="tool-status" id="${blockId}-status">⟳ running…</span>
      </div>
      <div class="tool-block-body" style="display:none">
        <div class="tool-block-label">Code:</div>
        ${paramsHtml}
        <div id="${blockId}-result"></div>
      </div>`;
    body.appendChild(div);
    this._scrollChat();
  }

  _updateLastToolBlock(msgId, resultText) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const blocks = body.querySelectorAll('.tool-block');
    const last = blocks[blocks.length - 1];
    if (!last) return;
    const statusEl = last.querySelector('[id$="-status"]');
    if (statusEl) {
      statusEl.textContent = '✓ done';
      statusEl.className = 'tool-status ok';
    }
    const resultEl = last.querySelector('[id$="-result"]');
    if (resultEl && resultText) {
      const preview = resultText.length > 200 ? resultText.slice(0, 200) + '…' : resultText;
      resultEl.innerHTML = `<div style="color:var(--text-dim);margin-top:6px">Result:</div>${this._esc(preview)}`;
    }
  }

  _appendTableResult(msgId, tableData) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const div = document.createElement('div');
    div.className = 'table-result';
    const totalRows = tableData.total_rows || tableData.rows.length;
    const shown = Math.min(tableData.rows.length, 20);
    const tableHtml = this._buildDataTable(tableData.columns, tableData.rows, totalRows, 20);
    div.innerHTML = `
      <div class="table-result-header">
        <span class="table-result-name">📊 ${this._esc(tableData.name)}</span>
        <span class="table-result-meta">${totalRows.toLocaleString()} rows × ${tableData.columns.length} cols</span>
        <div class="table-result-actions">
          <button class="btn sm" onclick="app.showTableModal('${tableData.table_id}')">View Full</button>
          <button class="btn sm" onclick="window.location.href='/api/tables/${tableData.table_id}/download'">⬇ CSV</button>
        </div>
      </div>
      ${tableHtml}`;
    body.appendChild(div);
    this._scrollChat();
  }

  _appendStepIndicator(msgId, stepNum, total, desc, done) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const id = `step-${msgId}-${stepNum}`;
    if (document.getElementById(id)) return; // already exists
    const div = document.createElement('div');
    div.className = `step-progress ${done ? 'step-done' : ''}`;
    div.id = id;
    div.innerHTML = `<span class="step-badge">Step ${stepNum}/${total}</span><span class="step-desc">${this._esc(desc)}</span>`;
    body.appendChild(div);
    this._scrollChat();
  }

  _markStepDone(msgId, stepNum) {
    const el = document.getElementById(`step-${msgId}-${stepNum}`);
    if (el) el.classList.add('step-done');
  }

  _appendReflectIndicator(msgId) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const id = `reflect-${msgId}`;
    if (document.getElementById(id)) return;
    const div = document.createElement('div');
    div.className = 'step-progress reflect-indicator';
    div.id = id;
    div.innerHTML = `<span class="step-badge reflect-badge">🔍 Self-check</span><span class="step-desc">Verifying results against original request…</span>`;
    body.appendChild(div);
    this._scrollChat();
  }

  _markReflectDone(msgId) {
    const el = document.getElementById(`reflect-${msgId}`);
    if (el) el.classList.add('step-done');
  }

  // ── Multi-agent pool UI ────────────────────────────────────────────────

  _createAgentPool(msgId, agents) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;

    const pool = document.createElement('div');
    pool.className = 'agent-pool';
    pool.id = `agent-pool-${msgId}`;

    const header = document.createElement('div');
    header.className = 'agent-pool-header';
    header.innerHTML = `🤖 Multi-Agent Analysis &mdash; ${agents.length} specialist agents running in parallel`;
    pool.appendChild(header);

    const grid = document.createElement('div');
    grid.className = 'agent-cards-grid';
    pool.appendChild(grid);

    this._agentState = {};
    for (const agent of agents) {
      const card = document.createElement('div');
      card.className = 'agent-card pending';
      card.id = `agent-card-${msgId}-${agent.id}`;
      card.innerHTML = `
        <div class="agent-card-name">
          <span class="agent-status-dot"></span>
          <span>${this._esc(agent.table_name)}</span>
        </div>
        <div class="agent-card-tools" id="agent-tools-${msgId}-${agent.id}"></div>
        <div class="agent-card-text" id="agent-text-${msgId}-${agent.id}">Waiting…</div>`;
      grid.appendChild(card);
      this._agentState[agent.id] = { textBuffer: '' };
    }

    body.appendChild(pool);
    this._scrollChat();
  }

  _activateAgentCard(msgId, agentId) {
    const card = document.getElementById(`agent-card-${msgId}-${agentId}`);
    if (card) { card.classList.remove('pending'); card.classList.add('running'); }
    const textEl = document.getElementById(`agent-text-${msgId}-${agentId}`);
    if (textEl) textEl.textContent = 'Analyzing…';
  }

  _addAgentToolBadge(msgId, agentId, skillName) {
    const el = document.getElementById(`agent-tools-${msgId}-${agentId}`);
    if (!el) return;
    const badge = document.createElement('span');
    badge.className = 'agent-tool-badge';
    badge.textContent = skillName;
    el.appendChild(badge);
  }

  _updateAgentCardText(msgId, agentId, chunk) {
    if (!this._agentState[agentId]) return;
    this._agentState[agentId].textBuffer += chunk;
    const el = document.getElementById(`agent-text-${msgId}-${agentId}`);
    if (el) {
      const buf = this._agentState[agentId].textBuffer;
      const preview = buf.length > 160 ? '…' + buf.slice(-160) : buf;
      el.textContent = preview.replace(/[#*`]/g, '').trim();
    }
  }

  _finishAgentCard(msgId, agentId, conclusion) {
    const card = document.getElementById(`agent-card-${msgId}-${agentId}`);
    if (card) { card.classList.remove('running', 'pending'); card.classList.add('done'); }
    const textEl = document.getElementById(`agent-text-${msgId}-${agentId}`);
    if (textEl && conclusion) {
      const lines = conclusion.replace(/[#*`\[\]]/g, '').split('\n').filter(l => l.trim());
      const snippet = lines.slice(0, 2).join(' ').slice(0, 140);
      textEl.textContent = snippet + (snippet.length >= 140 ? '…' : '');
    }
  }

  _appendAggregateHeader(msgId) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const div = document.createElement('div');
    div.className = 'aggregate-header';
    div.id = `aggregate-header-${msgId}`;
    div.innerHTML = `<span class="aggregate-spinner"></span> Synthesising findings &amp; quantifying uncertainty…`;
    body.appendChild(div);
    this._scrollChat();
  }

  _chatContainer() { return document.getElementById('chat-messages'); }

  /** Only auto-scroll if user is already near the bottom (within 120px). */
  _scrollChat() {
    const c = this._chatContainer();
    const distFromBottom = c.scrollHeight - c.scrollTop - c.clientHeight;
    if (distFromBottom < 120) {
      c.scrollTop = c.scrollHeight;
    }
  }

  /** Always scroll to bottom — used when user sends a new message. */
  _scrollChatForce() {
    const c = this._chatContainer();
    c.scrollTop = c.scrollHeight;
  }

  _hideChatEmpty() {
    document.getElementById('chat-empty')?.remove();
  }

  _setInputEnabled(enabled) {
    document.getElementById('message-input').disabled = !enabled;
    document.getElementById('send-btn').disabled = !enabled;
  }

  async _compactChat() {
    const btn = document.getElementById('compact-chat-btn');
    btn.disabled = true;
    btn.textContent = 'Compacting…';
    try {
      const data = await this._api('POST', '/api/chat/compact');
      if (data.status === 'compacted') {
        this._appendCompactedNotice(data.old_count, data.summary);
        this._notify(`Compacted ${data.old_count} messages into a summary`, 'success');
      } else if (data.status === 'skipped') {
        this._notify('History is too short to compact', 'info');
      } else {
        this._notify('Compaction failed', 'error');
      }
    } catch (e) {
      this._notify(e.message, 'error');
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/>
        <line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/>
      </svg> Compact`;
    }
  }

  _appendCompactedNotice(oldCount, summary) {
    const el = document.createElement('div');
    el.className = 'compact-notice';
    el.innerHTML = `
      <div class="compact-notice-header">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/>
          <line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/>
        </svg>
        <span>Chat compacted · ${oldCount} messages → 1 summary</span>
      </div>
      ${summary ? `<div class="compact-notice-summary">${this._esc(summary)}</div>` : ''}`;
    this._chatContainer().appendChild(el);
    this._scrollChat();
  }

  async _clearChat() {
    try {
      await this._api('DELETE', '/api/chat/history');
      this._chatContainer().innerHTML = `
        <div id="chat-empty">
          <div class="brand-logo-wrap"><img src="/asset/logo_rmbg.png" class="brand-logo" /></div>
          <p>Upload tables from the sidebar, then ask questions or request operations on your data.</p>
          <div class="suggestion-chips">
            <div class="chip" onclick="app.insertSuggestion(this)">Summarize all uploaded tables</div>
            <div class="chip" onclick="app.insertSuggestion(this)">Find rows where value is null</div>
            <div class="chip" onclick="app.insertSuggestion(this)">Merge tables on a common column</div>
            <div class="chip" onclick="app.insertSuggestion(this)">Show top 10 rows sorted by first numeric column</div>
          </div>
        </div>`;
      this._notify('Chat history cleared', 'success');
    } catch (e) { this._notify(e.message, 'error'); }
  }

  // -----------------------------------------------------------------------
  // Plan modal
  // -----------------------------------------------------------------------

  _showPlanModal(plan) {
    const container = document.getElementById('plan-steps-container');
    document.getElementById('plan-modal-subtitle').textContent = plan.title || 'Review and edit steps before execution';
    container.innerHTML = '';
    (plan.steps || []).forEach(step => this._renderPlanStep(step, container));
    document.getElementById('plan-modal').classList.remove('hidden');
  }

  _renderPlanStep(step, container) {
    const div = document.createElement('div');
    div.className = 'plan-step-item';
    div.dataset.stepId = step.id;
    div.innerHTML = `
      <div class="plan-step-num">${step.id}</div>
      <textarea class="plan-step-text" rows="2">${this._esc(step.description)}</textarea>
      <div class="plan-step-controls">
        <button class="btn icon-only sm" title="Move up" onclick="app._movePlanStep(this, -1)">↑</button>
        <button class="btn icon-only sm" title="Move down" onclick="app._movePlanStep(this, 1)">↓</button>
        <button class="btn icon-only sm danger" title="Delete step" onclick="app._deletePlanStep(this)">×</button>
      </div>`;
    // Auto-resize the textarea
    const ta = div.querySelector('textarea');
    ta.addEventListener('input', () => { ta.style.height = 'auto'; ta.style.height = ta.scrollHeight + 'px'; });
    container.appendChild(div);
    // Trigger resize
    setTimeout(() => { ta.style.height = 'auto'; ta.style.height = ta.scrollHeight + 'px'; }, 0);
  }

  _addPlanStep() {
    const container = document.getElementById('plan-steps-container');
    const steps = container.querySelectorAll('.plan-step-item');
    const nextId = steps.length + 1;
    this._renderPlanStep({ id: nextId, description: '' }, container);
    this._renumberPlanSteps();
  }

  _deletePlanStep(btn) {
    btn.closest('.plan-step-item').remove();
    this._renumberPlanSteps();
  }

  _movePlanStep(btn, delta) {
    const item = btn.closest('.plan-step-item');
    const container = item.parentElement;
    const items = [...container.querySelectorAll('.plan-step-item')];
    const idx = items.indexOf(item);
    const target = delta === -1 ? idx - 1 : idx + 2;
    if (target < 0 || target > items.length) return;
    container.insertBefore(item, items[target] || null);
    this._renumberPlanSteps();
  }

  _renumberPlanSteps() {
    document.querySelectorAll('#plan-steps-container .plan-step-item').forEach((el, i) => {
      el.dataset.stepId = i + 1;
      el.querySelector('.plan-step-num').textContent = i + 1;
    });
  }

  hidePlanModal() {
    document.getElementById('plan-modal').classList.add('hidden');
  }

  async _executePlan() {
    const items = document.querySelectorAll('#plan-steps-container .plan-step-item');
    const steps = [...items].map((el, i) => ({
      id: i + 1,
      description: el.querySelector('textarea').value.trim(),
    })).filter(s => s.description);

    if (!steps.length) { this._notify('No steps to execute', 'error'); return; }

    this.hidePlanModal();
    await this._streamChat(this.state.currentPlanMessage, true, steps);
  }

  // -----------------------------------------------------------------------
  // Skills
  // -----------------------------------------------------------------------

  async _loadSkills() {
    try {
      this.state.skills = await this._api('GET', '/api/skills');
      this._renderSkills();
    } catch (e) { console.error('loadSkills', e); }
  }

  _renderSkills() {
    const list = document.getElementById('skills-list');
    const { builtin, packages } = this.state.skills;
    let html = '<div class="skill-section-title">Built-in Skills</div>';
    html += (builtin || []).map(s => `
      <div class="skill-item skill-item-clickable" onclick="app.showBuiltinSkillDetail('${this._esc(s.name)}')">
        <span class="skill-dot builtin"></span>
        <div class="skill-info">
          <div class="skill-name">${this._esc(s.name)}</div>
          <div class="skill-desc">${this._esc(s.description)}</div>
          <div class="skill-category">${this._esc(s.category || '')}</div>
        </div>
        <button class="btn icon-only sm skill-info-btn" title="View details" onclick="event.stopPropagation();app.showBuiltinSkillDetail('${this._esc(s.name)}')">ℹ</button>
      </div>`).join('');

    // Package skills — ClawHub / OpenClaw-compatible SKILL.md format
    html += '<hr class="divider"><div class="skill-section-title">Package Skills</div>';
    if (!packages || !packages.length) {
      html += '<div class="empty-state">No package skills installed.<br>Click <b>📦 Import</b> to load a .zip or <b>+ Add</b> to create one.</div>';
    } else {
      html += packages.map(s => {
        const sourceLabel = s.source === 'distilled'
          ? '<span class="skill-badge-distilled" title="Auto-learned from session">🧠</span>'
          : '';
        return `
        <div class="skill-item${s.enabled ? '' : ' skill-disabled'}">
          <span class="skill-dot package"></span>
          <div class="skill-info">
            <div class="skill-name">${sourceLabel}${this._esc(s.name)}${s.version ? ` <span class="skill-version">v${this._esc(s.version)}</span>` : ''}</div>
            <div class="skill-desc">${this._esc(s.description)}</div>
          </div>
          <div class="skill-actions">
            <button class="btn icon-only sm" title="${s.enabled ? 'Disable' : 'Enable'}" onclick="app._togglePackageSkill('${this._esc(s.slug)}', ${!s.enabled})">${s.enabled ? '⏸' : '▶'}</button>
            <button class="btn icon-only sm danger" onclick="app._deletePackageSkill('${this._esc(s.slug)}')">🗑</button>
          </div>
        </div>`;
      }).join('');
    }
    list.innerHTML = html;
  }

  showSkillModal() {
    document.getElementById('skill-modal-title').textContent = 'Add Skill';
    document.getElementById('skill-name-input').value = '';
    document.getElementById('skill-desc-input').value = '';
    document.getElementById('skill-body-input').value = '';
    document.getElementById('skill-modal').classList.remove('hidden');
  }

  hideSkillModal() {
    document.getElementById('skill-modal').classList.add('hidden');
  }

  // Built-in skill detail (read-only)
  showBuiltinSkillDetail(skillName) {
    const skill = (this.state.skills.builtin || []).find(s => s.name === skillName);
    if (!skill) return;

    document.getElementById('skill-detail-name').textContent = skill.name;
    document.getElementById('skill-detail-category').textContent =
      `Category: ${skill.category || 'general'}`;

    // Build the detail body
    const params = skill.parameters || {};
    const props = params.properties || {};
    const required = params.required || [];

    let paramsHtml = '';
    const entries = Object.entries(props);
    if (entries.length) {
      paramsHtml = `
        <table class="skill-params-table">
          <thead><tr><th>Parameter</th><th>Type</th><th>Required</th><th>Description</th></tr></thead>
          <tbody>${entries.map(([pname, pdef]) => `
            <tr>
              <td><code>${this._esc(pname)}</code></td>
              <td>${this._esc(pdef.type || '—')}</td>
              <td>${required.includes(pname) ? '<span class="badge green">yes</span>' : '<span class="badge" style="background:var(--surface2);color:var(--text-dim)">no</span>'}</td>
              <td>${this._esc(pdef.description || '—')}</td>
            </tr>`).join('')}
          </tbody>
        </table>`;
    } else {
      paramsHtml = '<p style="color:var(--text-dim);font-size:12px">No parameters.</p>';
    }

    document.getElementById('skill-detail-body').innerHTML = `
      <div class="skill-detail-desc">${this._esc(skill.description)}</div>
      <div class="skill-detail-section-title">Parameters</div>
      ${paramsHtml}`;

    // "Use as Template" pre-fills the Add Skill form
    document.getElementById('skill-detail-use-btn').onclick = () => {
      this.hideSkillDetailModal();
      this.showSkillModal();
      document.getElementById('skill-desc-input').value = skill.description;
      document.getElementById('skill-body-input').value =
        `## Pattern: ${skill.name}\n\nWhen asked to ${skill.description.toLowerCase()}:\n\n1. Call \`table_info\` to inspect the table structure\n2. Use \`${skill.name}\` with the appropriate parameters\n3. Summarise the result clearly\n\nCustomize these steps to combine with other built-in skills or add pre/post-processing logic.`;
    };

    document.getElementById('skill-detail-modal').classList.remove('hidden');
  }

  hideSkillDetailModal() {
    document.getElementById('skill-detail-modal').classList.add('hidden');
  }

  async _saveSkill() {
    const name = document.getElementById('skill-name-input').value.trim();
    const description = document.getElementById('skill-desc-input').value.trim();
    const body = document.getElementById('skill-body-input').value.trim();
    if (!name || !description) { this._notify('Name and description are required', 'error'); return; }
    if (!body) { this._notify('Instructions body is required', 'error'); return; }
    try {
      await this._api('POST', '/api/skills/create', { name, description, body });
      this._notify('Skill created', 'success');
      this.hideSkillModal();
      await this._loadSkills();
    } catch (e) { this._notify(e.message, 'error'); }
  }

  async _clearAllSkills() {
    const packages = this.state.skills.packages || [];
    if (!packages.length) { this._notify('No package skills to clear', 'info'); return; }
    if (!confirm(`清空全部 ${packages.length} 个 Skill？此操作不可撤销。`)) return;
    try {
      await this._api('DELETE', '/api/skills');
      this._notify('All skills cleared', 'success');
      await this._loadSkills();
    } catch (e) { this._notify(e.message, 'error'); }
  }

  async _importSkillZip(file) {
    const form = new FormData();
    form.append('file', file);
    try {
      const resp = await fetch('/api/skills/import', { method: 'POST', body: form });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.detail || 'Import failed');
      }
      const result = await resp.json();
      this._notify(`Skill "${result.name || result.slug}" installed`, 'success');
      await this._loadSkills();
    } catch (e) { this._notify(e.message, 'error'); }
  }

  async _togglePackageSkill(slug, enabled) {
    try {
      await this._api('PUT', `/api/skills/package/${slug}/toggle`, { enabled });
      this._notify(enabled ? 'Skill enabled' : 'Skill disabled', 'success');
      await this._loadSkills();
    } catch (e) { this._notify(e.message, 'error'); }
  }

  async _deletePackageSkill(slug) {
    if (!confirm(`Delete package skill "${slug}"? This removes the entire skill directory.`)) return;
    try {
      await this._api('DELETE', `/api/skills/package/${slug}`);
      this._notify('Package skill deleted', 'success');
      await this._loadSkills();
    } catch (e) { this._notify(e.message, 'error'); }
  }

  // -----------------------------------------------------------------------
  // Memory
  // -----------------------------------------------------------------------

  async _loadMemory() {
    try {
      this.state.memory = await this._api('GET', '/api/memory');
      this._renderMemory();
    } catch (e) { console.error('loadMemory', e); }
  }

  _renderMemory() {
    const list = document.getElementById('memory-list');
    const mem = this.state.memory;
    const catLabels = {
      preferences: 'Preferences',
      domain_knowledge: 'Domain Knowledge',
      user_context: 'User Context',
      history_insights: 'History Insights',
    };
    let html = '';
    let total = 0;
    for (const [cat, items] of Object.entries(mem)) {
      const keys = Object.keys(items);
      total += keys.length;
      const catLabel = catLabels[cat] || cat;
      html += `
        <div class="memory-category">
          <div class="memory-category-header">
            ${catLabel}
            <span class="count">${keys.length}</span>
          </div>`;
      if (!keys.length) {
        html += '<div class="memory-empty">Empty</div>';
      } else {
        html += keys.map(key => {
          const entry = items[key];
          const val = typeof entry === 'object' ? entry.value : entry;
          const upd = typeof entry === 'object' && entry.updated ? entry.updated.slice(0, 10) : '';
          return `
            <div class="memory-item">
              <div style="flex:1;min-width:0">
                <div style="display:flex;gap:6px;align-items:baseline">
                  <span class="memory-key">${this._esc(key)}</span>
                  <span class="memory-value">${this._esc(String(val))}</span>
                </div>
                ${upd ? `<div class="memory-updated">${upd}</div>` : ''}
              </div>
              <div class="mem-actions">
                <button class="btn icon-only sm" onclick="app.showMemoryModal('${this._esc(cat)}','${this._esc(key)}')">✏</button>
                <button class="btn icon-only sm danger" onclick="app._deleteMemory('${this._esc(cat)}','${this._esc(key)}')">🗑</button>
              </div>
            </div>`;
        }).join('');
      }
      html += '</div>';
    }
    if (total === 0) {
      html += '<div class="empty-state">Memory is empty.<br>The system learns your preferences over time,<br>or you can add items manually.</div>';
    }
    list.innerHTML = html;
  }

  showMemoryModal(category, key) {
    if (category && key) {
      this.state.memoryEdit = { category, key };
      document.getElementById('memory-modal-title').textContent = 'Edit Memory';
      const entry = (this.state.memory[category] || {})[key];
      const val = typeof entry === 'object' ? entry.value : (entry || '');
      document.getElementById('mem-cat-input').value = category;
      document.getElementById('mem-key-input').value = key;
      document.getElementById('mem-val-input').value = val;
    } else {
      this.state.memoryEdit = null;
      document.getElementById('memory-modal-title').textContent = 'Add Memory';
      document.getElementById('mem-cat-input').value = 'preferences';
      document.getElementById('mem-key-input').value = '';
      document.getElementById('mem-val-input').value = '';
    }
    document.getElementById('memory-modal').classList.remove('hidden');
  }

  hideMemoryModal() {
    document.getElementById('memory-modal').classList.add('hidden');
  }

  async _saveMemory() {
    const category = document.getElementById('mem-cat-input').value;
    const key = document.getElementById('mem-key-input').value.trim();
    const value = document.getElementById('mem-val-input').value.trim();
    if (!key || !value) { this._notify('Key and value required', 'error'); return; }
    try {
      // If editing and key changed, delete old entry first
      if (this.state.memoryEdit && this.state.memoryEdit.key !== key) {
        await this._api('DELETE', `/api/memory/${this.state.memoryEdit.category}/${encodeURIComponent(this.state.memoryEdit.key)}`);
      }
      await this._api('POST', '/api/memory', { category, key, value });
      this._notify('Memory saved', 'success');
      this.hideMemoryModal();
      await this._loadMemory();
    } catch (e) { this._notify(e.message, 'error'); }
  }

  async _deleteMemory(category, key) {
    try {
      await this._api('DELETE', `/api/memory/${category}/${encodeURIComponent(key)}`);
      this._notify('Memory item deleted', 'success');
      await this._loadMemory();
    } catch (e) { this._notify(e.message, 'error'); }
  }

  async _clearAllMemory() {
    const mem = this.state.memory || {};
    const total = Object.values(mem).reduce((n, cat) => n + Object.keys(cat).length, 0);
    if (!total) { this._notify('Memory is already empty', 'info'); return; }
    if (!confirm(`清空全部 ${total} 条 Memory？此操作不可撤销。`)) return;
    try {
      await this._api('DELETE', '/api/memory');
      this._notify('All memory cleared', 'success');
      await this._loadMemory();
    } catch (e) { this._notify(e.message, 'error'); }
  }

  async _forgetMemory() {
    const input = document.getElementById('forget-input');
    const query = input.value.trim();
    if (!query) return;
    try {
      const result = await this._api('POST', '/api/memory/forget', { query });
      if (result.count > 0) {
        this._notify(`Forgot ${result.count} item(s)`, 'success');
        await this._loadMemory();
      } else {
        this._notify('No matching memories found', 'info');
      }
      input.value = '';
    } catch (e) { this._notify(e.message, 'error'); }
  }

  async _summarizeMemory() {
    const modal = document.getElementById('memory-summary-modal');
    const loading = document.getElementById('memory-summary-loading');
    const content = document.getElementById('memory-summary-content');
    const subtitle = document.getElementById('memory-summary-subtitle');
    const copyBtn = document.getElementById('memory-summary-copy-btn');
    const refreshBtn = document.getElementById('memory-summary-refresh-btn');

    // Open modal and show loading state
    content.innerHTML = '';
    loading.classList.remove('hidden');
    subtitle.textContent = '由 AI 根据当前记忆整理生成';
    modal.classList.remove('hidden');

    const doGenerate = async () => {
      content.innerHTML = '';
      loading.classList.remove('hidden');
      try {
        const result = await this._api('POST', '/api/memory/summarize');
        this._summaryText = result.summary || '';
        content.innerHTML = this._renderMarkdown(this._summaryText);
        const now = new Date().toLocaleTimeString();
        subtitle.textContent = `生成于 ${now}`;
      } catch (e) {
        content.innerHTML = `<span style="color:var(--red)">生成失败: ${this._esc(e.message)}</span>`;
      } finally {
        loading.classList.add('hidden');
      }
    };

    // Copy button
    copyBtn.onclick = () => {
      if (!this._summaryText) return;
      navigator.clipboard.writeText(this._summaryText)
        .then(() => this._notify('Copied to clipboard', 'success'))
        .catch(() => this._notify('Copy failed', 'error'));
    };

    // Regenerate button
    refreshBtn.onclick = () => doGenerate();

    await doGenerate();
  }

  hideMemorySummaryModal() {
    document.getElementById('memory-summary-modal').classList.add('hidden');
  }

  // -----------------------------------------------------------------------
  // Utilities
  // -----------------------------------------------------------------------

  _esc(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  /** Markdown → safe HTML via marked.js */
  _renderMarkdown(text) {
    if (!text) return '';
    try {
      return marked.parse(text);
    } catch {
      return this._esc(text).replace(/\n/g, '<br>');
    }
  }

  _notify(message, type = 'info') {
    const container = document.getElementById('notifications');
    const el = document.createElement('div');
    el.className = `notification ${type}`;
    const icons = { success: '✓', error: '✕', info: 'ℹ' };
    el.innerHTML = `<span>${icons[type] || ''}</span> ${this._esc(message)}`;
    container.appendChild(el);
    setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity 0.3s'; }, 2700);
    setTimeout(() => el.remove(), 3000);
  }

  // -----------------------------------------------------------------------
  // Demo — one-click experience
  // -----------------------------------------------------------------------

  showDemoModal() {
    this._renderDemoScenarios();
    document.getElementById('demo-modal').classList.remove('hidden');
  }

  hideDemoModal() {
    document.getElementById('demo-modal').classList.add('hidden');
  }

  _renderDemoScenarios() {
    const grid = document.getElementById('demo-scenarios-grid');
    grid.innerHTML = DEMO_SCENARIOS.map(s => `
      <div class="demo-card">
        <div class="demo-card-header">
          <span class="demo-card-icon">${s.icon}</span>
          <div>
            <div class="demo-card-title">${this._esc(s.title)}</div>
            <div class="demo-card-steps-count">${s.queries.length} 个分析步骤</div>
          </div>
        </div>
        <div class="demo-card-desc">${this._esc(s.description)}</div>
        <div class="demo-card-files">
          ${s.files.map(f => `<span class="badge blue">${this._esc(f)}</span>`).join('')}
        </div>
        <ul class="demo-card-steps-list">
          ${s.queries.slice(0, 2).map(q => `<li>${this._esc(q.slice(0, 58))}${q.length > 58 ? '…' : ''}</li>`).join('')}
          ${s.queries.length > 2 ? `<li class="more-steps">+ ${s.queries.length - 2} 更多步骤…</li>` : ''}
        </ul>
        <button class="btn primary" style="width:100%;justify-content:center;margin-top:4px"
                onclick="app.runDemo('${s.id}')">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" style="margin-right:4px">
            <polygon points="5 3 19 12 5 21 5 3"/>
          </svg>
          开始演示
        </button>
      </div>`).join('');
  }

  async runDemo(scenarioId) {
    const scenario = DEMO_SCENARIOS.find(s => s.id === scenarioId);
    if (!scenario) return;

    this.hideDemoModal();
    this.state.demoRunning = true;

    // Show demo control bar
    document.getElementById('demo-control-title').textContent = scenario.title;
    document.getElementById('demo-control-step').textContent = '';
    document.getElementById('demo-control').classList.remove('hidden');

    // 1. Load example files
    this._appendSystemMessage(`⏳ 正在加载示例数据：${scenario.files.join('、')}…`);
    try {
      const result = await this._api('POST', '/api/demo/load', {
        files: scenario.files,
        clear: true,
      });
      await this._loadTables();
      const names = result.loaded.map(t => `${t.name}（${t.rows} 行 × ${t.cols} 列）`).join('，');
      this._appendSystemMessage(`✓ 数据加载完成：${names}`);
    } catch (e) {
      this._appendSystemMessage(`✗ 数据加载失败：${e.message}`);
      this._demoCleanup();
      return;
    }

    // 2. Temporarily disable plan mode for automated run
    const prevPlanMode = this.state.planMode;
    this.state.planMode = false;
    document.getElementById('plan-mode-check').checked = false;

    // 3. Execute each query in sequence
    for (let i = 0; i < scenario.queries.length; i++) {
      if (!this.state.demoRunning) break;

      // Update progress
      document.getElementById('demo-control-step').textContent =
        `· 步骤 ${i + 1} / ${scenario.queries.length}`;

      // Divider in chat
      this._appendDemoStepDivider(i + 1, scenario.queries.length, scenario.queries[i]);

      // Show as user message and stream response
      this._hideChatEmpty();
      this._appendUserMessage(scenario.queries[i]);
      await this._streamChat(scenario.queries[i]);

      // Small pause so the user can see the result before the next step
      if (i < scenario.queries.length - 1 && this.state.demoRunning) {
        await new Promise(r => setTimeout(r, 600));
      }
    }

    // 4. Restore plan mode
    this.state.planMode = prevPlanMode;
    document.getElementById('plan-mode-check').checked = prevPlanMode;

    const finished = this.state.demoRunning; // false means user stopped it
    this._demoCleanup();

    if (finished) {
      this._appendSystemMessage(
        `🎉 演示完成！「${scenario.title}」共执行 ${scenario.queries.length} 个分析步骤。`
      );
    } else {
      this._appendSystemMessage('⏹ 演示已停止。');
    }
  }

  stopDemo() {
    this.state.demoRunning = false;
  }

  _demoCleanup() {
    this.state.demoRunning = false;
    document.getElementById('demo-control').classList.add('hidden');
  }

  /** Insert a visual step divider between demo queries. */
  _appendDemoStepDivider(stepNum, total, queryText) {
    const el = document.createElement('div');
    el.className = 'demo-step-divider';
    const shortQ = queryText.length > 50 ? queryText.slice(0, 50) + '…' : queryText;
    el.innerHTML = `
      <div class="demo-step-line"></div>
      <span class="demo-step-label">步骤 ${stepNum} / ${total}</span>
      <div class="demo-step-line"></div>`;
    this._chatContainer().appendChild(el);
    this._scrollChat();
  }

  /** Render a small centered system info line in the chat. */
  _appendSystemMessage(text) {
    const el = document.createElement('div');
    el.className = 'system-message';
    el.innerHTML = `<span>${this._esc(text)}</span>`;
    this._chatContainer().appendChild(el);
    this._scrollChat();
  }

  // -----------------------------------------------------------------------
  // Intent Clarification
  // -----------------------------------------------------------------------

  _showClarificationCard(originalMsg, question, options) {
    const cardId = `clarify-${Date.now()}`;
    const el = document.createElement('div');
    el.className = 'clarify-bubble';
    el.id = cardId;
    el.dataset.originalMsg = originalMsg;

    const optionsHtml = (options || []).map((opt, i) =>
      `<button class="clarify-option" onclick="app._selectClarifyOption('${cardId}', ${i})">${this._esc(opt)}</button>`
    ).join('');

    el.innerHTML = `
      <div class="clarify-question">${this._esc(question)}</div>
      <div class="clarify-options" id="${cardId}-opts">${optionsHtml}</div>
      <div class="clarify-custom-row" id="${cardId}-custom">
        <input type="text" class="clarify-input" id="${cardId}-input"
               placeholder="或自定义说明…"
               onkeydown="if(event.key==='Enter')app._submitClarification('${cardId}',null)" />
        <button class="clarify-submit-btn" onclick="app._submitClarification('${cardId}',null)">确认</button>
      </div>`;

    this._chatContainer().appendChild(el);
    this._scrollChat();
  }

  _selectClarifyOption(cardId, optionIndex) {
    const card = document.getElementById(cardId);
    if (!card || card.classList.contains('answered')) return;
    const buttons = card.querySelectorAll('.clarify-option');
    const selectedText = buttons[optionIndex]?.textContent || '';
    this._submitClarification(cardId, selectedText);
  }

  _submitClarification(cardId, selectedText) {
    const card = document.getElementById(cardId);
    if (!card || card.classList.contains('answered')) return;

    const originalMsg = card.dataset.originalMsg || '';

    if (selectedText === null) {
      const input = document.getElementById(`${cardId}-input`);
      selectedText = input?.value.trim() || '';
      if (!selectedText) return;
    }

    // Lock the card
    card.classList.add('answered');
    card.querySelectorAll('.clarify-option').forEach(btn => {
      if (btn.textContent === selectedText) btn.classList.add('selected');
    });
    const customRow = document.getElementById(`${cardId}-custom`);
    if (customRow) customRow.style.display = 'none';

    const clarifiedMsg = `${originalMsg}\n\n[用户补充说明: ${selectedText}]`;

    if (this.state.planMode) {
      this._generateAndShowPlan(clarifiedMsg);
    } else {
      this._streamChat(clarifiedMsg);
    }
  }

  // -----------------------------------------------------------------------
  // Skill Learning Badge
  // -----------------------------------------------------------------------

  _appendSkillLearnedBadge(msgId, skill) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const div = document.createElement('div');
    div.className = 'skill-learned-badge';
    div.innerHTML = `
      <span class="skill-learned-icon">🧠</span>
      <span class="skill-learned-text">
        从本次推理中抽象出新 Skill：<strong>${this._esc(skill.name)}</strong>
        — ${this._esc(skill.description)}
      </span>
      <span class="skill-learned-mode">📦 package</span>`;
    body.appendChild(div);
    this._scrollChat();
    this._notify(`New skill learned: ${skill.name}`, 'success');
  }

  // -----------------------------------------------------------------------
  // Feedback buttons (👍/👎)
  // -----------------------------------------------------------------------

  _appendFeedbackButtons(msgId, sessionId) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const div = document.createElement('div');
    div.className = 'feedback-actions';
    div.id = `feedback-${msgId}`;
    div.innerHTML = `
      <button class="feedback-btn good" onclick="app.sendFeedback('${sessionId}', 'good', '${msgId}')" title="有用">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 9V5a3 3 0 00-3-3l-4 9v11h11.28a2 2 0 002-1.7l1.38-9a2 2 0 00-2-2.3H14z"/><path d="M7 22H4a2 2 0 01-2-2v-7a2 2 0 012-2h3"/></svg>
      </button>
      <button class="feedback-btn bad" onclick="app.sendFeedback('${sessionId}', 'bad', '${msgId}')" title="不准确">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 15v4a3 3 0 003 3l4-9V2H5.72a2 2 0 00-2 1.7l-1.38 9a2 2 0 002 2.3H10z"/><path d="M17 2h3a2 2 0 012 2v7a2 2 0 01-2 2h-3"/></svg>
      </button>`;
    body.appendChild(div);
  }

  async sendFeedback(sessionId, feedback, msgId) {
    const container = document.getElementById(`feedback-${msgId}`);
    if (!container) return;
    try {
      await this._api('POST', `/api/workflow/${sessionId}/feedback`, { feedback });
      container.innerHTML = feedback === 'good'
        ? '<span class="feedback-done good">👍 感谢反馈</span>'
        : '<span class="feedback-done bad">👎 已记录，将用于改进</span>';
      this._notify(feedback === 'good' ? '感谢正面反馈！' : '已记录，TabClaw 将从中学习', 'success');
    } catch (e) {
      this._notify('反馈提交失败', 'error');
    }
  }

  // -----------------------------------------------------------------------
  // Skill-reused hint
  // -----------------------------------------------------------------------

  _appendSkillReusedHint(msgId, skillName, message) {
    const body = document.getElementById(`${msgId}-body`);
    if (!body) return;
    const div = document.createElement('div');
    div.className = 'skill-reused-hint';
    div.innerHTML = `
      <span class="skill-reused-icon">💡</span>
      <span class="skill-reused-text">${this._esc(message || `复用已学技能「${skillName}」`)}</span>`;
    body.appendChild(div);
    this._scrollChat();
  }

  // -----------------------------------------------------------------------
  // Growth dashboard
  // -----------------------------------------------------------------------

  async showGrowthDashboard() {
    const modal = document.getElementById('growth-modal');
    const content = document.getElementById('growth-content');
    if (!modal || !content) return;

    content.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted)">Loading…</div>';
    modal.classList.remove('hidden');

    try {
      const profile = await this._api('GET', '/api/growth/profile');
      const rate = profile.satisfaction_rate !== null
        ? `${Math.round(profile.satisfaction_rate * 100)}%` : '—';
      const skillStats = (profile.skills_stats || []);
      const topTools = Object.entries(profile.tool_frequency || {}).slice(0, 8);

      let skillsHtml = '';
      if (skillStats.length) {
        skillsHtml = `<div class="growth-section">
          <div class="growth-section-title">已学技能</div>
          <div class="growth-skills-list">${skillStats.map(s => `
            <div class="growth-skill-item">
              <span class="growth-skill-name">${this._esc(s.name)}</span>
              <span class="growth-skill-meta">v${s.version} · 使用 ${s.usage_count} 次 · 👍${s.success_count} 👎${s.failure_count}</span>
            </div>`).join('')}
          </div>
        </div>`;
      }

      let toolsHtml = '';
      if (topTools.length) {
        const maxCount = topTools[0]?.[1] || 1;
        toolsHtml = `<div class="growth-section">
          <div class="growth-section-title">工具使用频率</div>
          <div class="growth-tools-list">${topTools.map(([name, count]) => `
            <div class="growth-tool-bar-row">
              <span class="growth-tool-name">${this._esc(name)}</span>
              <div class="growth-tool-bar"><div class="growth-tool-bar-fill" style="width:${Math.round(count / maxCount * 100)}%"></div></div>
              <span class="growth-tool-count">${count}</span>
            </div>`).join('')}
          </div>
        </div>`;
      }

      let eventsHtml = '';
      if (profile.recent_events && profile.recent_events.length) {
        eventsHtml = `<div class="growth-section">
          <div class="growth-section-title">成长时间线</div>
          <div class="growth-timeline">${profile.recent_events.map(e => `
            <div class="growth-event">
              <span class="growth-event-date">${this._esc(e.date)}</span>
              <span class="growth-event-text">${this._esc(e.event)}</span>
            </div>`).join('')}
          </div>
        </div>`;
      }

      content.innerHTML = `
        <div class="growth-stats-grid">
          <div class="growth-stat-card">
            <div class="growth-stat-value">${profile.total_sessions}</div>
            <div class="growth-stat-label">分析会话</div>
          </div>
          <div class="growth-stat-card">
            <div class="growth-stat-value">${profile.skills_learned}</div>
            <div class="growth-stat-label">已学技能</div>
          </div>
          <div class="growth-stat-card">
            <div class="growth-stat-value">${rate}</div>
            <div class="growth-stat-label">满意率</div>
          </div>
          <div class="growth-stat-card">
            <div class="growth-stat-value">${profile.feedback_counts?.good || 0} / ${profile.feedback_counts?.bad || 0}</div>
            <div class="growth-stat-label">👍 / 👎</div>
          </div>
        </div>
        ${skillsHtml}
        ${toolsHtml}
        ${eventsHtml}
        ${!skillStats.length && !topTools.length
          ? '<div class="empty-state" style="margin-top:20px">还没有分析记录。开始使用 TabClaw 后，这里会展示成长轨迹。</div>'
          : ''}`;
    } catch (e) {
      content.innerHTML = `<div style="padding:20px;color:var(--red)">${this._esc(e.message)}</div>`;
    }
  }

  hideGrowthModal() {
    const modal = document.getElementById('growth-modal');
    if (modal) modal.classList.add('hidden');
  }
}

// Start the app
const app = new TabClawApp();
