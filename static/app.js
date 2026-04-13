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
      implicitFeedbackEnabled: false,
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

    // Stores the workflow_id of the last completed response for implicit feedback
    this._lastCompletedWorkflowId = null;
    // Maps workflow session_id → feedback message DOM id (for implicit feedback UI updates)
    this._workflowMsgMap = {};

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
    this._applyLangLabels();
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

    document.getElementById('new-table-btn').addEventListener('click', () => this._createBlankTable());

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
    document.getElementById('implicit-feedback-check').addEventListener('change', e => {
      this.state.implicitFeedbackEnabled = e.target.checked;
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
    document.getElementById('discover-skill-btn').addEventListener('click', () => this._discoverSkills());
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
    const allModals = ['plan-modal', 'table-modal', 'skill-modal', 'memory-modal', 'demo-modal', 'growth-modal', 'pkg-detail-modal', 'guide-modal', 'features-guide-modal'];
    allModals.forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('click', e => {
        if (e.target.id === id) this._closeModalById(id);
      });
    });

    // Escape key closes modals
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') {
        allModals.forEach(id => {
          const el = document.getElementById(id);
          if (el && !el.classList.contains('hidden')) this._closeModalById(id);
        });
      }
    });

    // First-visit guide
    if (!localStorage.getItem('tabclaw_guide_dismissed')) {
      setTimeout(() => this.showGuideModal(), 800);
    }
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
    else if (id === 'pkg-detail-modal') this.hidePkgDetailModal();
    else if (id === 'guide-modal') this.hideGuideModal();
    else if (id === 'features-guide-modal') this.hideFeaturesGuideModal();
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
      this._refreshThemeButtonTitle(isLight);
    }
    localStorage.setItem('theme', theme);
  }

  _refreshThemeButtonTitle(isLight) {
    const btn = document.getElementById('theme-btn');
    if (!btn) return;
    const zh = this._lang === 'zh';
    btn.title = zh
      ? (isLight ? '切换夜间模式' : '切换日间模式')
      : (isLight ? 'Switch to dark mode' : 'Switch to light mode');
  }

  _toggleTheme() {
    const current = document.documentElement.classList.contains('light') ? 'light' : 'dark';
    this._applyTheme(current === 'light' ? 'dark' : 'light');
  }

  _toggleLang() {
    this._lang = this._lang === 'en' ? 'zh' : 'en';
    localStorage.setItem('lang', this._lang);
    this._applyLangLabels();
    const growthOpen = document.getElementById('growth-modal');
    if (growthOpen && !growthOpen.classList.contains('hidden')) this.showGrowthDashboard();
    const demoModal = document.getElementById('demo-modal');
    if (demoModal && !demoModal.classList.contains('hidden')) this._renderDemoScenarios();
  }

  /** Self-evolution guide body (replaces #guide-content). */
  _selfEvolutionGuideInnerHtml() {
    const zh = this._lang === 'zh';
    const steps = zh
      ? [
        { t: '📊 上传表格，提出问题', d: '上传 CSV 或 Excel 文件，用自然语言描述你想要的分析。TabClaw 会制定计划、调用工具、给出结论。' },
        { t: '👍👎 给出反馈', d: '每次分析完成后，点击 <strong>👍</strong> 或 <strong>👎</strong> 告诉 TabClaw 效果如何。这是它学习的核心信号——不满意的结果会驱动改进。' },
        { t: '🧠 自动学习技能', d: '开启底部的 <strong>Skill Learning</strong> 开关后，TabClaw 会从复杂任务中自动提炼可复用的分析技能。你也可以点击 Skills 面板的 <strong>🔍</strong> 按钮，主动扫描历史记录发现重复模式并采纳为技能。' },
        { t: '⬆ 反馈驱动进化', d: '当某个技能累计收到 <strong>2 次以上 👎</strong> 时，TabClaw 会自动分析失败原因并升级该技能。技能版本号递增，旧版本保留。你也可以在技能详情里手动点 <strong>⬆ Improve</strong> 触发改进。' },
        { t: '📈 越来越好', d: '下次问类似问题时，进化后的技能会自动注入 Agent 的思维链，让分析更准确、更有条理。点击侧边栏 <strong>成长报告</strong> 查看领域熟练度、效率变化和里程碑。' },
      ]
      : [
        { t: '📊 Upload tables and ask questions', d: 'Upload CSV or Excel, describe what you want in plain language. TabClaw plans, calls tools, and answers.' },
        { t: '👍👎 Give feedback', d: 'After each reply, use <strong>👍</strong> or <strong>👎</strong>. This is the main learning signal—bad outcomes drive fixes.' },
        { t: '🧠 Skill learning', d: 'Turn on <strong>Skill Learning</strong> below to distil reusable skills from harder tasks. Or use <strong>🔍</strong> in Skills to scan history and adopt patterns as skills.' },
        { t: '⬆ Feedback-driven upgrades', d: 'After <strong>two or more 👎</strong> on a skill, TabClaw analyses failures and upgrades it (version bumps; old versions kept). You can also click <strong>⬆ Improve</strong> in skill details.' },
        { t: '📈 Keeps improving', d: 'On similar questions later, upgraded skills feed the agent. Open <strong>Growth report</strong> in the sidebar for domains, efficiency, and milestones.' },
      ];
    const tipsTitle = zh ? '💡 快速上手建议' : '💡 Quick tips';
    const tips = zh
      ? [
        '<strong>多给反馈</strong>——👍👎 是 TabClaw 进化的燃料',
        '<strong>开启 Skill Learning</strong>——让系统自动从每次分析中学习',
        '<strong>定期点 🔍 发现技能</strong>——从历史中挖掘你还没注意到的分析模式',
        '<strong>查看成长报告</strong>——观察 TabClaw 在各个领域的进步',
      ]
      : [
        '<strong>Give feedback often</strong> — 👍👎 is the fuel for improvement.',
        '<strong>Enable Skill Learning</strong> so the system learns from each task.',
        '<strong>Use 🔍 Discover</strong> to turn recurring patterns into skills.',
        '<strong>Open Growth report</strong> to see progress by domain.',
      ];
    const flow = steps.map((s, i) => `
      ${i ? '<div class="guide-arrow">↓</div>' : ''}
      <div class="guide-step">
        <div class="guide-step-num">${i + 1}</div>
        <div class="guide-step-body">
          <div class="guide-step-title">${s.t}</div>
          <div class="guide-step-desc">${s.d}</div>
        </div>
      </div>`).join('');
    const tipsUl = tips.map(li => `<li>${li}</li>`).join('');
    return `<div class="guide-flow">${flow}</div>
      <div class="guide-tips">
        <div class="guide-tips-title">${tipsTitle}</div>
        <ul>${tipsUl}</ul>
      </div>`;
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
    const implicitFbLabel = document.getElementById('implicit-feedback-label-text');
    if (implicitFbLabel) implicitFbLabel.textContent = zh ? '隐式反馈' : 'Implicit Feedback';
    const implicitFbHint = document.getElementById('implicit-feedback-hint-span');
    if (implicitFbHint) implicitFbHint.textContent = zh ? '— 默认关闭' : '— auto off';
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
    const tablesTitle = document.getElementById('tables-panel-title');
    if (tablesTitle) tablesTitle.textContent = zh ? '数据表' : 'Tables';
    const newTableBtn = document.getElementById('new-table-btn');
    if (newTableBtn) newTableBtn.textContent = zh ? '+ 新建空白表格' : '+ New blank table';
    document.getElementById('compact-chat-btn')?.setAttribute('title', zh ? '将长对话压缩为一条摘要' : 'Compact chat history into a summary');
    document.getElementById('clear-chat-btn')?.setAttribute('title', zh ? '清空聊天记录' : 'Clear chat history');
    const chatEmptyDesc = document.getElementById('chat-empty-desc');
    if (chatEmptyDesc) {
      chatEmptyDesc.textContent = zh
        ? '在侧栏上传或新建空白表格，然后对数据提问或下达操作指令。'
        : 'Upload or create a blank table in the sidebar, then ask questions about your data.';
    }
    // Lab credit
    const labCredit = document.getElementById('lab-credit');
    if (labCredit) labCredit.textContent = zh
      ? '中国科学技术大学认知智能全国重点实验室 AGI 组'
      : 'State Key Laboratory of Cognitive Intelligence, USTC · AGI Group';
    const guideHelpLbl = document.getElementById('guide-help-btn-label');
    if (guideHelpLbl) guideHelpLbl.textContent = zh ? '❓ 使用指南' : '❓ User guide';
    const featBtnLbl = document.getElementById('features-guide-btn-label');
    if (featBtnLbl) featBtnLbl.textContent = zh ? '📖 功能一览' : '📖 Features';
    document.getElementById('guide-help-btn')?.setAttribute('title', zh ? '了解 TabClaw 如何自我进化' : 'How TabClaw learns from feedback');
    document.getElementById('features-guide-btn')?.setAttribute('title', zh ? '界面里有哪些功能' : 'What each part of the UI does');
    const tipEvo = document.getElementById('chat-empty-tip-evolution-text');
    if (tipEvo) {
      tipEvo.textContent = zh
        ? 'TabClaw 越用越聪明 — 点这里了解自进化机制'
        : 'TabClaw learns as you use it — tap for how self-evolution works';
    }
    const tipFeat = document.getElementById('chat-empty-tip-features-text');
    if (tipFeat) {
      tipFeat.textContent = zh
        ? '界面功能说明：Compact、技能、记忆…点这里'
        : 'Feature overview: Compact, skills, memory… tap here';
    }
    const fgTitle = document.getElementById('features-guide-title');
    if (fgTitle) fgTitle.textContent = zh ? '功能一览' : 'Feature overview';
    const fgSub = document.getElementById('features-guide-subtitle');
    if (fgSub) fgSub.textContent = zh ? '各按钮与开关在做什么' : 'What each control does';
    const fgClose = document.getElementById('features-guide-close-btn');
    if (fgClose) fgClose.textContent = zh ? '知道了' : 'Got it';

    document.getElementById('lang-btn')?.setAttribute('title', zh ? '切换到英文界面' : 'Switch to Chinese');
    const demoLbl = document.getElementById('demo-btn-label');
    if (demoLbl) demoLbl.textContent = zh ? '一键体验' : 'Try a demo';
    document.getElementById('demo-btn')?.setAttribute('title', zh ? '一键体验示例场景' : 'Load sample data and suggested questions');
    const growthLbl = document.getElementById('growth-btn-label');
    if (growthLbl) growthLbl.textContent = zh ? '成长报告' : 'Growth report';
    document.getElementById('growth-btn')?.setAttribute('title', zh ? '查看 TabClaw 的成长轨迹' : 'View learning progress and milestones');
    const dsLbl = document.getElementById('discover-skill-btn-label');
    if (dsLbl) dsLbl.textContent = zh ? '🔍 从历史中发现技能' : '🔍 Discover skills from history';
    document.getElementById('discover-skill-btn')?.setAttribute('title', zh ? '从历史分析中发现重复模式并提炼为可复用技能' : 'Find recurring patterns in past chats and turn them into skills');
    const dcp = document.getElementById('demo-control-prefix');
    if (dcp) dcp.textContent = zh ? '演示进行中：' : 'Demo running:';
    const demoStop = document.getElementById('demo-stop-btn');
    if (demoStop) demoStop.textContent = zh ? '⏹ 停止演示' : '⏹ Stop demo';
    const dmt = document.getElementById('demo-modal-title');
    if (dmt) dmt.textContent = zh ? '🎯 一键体验' : '🎯 Try a demo';
    const dms = document.getElementById('demo-modal-subtitle');
    if (dms) dms.textContent = zh ? '选择一个场景，系统自动加载数据并逐步执行完整分析流程' : 'Pick a scenario — sample data loads and analyses run step by step';
    const gmht = document.getElementById('growth-modal-header-title');
    if (gmht) gmht.textContent = zh ? '📈 TabClaw 成长报告' : '📈 Growth report';
    const gmhss = document.getElementById('growth-modal-header-subtitle');
    if (gmhss) gmhss.textContent = zh ? '越用越聪明 — 查看 TabClaw 的学习轨迹与能力演进' : 'Learning trajectory and how TabClaw improves over time';
    const gmcb = document.getElementById('growth-modal-close-btn');
    if (gmcb) gmcb.textContent = zh ? '关闭' : 'Close';
    const msmt = document.getElementById('memory-summary-modal-title');
    if (msmt) msmt.textContent = zh ? '👤 用户偏好概览' : '👤 Preference overview';
    const msload = document.getElementById('memory-summary-loading-text');
    if (msload) msload.textContent = zh ? '正在整理中…' : 'Organizing…';
    const msclose = document.getElementById('memory-summary-close-btn');
    if (msclose) msclose.textContent = zh ? '关闭' : 'Close';
    const msrefresh = document.getElementById('memory-summary-refresh-btn');
    if (msrefresh) msrefresh.textContent = zh ? '↻ 重新生成' : '↻ Regenerate';
    const mscopy = document.getElementById('memory-summary-copy-btn');
    if (mscopy) mscopy.textContent = zh ? '📋 复制' : '📋 Copy';
    const gmh = document.getElementById('guide-modal-header-title');
    if (gmh) gmh.textContent = zh ? '🧬 TabClaw 如何越用越聪明' : '🧬 How TabClaw gets smarter over time';
    const gmhs = document.getElementById('guide-modal-header-subtitle');
    if (gmhs) gmhs.textContent = zh ? '了解自进化流程，让 TabClaw 成为你的专属数据分析师' : 'Self-evolution: feedback, skills, and growth';
    const gc = document.getElementById('guide-content');
    if (gc) gc.innerHTML = this._selfEvolutionGuideInnerHtml();
    const gdont = document.getElementById('guide-footer-dont-show-label');
    if (gdont) gdont.textContent = zh ? '下次不再自动显示' : "Don't show again on startup";
    const guideStart = document.getElementById('guide-start-btn');
    if (guideStart) guideStart.textContent = zh ? '开始使用' : 'Get started';
    const skillsHead = document.getElementById('skills-panel-header-title');
    if (skillsHead) skillsHead.textContent = zh ? '技能' : 'Skills';
    const memHead = document.getElementById('memory-panel-header-title');
    if (memHead) memHead.textContent = zh ? '记忆' : 'Memory';

    const isLight = document.documentElement.classList.contains('light');
    this._refreshThemeButtonTitle(isLight);

    const fgm = document.getElementById('features-guide-modal');
    if (fgm && !fgm.classList.contains('hidden')) {
      const fb = document.getElementById('features-guide-body');
      if (fb) fb.innerHTML = this._featuresGuideHtml();
    }
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

  async _createBlankTable() {
    const zh = this._lang === 'zh';
    this._notify(zh ? '正在创建空白表格…' : 'Creating blank table…', 'info');
    try {
      const data = await this._api('POST', '/api/tables/create', {
        name: zh ? '未命名表格' : 'Untitled',
        rows: 10,
        cols: 6,
      });
      this.state.tables = await this._api('GET', '/api/tables');
      this._renderTables();
      this._notify(zh ? '已创建，可在弹窗中编辑或粘贴' : 'Created — edit or paste in the viewer', 'success');
      this._hideChatEmpty();
      await this.showTableModal(data.table_id, 1);
    } catch (e) {
      this._notify(`${zh ? '创建失败' : 'Create failed'}: ${e.message}`, 'error');
    }
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
      const zh = this._lang === 'zh';
      list.innerHTML = zh
        ? '<div class="empty-state">暂无数据表。<br>可新建空白表格或上传 CSV / Excel。</div>'
        : '<div class="empty-state">No tables yet.<br>Create a blank table or upload CSV / Excel below.</div>';
      return;
    }
    list.innerHTML = this.state.tables.map(t => {
      const badge = t.source === 'computed' ? 'result' : (t.source === 'manual' ? 'blank' : 'csv');
      const badgeClass = t.source === 'computed' ? 'purple' : (t.source === 'manual' ? 'green' : '');
      return `
      <div class="table-item">
        <span class="table-item-icon">📊</span>
        <div class="table-item-info" onclick="app.showTableModal('${t.table_id}')">
          <div class="table-item-name">${this._esc(t.name)}</div>
          <div class="table-item-meta">${t.rows.toLocaleString()} rows × ${t.cols} cols</div>
        </div>
        <span class="table-item-badge ${badgeClass}">${badge}</span>
        <div class="table-item-actions">
          <button class="btn icon-only sm" title="View" onclick="app.showTableModal('${t.table_id}')">👁</button>
          <button class="btn icon-only sm danger" title="Delete" onclick="app._deleteTable('${t.table_id}')">🗑</button>
        </div>
      </div>
    `;
    }).join('');
    this._hideChatEmpty();
  }

  // -----------------------------------------------------------------------
  // Table modal
  // -----------------------------------------------------------------------

  async showTableModal(tableId, page = 1) {
    this.state.tableModal = { tableId, page, totalPages: 1, source: null };
    document.getElementById('table-modal').classList.remove('hidden');
    await this._loadTablePage(tableId, page);
  }

  hideTableModal() {
    document.getElementById('table-modal').classList.add('hidden');
  }

  async _loadTablePage(tableId, page) {
    const content = document.getElementById('table-modal-content');
    const zh = this._lang === 'zh';
    content.innerHTML = `<div style="padding:20px;text-align:center;color:var(--text-muted)">${zh ? '加载中…' : 'Loading…'}</div>`;
    try {
      const data = await this._api('GET', `/api/tables/${tableId}?page=${page}&page_size=50`);
      this.state.tableModal.totalPages = data.total_pages;
      this.state.tableModal.page = data.page;
      this.state.tableModal.source = data.source;
      document.getElementById('table-modal-title').textContent = data.name;
      document.getElementById('table-modal-meta').textContent =
        `${data.total_rows.toLocaleString()} rows × ${data.columns.length} columns`;
      const prevBtn = document.getElementById('table-modal-prev');
      const nextBtn = document.getElementById('table-modal-next');
      const pageSpan = document.getElementById('table-modal-page');
      const isManual = data.source === 'manual';
      if (isManual) {
        prevBtn.style.display = 'none';
        nextBtn.style.display = 'none';
        pageSpan.textContent = zh ? '可编辑' : 'Editable';
      } else {
        prevBtn.style.display = '';
        nextBtn.style.display = '';
        pageSpan.textContent = `Page ${data.page} / ${data.total_pages}`;
        prevBtn.disabled = data.page <= 1;
        nextBtn.disabled = data.page >= data.total_pages;
      }
      if (isManual) {
        content.innerHTML = this._buildEditableTable(data.columns, data.rows);
        this._bindManualTableEditor(tableId);
      } else {
        content.innerHTML = this._buildDataTable(data.columns, data.rows, data.total_rows);
      }
    } catch (e) {
      content.innerHTML = `<div style="padding:20px;color:var(--red)">${this._esc(e.message)}</div>`;
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

  _buildEditableTable(columns, rows) {
    const zh = this._lang === 'zh';
    const th = columns.map((c, i) =>
      `<th><input type="text" class="col-edit" data-ci="${i}" value="${this._esc(String(c))}" /></th>`
    ).join('');
    const bodyRows = (rows && rows.length)
      ? rows.map((row, ri) =>
        `<tr>${columns.map((c, ci) =>
          `<td><input type="text" class="cell-edit" data-ri="${ri}" data-ci="${ci}" value="${this._esc(String(row[c] ?? ''))}" /></td>`
        ).join('')}</tr>`
      ).join('')
      : '';
    return `
      <div class="table-editor-toolbar">
        <button type="button" class="btn sm primary" id="table-save-btn">${zh ? '保存' : 'Save'}</button>
        <button type="button" class="btn sm" id="table-paste-btn">${zh ? '从剪贴板粘贴' : 'Paste from clipboard'}</button>
        <span style="font-size:11px;color:var(--text-dim)">${zh ? '可直接编辑单元格；粘贴兼容 Excel / TSV / CSV' : 'Edit cells; paste from Excel, TSV, or CSV'}</span>
      </div>
      <div class="table-scroll">
        <table class="data-table editable-table"><thead><tr>${th}</tr></thead><tbody>${bodyRows}</tbody></table>
      </div>
    `;
  }

  _bindManualTableEditor(tableId) {
    const save = document.getElementById('table-save-btn');
    const paste = document.getElementById('table-paste-btn');
    if (save) save.onclick = () => this._saveManualTable(tableId);
    if (paste) paste.onclick = () => this._pasteManualTable(tableId);
  }

  async _saveManualTable(tableId) {
    const zh = this._lang === 'zh';
    const wrap = document.querySelector('#table-modal-content .editable-table');
    if (!wrap) return;
    const colInputs = [...wrap.querySelectorAll('thead input.col-edit')];
    const columns = colInputs.map(inp => inp.value);
    const cells = [...wrap.querySelectorAll('tbody input.cell-edit')];
    const colCount = columns.length;
    if (!colCount) {
      this._notify(zh ? '至少保留一列' : 'Need at least one column', 'error');
      return;
    }
    const rowCount = colCount ? Math.floor(cells.length / colCount) : 0;
    if (cells.length !== rowCount * colCount) {
      this._notify(zh ? '单元格数量与列数不一致' : 'Cell count does not match columns', 'error');
      return;
    }
    const data = [];
    for (let r = 0; r < rowCount; r++) {
      const row = [];
      for (let c = 0; c < colCount; c++) row.push(cells[r * colCount + c].value);
      data.push(row);
    }
    try {
      await this._api('PUT', `/api/tables/${tableId}`, { columns, data });
      this._notify(zh ? '已保存' : 'Saved', 'success');
      this.state.tables = await this._api('GET', '/api/tables');
      this._renderTables();
      await this._loadTablePage(tableId, 1);
    } catch (e) {
      this._notify(`${zh ? '保存失败' : 'Save failed'}: ${e.message}`, 'error');
    }
  }

  _parsePastedGrid(text) {
    const lines = text.trim().split(/\r?\n/).filter(l => l.length);
    if (!lines.length) return null;
    const splitLine = (line) => {
      if (line.includes('\t')) return line.split('\t').map(s => s.trim());
      return line.split(',').map(s => s.trim().replace(/^"|"$/g, ''));
    };
    const rows = lines.map(splitLine);
    const width = Math.max(...rows.map(r => r.length), 0);
    if (!width) return null;
    return rows.map(r => {
      const copy = [...r];
      while (copy.length < width) copy.push('');
      return copy;
    });
  }

  async _pasteManualTable(tableId) {
    const zh = this._lang === 'zh';
    let text;
    try {
      text = await navigator.clipboard.readText();
    } catch {
      this._notify(zh ? '无法读取剪贴板（需 HTTPS 或浏览器权限）' : 'Cannot read clipboard (needs HTTPS or permission)', 'error');
      return;
    }
    const grid = this._parsePastedGrid(text);
    if (!grid || !grid.length) {
      this._notify(zh ? '剪贴板为空或无法解析' : 'Clipboard is empty or could not be parsed', 'error');
      return;
    }
    let columns;
    let data;
    if (grid.length === 1) {
      columns = grid[0].map((_, i) => `Col${i + 1}`);
      data = [grid[0]];
    } else {
      columns = grid[0];
      data = grid.slice(1);
    }
    try {
      await this._api('PUT', `/api/tables/${tableId}`, { columns, data });
      this._notify(zh ? '已粘贴并保存' : 'Pasted and saved', 'success');
      this.state.tables = await this._api('GET', '/api/tables');
      this._renderTables();
      await this._loadTablePage(tableId, 1);
    } catch (e) {
      this._notify(`${zh ? '粘贴失败' : 'Paste failed'}: ${e.message}`, 'error');
    }
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
      const implicitFeedback = this.state.implicitFeedbackEnabled;
      const lastWorkflowId = this._lastCompletedWorkflowId;
      const body = executePlan
        ? { message, steps, code_tool: codeTool, skill_learn: skillLearn }
        : {
            message,
            code_tool: codeTool,
            skill_learn: skillLearn,
            implicit_feedback: implicitFeedback,
            last_workflow_id: lastWorkflowId,
          };

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
        // Track msgId so implicit feedback can update the UI
        this._workflowMsgMap[this.state.currentWorkflowId] = msgId;
        this._lastCompletedWorkflowId = this.state.currentWorkflowId;
      } else {
        this._lastCompletedWorkflowId = null;
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

      case 'implicit_feedback_applied':
        this._handleImplicitFeedbackApplied(event);
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
          <p id="chat-empty-desc">Upload or create a blank table in the sidebar, then ask questions about your data.</p>
          <div class="suggestion-chips">
            <div class="chip" onclick="app.insertSuggestion(this)">Summarize all uploaded tables</div>
            <div class="chip" onclick="app.insertSuggestion(this)">Find rows where value is null</div>
            <div class="chip" onclick="app.insertSuggestion(this)">Merge tables on a common column</div>
            <div class="chip" onclick="app.insertSuggestion(this)">Show top 10 rows sorted by first numeric column</div>
          </div>
          <div class="chat-empty-tip" id="chat-empty-tip-evolution" onclick="app.showGuideModal()">
            <span>🧬</span> <span id="chat-empty-tip-evolution-text">TabClaw learns as you use it — tap for how self-evolution works</span>
          </div>
          <div class="chat-empty-tip chat-empty-tip-secondary" id="chat-empty-tip-features" onclick="app.showFeaturesGuideModal()">
            <span>📖</span> <span id="chat-empty-tip-features-text">Feature overview: Compact, skills, memory… tap here</span>
          </div>
        </div>`;
      this._applyLangLabels();
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
      html += `<div class="empty-state skill-empty-guide">
        <div style="margin-bottom:8px">还没有学会任何技能</div>
        <div class="skill-empty-steps">
          <div>1️⃣ 用 TabClaw 分析几次表格</div>
          <div>2️⃣ 给出 👍👎 反馈</div>
          <div>3️⃣ 点下方 <b>「🔍 从历史中发现技能」</b></div>
        </div>
        <div style="margin-top:6px;font-size:11px;color:var(--text-dim)">或者开启底部 <b>Skill Learning</b> 让系统自动学习</div>
      </div>`;
    } else {
      html += packages.map(s => {
        const sourceLabel = s.source === 'distilled'
          ? '<span class="skill-badge-distilled" title="Auto-learned from session">🧠</span>'
          : s.source === 'discovered'
          ? '<span class="skill-badge-distilled" title="Discovered from patterns">🔍</span>'
          : '';
        const vLabel = s.version ? ` <span class="skill-version">v${this._esc(String(s.version))}</span>` : '';
        return `
        <div class="skill-item skill-item-clickable${s.enabled ? '' : ' skill-disabled'}" onclick="app.showPkgDetailModal('${this._esc(s.slug)}')">
          <span class="skill-dot package"></span>
          <div class="skill-info">
            <div class="skill-name">${sourceLabel}${this._esc(s.name)}${vLabel}</div>
            <div class="skill-desc">${this._esc(s.description)}</div>
          </div>
          <div class="skill-actions">
            <button class="btn icon-only sm" title="${s.enabled ? 'Disable' : 'Enable'}" onclick="event.stopPropagation();app._togglePackageSkill('${this._esc(s.slug)}', ${!s.enabled})">${s.enabled ? '⏸' : '▶'}</button>
            <button class="btn icon-only sm danger" onclick="event.stopPropagation();app._deletePackageSkill('${this._esc(s.slug)}')">🗑</button>
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

  async _discoverSkills() {
    const btn = document.getElementById('discover-skill-btn');
    btn.disabled = true;
    btn.textContent = '⏳';
    try {
      const data = await this._api('POST', '/api/skills/discover');
      if (!data.suggestions || !data.suggestions.length) {
        this._notify('暂未发现新的可提取模式。继续使用，TabClaw 会自动发现重复模式。', 'info');
        return;
      }
      this._showDiscoverResults(data.suggestions);
    } catch (e) {
      this._notify(`Discovery failed: ${e.message}`, 'error');
    } finally {
      btn.disabled = false;
      btn.textContent = '🔍';
    }
  }

  _showDiscoverResults(suggestions) {
    this._discoveredSuggestions = suggestions;
    const container = this._chatContainer();
    const el = document.createElement('div');
    el.className = 'discover-results';
    el.innerHTML = `
      <div class="discover-header">🔍 发现 ${suggestions.length} 个可提取的分析模式</div>
      ${suggestions.map((s, i) => `
        <div class="discover-item" id="discover-item-${i}">
          <div class="discover-item-name">${this._esc(s.name)}</div>
          <div class="discover-item-desc">${this._esc(s.description)}</div>
          <div class="discover-item-actions">
            <button class="btn sm primary" onclick="app._acceptDiscoveredSkill(${i})">✓ 采纳为技能</button>
            <button class="btn sm" onclick="document.getElementById('discover-item-${i}').style.display='none'">忽略</button>
          </div>
        </div>`).join('')}`;
    container.appendChild(el);
    this._scrollChat();
  }

  async _acceptDiscoveredSkill(index) {
    const skill = this._discoveredSuggestions && this._discoveredSuggestions[index];
    if (!skill) { this._notify('技能数据丢失，请重新发现', 'error'); return; }
    try {
      await this._api('POST', '/api/skills/accept', {
        name: skill.name,
        description: skill.description,
        body: skill.body,
      });
      const item = document.getElementById(`discover-item-${index}`);
      if (item) item.innerHTML = `<span class="feedback-done good">✓ 已创建技能「${this._esc(skill.name)}」</span>`;
      this._notify(`New skill created: ${skill.name}`, 'success');
      await this._loadSkills();
    } catch (e) {
      this._notify(e.message, 'error');
    }
  }

  async _improveSkill(slug) {
    this._notify('正在分析反馈并改进技能…', 'info');
    try {
      const result = await this._api('POST', `/api/skills/package/${slug}/improve`);
      if (result.status === 'upgraded') {
        this._notify(`技能已升级到 v${result.version}：${result.reason}`, 'success');
        await this._loadSkills();
      } else {
        this._notify('未发现可改进之处', 'info');
      }
    } catch (e) {
      this._notify(e.message, 'error');
    }
  }

  // -----------------------------------------------------------------------
  // Package skill detail modal
  // -----------------------------------------------------------------------

  async showPkgDetailModal(slug) {
    const modal = document.getElementById('pkg-detail-modal');
    const body = document.getElementById('pkg-detail-body');
    body.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted)">Loading…</div>';
    modal.classList.remove('hidden');

    try {
      const d = await this._api('GET', `/api/skills/package/${slug}/detail`);
      document.getElementById('pkg-detail-name').textContent = `${d.name}`;
      const sourceMap = {distilled: '🧠 自动学习', discovered: '🔍 模式发现', manual: '✏️ 手动创建'};
      document.getElementById('pkg-detail-sub').textContent =
        `v${d.version} · ${sourceMap[d.source] || d.source} · ${d.enabled ? '已启用' : '已禁用'}`;

      document.getElementById('pkg-detail-improve-btn').onclick = () => {
        this.hidePkgDetailModal();
        this._improveSkill(slug);
      };

      const totalFb = d.success_count + d.failure_count;
      const rate = totalFb ? Math.round(d.success_count / totalFb * 100) + '%' : '—';

      let historyHtml = '';
      if (d.upgrade_history && d.upgrade_history.length) {
        historyHtml = `<div class="pkg-detail-section">
          <div class="pkg-detail-section-title">版本历史</div>
          ${d.upgrade_history.map(h => `
            <div class="pkg-detail-history-item">
              <span class="pkg-detail-ver">v${h.from_version} → v${h.to_version}</span>
              <span class="pkg-detail-reason">${this._esc(h.reason)}</span>
              <span class="pkg-detail-date">${(h.timestamp || '').slice(0, 10)}</span>
            </div>`).join('')}
        </div>`;
      }

      body.innerHTML = `
        <div class="pkg-detail-stats-row">
          <div class="pkg-detail-stat"><span class="pkg-detail-stat-val">${d.usage_count}</span><span class="pkg-detail-stat-lbl">使用次数</span></div>
          <div class="pkg-detail-stat"><span class="pkg-detail-stat-val">${d.success_count}</span><span class="pkg-detail-stat-lbl">👍</span></div>
          <div class="pkg-detail-stat"><span class="pkg-detail-stat-val">${d.failure_count}</span><span class="pkg-detail-stat-lbl">👎</span></div>
          <div class="pkg-detail-stat"><span class="pkg-detail-stat-val">${rate}</span><span class="pkg-detail-stat-lbl">好评率</span></div>
        </div>
        <div class="pkg-detail-section">
          <div class="pkg-detail-section-title">技能描述</div>
          <div class="pkg-detail-desc">${this._esc(d.description)}</div>
        </div>
        <div class="pkg-detail-section">
          <div class="pkg-detail-section-title">SKILL.md 指令内容</div>
          <div class="pkg-detail-body-content">${this._renderMarkdown(d.body)}</div>
        </div>
        ${historyHtml}
        <div class="pkg-detail-meta-row">
          ${d.created_at ? `<span>创建于 ${d.created_at.slice(0, 10)}</span>` : ''}
          ${d.last_used_at ? `<span>最后使用 ${d.last_used_at.slice(0, 10)}</span>` : ''}
          ${d.derived_from_workflow ? `<span>来源会话 ${d.derived_from_workflow}</span>` : ''}
        </div>`;
    } catch (e) {
      body.innerHTML = `<div style="padding:20px;color:var(--red)">${this._esc(e.message)}</div>`;
    }
  }

  hidePkgDetailModal() {
    document.getElementById('pkg-detail-modal').classList.add('hidden');
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

    const zh = this._lang === 'zh';
    // Open modal and show loading state
    content.innerHTML = '';
    loading.classList.remove('hidden');
    subtitle.textContent = zh ? '由 AI 根据当前记忆整理生成' : 'Generated from your saved memory';
    modal.classList.remove('hidden');

    const doGenerate = async () => {
      content.innerHTML = '';
      loading.classList.remove('hidden');
      try {
        const result = await this._api('POST', '/api/memory/summarize');
        this._summaryText = result.summary || '';
        content.innerHTML = this._renderMarkdown(this._summaryText);
        const now = new Date().toLocaleTimeString();
        subtitle.textContent = zh ? `生成于 ${now}` : `Generated at ${now}`;
      } catch (e) {
        content.innerHTML = `<span style="color:var(--red)">${zh ? '生成失败' : 'Failed'}: ${this._esc(e.message)}</span>`;
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
    const zh = this._lang === 'zh';
    const grid = document.getElementById('demo-scenarios-grid');
    grid.innerHTML = DEMO_SCENARIOS.map(s => `
      <div class="demo-card">
        <div class="demo-card-header">
          <span class="demo-card-icon">${s.icon}</span>
          <div>
            <div class="demo-card-title">${this._esc(s.title)}</div>
            <div class="demo-card-steps-count">${s.queries.length} ${zh ? '个分析步骤' : 'analysis steps'}</div>
          </div>
        </div>
        <div class="demo-card-desc">${this._esc(s.description)}</div>
        <div class="demo-card-files">
          ${s.files.map(f => `<span class="badge blue">${this._esc(f)}</span>`).join('')}
        </div>
        <ul class="demo-card-steps-list">
          ${s.queries.slice(0, 2).map(q => `<li>${this._esc(q.slice(0, 58))}${q.length > 58 ? '…' : ''}</li>`).join('')}
          ${s.queries.length > 2 ? `<li class="more-steps">+ ${s.queries.length - 2} ${zh ? '更多步骤…' : 'more steps…'}</li>` : ''}
        </ul>
        <button class="btn primary" style="width:100%;justify-content:center;margin-top:4px"
                onclick="app.runDemo('${s.id}')">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" style="margin-right:4px">
            <polygon points="5 3 19 12 5 21 5 3"/>
          </svg>
          ${zh ? '开始演示' : 'Start demo'}
        </button>
      </div>`).join('');
  }

  async runDemo(scenarioId) {
    const scenario = DEMO_SCENARIOS.find(s => s.id === scenarioId);
    if (!scenario) return;
    const zh = this._lang === 'zh';

    this.hideDemoModal();
    this.state.demoRunning = true;

    // Show demo control bar
    document.getElementById('demo-control-title').textContent = scenario.title;
    document.getElementById('demo-control-step').textContent = '';
    document.getElementById('demo-control').classList.remove('hidden');

    // 1. Load example files
    this._appendSystemMessage(`⏳ ${zh ? '正在加载示例数据：' : 'Loading sample data: '}${scenario.files.join(zh ? '、' : ', ')}…`);
    try {
      const result = await this._api('POST', '/api/demo/load', {
        files: scenario.files,
        clear: true,
      });
      await this._loadTables();
      const names = result.loaded.map(t =>
        zh ? `${t.name}（${t.rows} 行 × ${t.cols} 列）` : `${t.name} (${t.rows} × ${t.cols})`
      ).join(zh ? '，' : ', ');
      this._appendSystemMessage(`✓ ${zh ? '数据加载完成：' : 'Data loaded: '}${names}`);
    } catch (e) {
      this._appendSystemMessage(`✗ ${zh ? '数据加载失败：' : 'Load failed: '}${e.message}`);
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
        `· ${zh ? '步骤' : 'Step'} ${i + 1} / ${scenario.queries.length}`;

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
        zh
          ? `🎉 演示完成！「${scenario.title}」共执行 ${scenario.queries.length} 个分析步骤。`
          : `🎉 Demo finished: "${scenario.title}" — ${scenario.queries.length} analysis steps.`
      );
    } else {
      this._appendSystemMessage(zh ? '⏹ 演示已停止。' : '⏹ Demo stopped.');
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
    const zh = this._lang === 'zh';
    const el = document.createElement('div');
    el.className = 'demo-step-divider';
    const shortQ = queryText.length > 50 ? queryText.slice(0, 50) + '…' : queryText;
    el.innerHTML = `
      <div class="demo-step-line"></div>
      <span class="demo-step-label">${zh ? '步骤' : 'Step'} ${stepNum} / ${total}</span>
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
      <span class="feedback-hint">这次分析怎么样？你的反馈是 TabClaw 进化的燃料</span>
      <button class="feedback-btn good" onclick="app.sendFeedback('${sessionId}', 'good', '${msgId}')" title="分析准确、有帮助">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 9V5a3 3 0 00-3-3l-4 9v11h11.28a2 2 0 002-1.7l1.38-9a2 2 0 00-2-2.3H14z"/><path d="M7 22H4a2 2 0 01-2-2v-7a2 2 0 012-2h3"/></svg>
      </button>
      <button class="feedback-btn bad" onclick="app.sendFeedback('${sessionId}', 'bad', '${msgId}')" title="结果不准确，TabClaw 将从中学习改进">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 15v4a3 3 0 003 3l4-9V2H5.72a2 2 0 00-2 1.7l-1.38 9a2 2 0 002 2.3H10z"/><path d="M17 2h3a2 2 0 012 2v7a2 2 0 01-2 2h-3"/></svg>
      </button>`;
    body.appendChild(div);
  }

  async sendFeedback(sessionId, feedback, msgId) {
    const container = document.getElementById(`feedback-${msgId}`);
    if (!container) return;
    try {
      const result = await this._api('POST', `/api/workflow/${sessionId}/feedback`, { feedback });
      container.innerHTML = feedback === 'good'
        ? '<span class="feedback-done good">👍 感谢反馈</span>'
        : '<span class="feedback-done bad">👎 已记录，将用于改进</span>';
      this._notify(feedback === 'good' ? '感谢正面反馈！' : '已记录，TabClaw 将从中学习', 'success');
      if (result.skill_upgraded) {
        const u = result.skill_upgraded;
        this._notify(`技能「${u.name}」已根据反馈自动升级到 v${u.version}`, 'success');
        await this._loadSkills();
      }
    } catch (e) {
      this._notify('反馈提交失败', 'error');
    }
  }

  _handleImplicitFeedbackApplied(event) {
    const { session_id, feedback, already_rated } = event;
    if (already_rated) return; // Already manually rated, don't override UI

    const msgId = this._workflowMsgMap[session_id];
    if (!msgId) return;

    const container = document.getElementById(`feedback-${msgId}`);
    if (!container) return;

    if (feedback === 'good') {
      container.innerHTML =
        '<span class="feedback-done good implicit">👍 隐式反馈（正向）— 已自动记录</span>';
    } else if (feedback === 'bad') {
      container.innerHTML =
        '<span class="feedback-done bad implicit">👎 隐式反馈（负向）— 已自动记录</span>';
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

    const zh0 = this._lang === 'zh';
    content.innerHTML = `<div style="padding:20px;text-align:center;color:var(--text-muted)">${zh0 ? '加载中…' : 'Loading…'}</div>`;
    modal.classList.remove('hidden');

    try {
      const zh = this._lang === 'zh';
      const p = await this._api('GET', '/api/growth/profile');
      const rate = p.satisfaction_rate !== null ? `${Math.round(p.satisfaction_rate * 100)}%` : '—';
      const skillStats = (p.skills_stats || []);
      const topTools = Object.entries(p.tool_frequency || {}).slice(0, 8);
      const domains = p.domains || [];
      const eff = p.efficiency || {};
      const milestones = p.milestones || [];

      // Milestones
      let msHtml = '';
      if (milestones.length) {
        msHtml = `<div class="growth-milestones">${milestones.map(m =>
          `<span class="growth-ms ${m.reached ? 'reached' : 'pending'}">${m.label}</span>`
        ).join('')}</div>`;
      }

      // Domain proficiency bars
      let domainHtml = '';
      if (domains.length) {
        domainHtml = `<div class="growth-section">
          <div class="growth-section-title">
            ${zh ? '领域熟练度' : 'Domain proficiency'}
            <button class="btn sm" style="margin-left:auto;font-size:10px" onclick="app._showAddDomainForm()">${zh ? '+ 自定义领域' : '+ Custom domain'}</button>
          </div>
          ${domains.map(d => {
            const pct = Math.round(d.proficiency * 100);
            return `<div class="growth-domain-row">
              <span class="growth-domain-name">${this._esc(d.name)}</span>
              <div class="growth-domain-bar"><div class="growth-domain-fill" style="width:${pct}%"></div></div>
              <span class="growth-domain-meta">${zh ? `${d.sessions} 次 · ${pct}%` : `${d.sessions} sessions · ${pct}%`}</span>
            </div>`;
          }).join('')}
          <div id="add-domain-form" class="add-domain-form hidden">
            <input type="text" id="add-domain-name" class="form-input sm" placeholder="${zh ? '领域名称，如：医疗健康' : 'Domain name, e.g. Healthcare'}" />
            <input type="text" id="add-domain-keywords" class="form-input sm" placeholder="${zh ? '关键词（逗号分隔），如：患者,医疗,诊断' : 'Keywords (comma-separated), e.g. patient,diagnosis'}" />
            <div style="display:flex;gap:6px;justify-content:flex-end">
              <button class="btn sm" onclick="document.getElementById('add-domain-form').classList.add('hidden')">${zh ? '取消' : 'Cancel'}</button>
              <button class="btn sm primary" onclick="app._submitCustomDomain()">${zh ? '添加' : 'Add'}</button>
            </div>
          </div>
        </div>`;
      }

      // Efficiency comparison
      let effHtml = '';
      if (eff.early_avg_steps) {
        const dPct = eff.duration_change_pct;
        const sPct = eff.steps_change_pct;
        const dColor = dPct <= 0 ? 'var(--green)' : 'var(--red)';
        const sColor = sPct <= 0 ? 'var(--green)' : 'var(--red)';
        const dLabel = zh
          ? (dPct <= 0 ? `快了 ${Math.abs(dPct)}%` : `慢了 ${dPct}%`)
          : (dPct <= 0 ? `${Math.abs(dPct)}% faster` : `${dPct}% slower`);
        const sLabel = zh
          ? (sPct <= 0 ? `少了 ${Math.abs(sPct)}%` : `多了 ${sPct}%`)
          : (sPct <= 0 ? `${Math.abs(sPct)}% fewer` : `${sPct}% more`);
        effHtml = `<div class="growth-section">
          <div class="growth-section-title">${zh ? '效率变化' : 'Efficiency'} <span style="font-weight:400;color:var(--text-dim);font-size:11px">${zh ? '（前半 vs 近期）' : '(early vs recent)'}</span></div>
          <div class="growth-eff-grid">
            <div class="growth-eff-card">
              <div class="growth-eff-label">${zh ? '平均耗时' : 'Avg. duration'}</div>
              <div class="growth-eff-vals">
                <span class="growth-eff-old">${(eff.early_avg_duration_ms / 1000).toFixed(1)}s</span>
                <span class="growth-eff-arrow">→</span>
                <span class="growth-eff-new">${(eff.recent_avg_duration_ms / 1000).toFixed(1)}s</span>
              </div>
              <div class="growth-eff-delta" style="color:${dColor}">${dLabel}</div>
            </div>
            <div class="growth-eff-card">
              <div class="growth-eff-label">${zh ? '平均步数' : 'Avg. steps'}</div>
              <div class="growth-eff-vals">
                <span class="growth-eff-old">${eff.early_avg_steps}</span>
                <span class="growth-eff-arrow">→</span>
                <span class="growth-eff-new">${eff.recent_avg_steps}</span>
              </div>
              <div class="growth-eff-delta" style="color:${sColor}">${sLabel}</div>
            </div>
          </div>
        </div>`;
      }

      // Skills
      let skillsHtml = '';
      if (skillStats.length) {
        skillsHtml = `<div class="growth-section">
          <div class="growth-section-title">${zh ? '已学技能' : 'Learned skills'}</div>
          <div class="growth-skills-list">${skillStats.map(s => `
            <div class="growth-skill-item" style="cursor:pointer" onclick="app.hideGrowthModal();app.showPkgDetailModal('${this._esc(s.slug)}')">
              <span class="growth-skill-name">${this._esc(s.name)}</span>
              <span class="growth-skill-meta">v${s.version} · ${zh ? `使用 ${s.usage_count} 次` : `used ${s.usage_count}×`} · 👍${s.success_count} 👎${s.failure_count}</span>
            </div>`).join('')}
          </div>
        </div>`;
      }

      // Tools
      let toolsHtml = '';
      if (topTools.length) {
        const maxCount = topTools[0]?.[1] || 1;
        toolsHtml = `<div class="growth-section">
          <div class="growth-section-title">${zh ? '工具使用频率' : 'Tool usage'}</div>
          <div class="growth-tools-list">${topTools.map(([name, count]) => `
            <div class="growth-tool-bar-row">
              <span class="growth-tool-name">${this._esc(name)}</span>
              <div class="growth-tool-bar"><div class="growth-tool-bar-fill" style="width:${Math.round(count / maxCount * 100)}%"></div></div>
              <span class="growth-tool-count">${count}</span>
            </div>`).join('')}
          </div>
        </div>`;
      }

      // Timeline
      let eventsHtml = '';
      if (p.recent_events && p.recent_events.length) {
        eventsHtml = `<div class="growth-section">
          <div class="growth-section-title">${zh ? '成长时间线' : 'Timeline'}</div>
          <div class="growth-timeline">${p.recent_events.map(e => `
            <div class="growth-event">
              <span class="growth-event-date">${this._esc(e.date)}</span>
              <span class="growth-event-text">${this._esc(e.event)}</span>
            </div>`).join('')}
          </div>
        </div>`;
      }

      const isEmpty = !skillStats.length && !topTools.length && !domains.length;

      content.innerHTML = `
        <div class="growth-stats-grid">
          <div class="growth-stat-card">
            <div class="growth-stat-value">${p.total_sessions}</div>
            <div class="growth-stat-label">${zh ? '分析会话' : 'Sessions'}</div>
          </div>
          <div class="growth-stat-card">
            <div class="growth-stat-value">${p.skills_learned}</div>
            <div class="growth-stat-label">${zh ? '已学技能' : 'Skills learned'}</div>
          </div>
          <div class="growth-stat-card">
            <div class="growth-stat-value">${p.skill_reuse_count || 0}</div>
            <div class="growth-stat-label">${zh ? '技能复用' : 'Skill reuse'}</div>
          </div>
          <div class="growth-stat-card">
            <div class="growth-stat-value">${rate}</div>
            <div class="growth-stat-label">${zh ? '满意率' : 'Satisfaction'}</div>
          </div>
        </div>
        ${msHtml}
        ${domainHtml}
        ${effHtml}
        ${skillsHtml}
        ${toolsHtml}
        ${eventsHtml}
        ${isEmpty
          ? `<div class="growth-empty-state">
              <div class="growth-empty-title">${zh ? '还没有成长数据' : 'No growth data yet'}</div>
              <div class="growth-empty-flow">
                <div class="growth-empty-step">${zh ? '📊 上传表格提问' : '📊 Upload & ask'}</div>
                <div class="growth-empty-arrow">→</div>
                <div class="growth-empty-step">${zh ? '👍👎 给出反馈' : '👍👎 Feedback'}</div>
                <div class="growth-empty-arrow">→</div>
                <div class="growth-empty-step">${zh ? '🧠 自动学习技能' : '🧠 Skills learn'}</div>
                <div class="growth-empty-arrow">→</div>
                <div class="growth-empty-step">${zh ? '📈 越来越准' : '📈 Gets better'}</div>
              </div>
              <div class="growth-empty-hint">${zh ? '开始使用 TabClaw 后，这里会展示领域熟练度、效率变化和学习里程碑。' : 'After you use TabClaw, domain proficiency, efficiency, and milestones appear here.'}</div>
            </div>`
          : ''}`;
    } catch (e) {
      content.innerHTML = `<div style="padding:20px;color:var(--red)">${this._esc(e.message)}</div>`;
    }
  }

  hideGrowthModal() {
    const modal = document.getElementById('growth-modal');
    if (modal) modal.classList.add('hidden');
  }

  _showAddDomainForm() {
    const form = document.getElementById('add-domain-form');
    if (form) form.classList.remove('hidden');
  }

  async _submitCustomDomain() {
    const zh = this._lang === 'zh';
    const nameEl = document.getElementById('add-domain-name');
    const kwEl = document.getElementById('add-domain-keywords');
    const name = (nameEl?.value || '').trim();
    const kwStr = (kwEl?.value || '').trim();
    if (!name || !kwStr) { this._notify(zh ? '请填写领域名称和关键词' : 'Enter domain name and keywords', 'error'); return; }
    const keywords = kwStr.split(/[,，、\s]+/).map(s => s.trim().toLowerCase()).filter(Boolean);
    if (keywords.length === 0) { this._notify(zh ? '请至少填写一个关键词' : 'Enter at least one keyword', 'error'); return; }
    try {
      await this._api('POST', '/api/growth/domains', { name, keywords });
      this._notify(zh ? `领域「${name}」已添加，新的会话将自动归类` : `Domain "${name}" added — new sessions will be classified`, 'success');
      this.hideGrowthModal();
      setTimeout(() => this.showGrowthDashboard(), 300);
    } catch (e) {
      this._notify(e.message, 'error');
    }
  }

  // -----------------------------------------------------------------------
  // Guide modal & feature overview
  // -----------------------------------------------------------------------

  _featuresGuideHtml() {
    const zh = this._lang === 'zh';
    if (zh) {
      return `
<div class="features-guide-section"><h4>顶部栏</h4><ul>
<li><strong>主题</strong>：切换浅色 / 深色。</li>
<li><strong>语言</strong>：切换中英文界面文案。</li>
<li><strong>一键体验</strong>：加载示例 CSV 与推荐问题，快速走通分析流程。</li>
<li><strong>Compact（压缩）</strong>：把当前长对话<strong>合并成一条摘要</strong>，节省上下文；<strong>不会删除</strong>侧栏里的数据表。</li>
<li><strong>Clear Chat（清空对话）</strong>：只清空聊天记录，已上传或新建的表仍在。</li>
</ul></div>
<div class="features-guide-section"><h4>输入区上方开关</h4><ul>
<li><strong>规划模式</strong>：先出可编辑的执行计划，确认后再跑，适合复杂任务。</li>
<li><strong>代码工具</strong>：允许在沙箱里执行 Python，处理合并、透视等重操作。</li>
<li><strong>技能学习</strong>：任务完成后自动尝试把做法提炼成可复用技能（与「使用指南」里的自进化配合）。</li>
</ul></div>
<div class="features-guide-section"><h4>数据表</h4><ul>
<li><strong>上传</strong>：支持 CSV / Excel，可多文件。</li>
<li><strong>新建空白表格</strong>：无文件时也可建表，在弹窗里编辑单元格或从 Excel 复制后粘贴。</li>
<li>点击表名可预览；可下载 CSV；手工表点<strong>保存</strong>写入服务端。</li>
</ul></div>
<div class="features-guide-section"><h4>技能（Skills）</h4><ul>
<li><strong>+ Add / Import</strong>：手写技能或导入 zip 包。</li>
<li><strong>从历史中发现技能</strong>：扫描对话，把重复分析模式收成技能。</li>
<li><strong>Clear</strong>：清空已安装的包技能（谨慎）。</li>
</ul></div>
<div class="features-guide-section"><h4>记忆（Memory）</h4><ul>
<li><strong>Overview</strong>：生成偏好与上下文的文字总览。</li>
<li><strong>+ Add</strong>：手动写入长期记忆。</li>
<li><strong>Forget</strong>：按描述删除相关记忆条目。</li>
<li><strong>Clear</strong>：清空全部记忆。</li>
</ul></div>
<div class="features-guide-section"><h4>侧栏底部</h4><ul>
<li><strong>成长报告</strong>：查看领域熟练度、效率与里程碑（自进化相关）。</li>
<li><strong>使用指南</strong>：专门讲「越用越聪明」、反馈与技能进化流程。</li>
<li><strong>功能一览</strong>：即本窗口，梳理界面功能。</li>
</ul></div>`;
    }
    return `
<div class="features-guide-section"><h4>Top bar</h4><ul>
<li><strong>Theme</strong> — switch light / dark.</li>
<li><strong>Language</strong> — switch English / Chinese labels.</li>
<li><strong>Demo</strong> — load sample CSVs and suggested questions to try the flow quickly.</li>
<li><strong>Compact</strong> — merge the current long chat into <strong>one summary message</strong> to save context. Your <strong>tables in the sidebar are not removed</strong>.</li>
<li><strong>Clear Chat</strong> — clears chat history only; uploaded or created tables stay.</li>
</ul></div>
<div class="features-guide-section"><h4>Toolbar above the input</h4><ul>
<li><strong>Plan mode</strong> — generate a reviewable plan before execution.</li>
<li><strong>Code tool</strong> — allow sandboxed Python for heavy joins, pivots, etc.</li>
<li><strong>Skill learning</strong> — after tasks, optionally distil reusable skills (works with the self-evolution guide).</li>
</ul></div>
<div class="features-guide-section"><h4>Tables</h4><ul>
<li><strong>Upload</strong> — CSV / Excel, multiple files OK.</li>
<li><strong>New blank table</strong> — create a sheet without a file; edit cells or paste from Excel in the viewer.</li>
<li>Click a table to preview; download CSV; for manual tables use <strong>Save</strong> to persist edits.</li>
</ul></div>
<div class="features-guide-section"><h4>Skills</h4><ul>
<li><strong>+ Add / Import</strong> — add a skill or import a .zip package.</li>
<li><strong>Discover from history</strong> — mine recurring patterns from past chats.</li>
<li><strong>Clear</strong> — remove all package skills (use with care).</li>
</ul></div>
<div class="features-guide-section"><h4>Memory</h4><ul>
<li><strong>Overview</strong> — generate a text summary of stored preferences/context.</li>
<li><strong>+ Add</strong> — add a long-term memory note.</li>
<li><strong>Forget</strong> — remove memories matching a short description.</li>
<li><strong>Clear</strong> — wipe all memory.</li>
</ul></div>
<div class="features-guide-section"><h4>Sidebar footer</h4><ul>
<li><strong>Growth report</strong> — domain stats, efficiency, milestones (self-evolution).</li>
<li><strong>User guide</strong> — how TabClaw learns from feedback (separate from this list).</li>
<li><strong>Features</strong> — this overview of UI capabilities.</li>
</ul></div>`;
  }

  showFeaturesGuideModal() {
    const modal = document.getElementById('features-guide-modal');
    const zh = this._lang === 'zh';
    const title = document.getElementById('features-guide-title');
    if (title) title.textContent = zh ? '功能一览' : 'Feature overview';
    const sub = document.getElementById('features-guide-subtitle');
    if (sub) sub.textContent = zh ? '各按钮与开关在做什么' : 'What each control does';
    const closeBtn = document.getElementById('features-guide-close-btn');
    if (closeBtn) closeBtn.textContent = zh ? '知道了' : 'Got it';
    const body = document.getElementById('features-guide-body');
    if (body) body.innerHTML = this._featuresGuideHtml();
    if (modal) modal.classList.remove('hidden');
  }

  hideFeaturesGuideModal() {
    const modal = document.getElementById('features-guide-modal');
    if (modal) modal.classList.add('hidden');
  }

  showGuideModal() {
    const modal = document.getElementById('guide-modal');
    if (modal) modal.classList.remove('hidden');
  }

  hideGuideModal() {
    const modal = document.getElementById('guide-modal');
    if (modal) modal.classList.add('hidden');
    const chk = document.getElementById('guide-dont-show');
    if (chk && chk.checked) {
      localStorage.setItem('tabclaw_guide_dismissed', '1');
    }
  }
}

// Start the app
const app = new TabClawApp();
