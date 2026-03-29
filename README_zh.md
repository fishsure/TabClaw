<h1 align="center"> TabClaw：交互式表格分析 AI 智能体 </h1>

<p align="center">
  <a href="README.md">English</a> · <strong>中文</strong>
</p>

<p align="center">
  <a href="https://agentr1.github.io/tab-claw/"><img src="https://img.shields.io/badge/Project-Home-orange.svg" alt="项目主页"></a>
  <a href="https://github.com/fishsure/TabClaw/stargazers"><img src="https://img.shields.io/github/stars/fishsure/TabClaw" alt="GitHub Stars"></a>
  <a href="https://github.com/fishsure/TabClaw/network/members"><img src="https://img.shields.io/github/forks/fishsure/TabClaw" alt="GitHub Forks"></a>
  <a href="https://agentr1.github.io/tab-claw/"><img src="https://img.shields.io/badge/docs-latest-blue.svg" alt="文档"></a>
</p>

<img src="asset/logo_rmbg.png" alt="TabClaw" width="100%" />



> **千人千面 · 越用越强 · 全程交互**



拖入一张 CSV 或 Excel，用自然语言说出你想要什么——TabClaw 会先展示执行计划，再并行调度多个智能体处理你的表格，跨会话记住你的偏好，并从每次交互中提炼可复用技能。用得越多，它就越懂你。

---

## 架构

<img src="asset/TabClaw_framework.png" alt="TabClaw 架构" width="100%" />

---

## 🗞️ 动态

- **[2026-03-19]** TabClaw 正式开源！代码与文档已在 GitHub 公开。

---

## TabClaw 的独特之处

### 🙋 拿不准就先问
当你的请求存在多种合理解读时，TabClaw 会暂停并列出简明的澄清选项，由你选择后再继续——不会在沉默中猜错方向。

<p align="center"><img src="asset/clarify.png" alt="意图澄清" width="75%" /></p>

### 🗺️ 先规划，再动手
在操作数据之前，TabClaw 会生成分步执行计划并展示给你。你可以调整顺序、改写或新增步骤，确认后再执行。任务完成后还会自动复查，确保没有遗漏。

<p align="center"><img src="asset/plan.png" alt="计划模式" width="75%" /></p>

### 🤖 多智能体并行分析
上传多张表并提出对比类问题时，TabClaw 会为每张表分配独立的分析智能体并行处理，最后由汇总器整合结论——一致之处标记为 **[CONSENSUS]**，存在分歧则标记为 **[UNCERTAIN]**。

<p align="center"><img src="asset/para.png" alt="多智能体并行分析" width="75%" /></p>

### 🧠 越用越聪明
每完成一项有一定复杂度的任务，TabClaw 都会回顾过程、总结规律，并将其提炼为可复用的**自定义技能**。下次遇到类似问题时可直接调用，省去重复摸索。

<p align="center"><img src="asset/skill.png" alt="技能学习" width="75%" /></p>

### 💾 记住你的习惯
TabClaw 会捕捉你的工作偏好——常用指标、输出格式、领域术语——并将其沉淀为持久记忆，自动带入后续对话。你也可以随时在侧栏查看、编辑或清空。

<p align="center"><img src="asset/Memory.png" alt="持久记忆" width="75%" /></p>

### 🛠️ 支持自定义技能扩展
你可以编写提示词模板或 Python 代码来定义自己的技能，智能体会像调用内置技能一样使用它们。配合自动技能学习，TabClaw 会逐步积累一套专属于你工作流的技能库。

<p align="center"><img src="asset/extend.png" alt="自定义技能" width="75%" /></p>

### 🗜️ 长对话自动压缩
当对话逐渐变长，TabClaw 会在发送新请求前自动将历史内容浓缩为精炼摘要，让智能体始终保持专注而不丢失关键上下文。你也可以随时点击 **Compact** 按钮手动触发。

<p align="center"><img src="asset/campact.png" alt="对话压缩" width="75%" /></p>

---

## 快速开始

```bash
git clone https://github.com/fishsure/TabClaw.git
cd TabClaw

cp setting.txt.example setting.txt
# 在 setting.txt 中填写 API_KEY 与 BASE_URL

pip install -r requirements.txt
bash run.sh
```

浏览器打开 **http://localhost:8000**，点击 **一键体验** 即可进入引导式演示场景。

<p align="center"><img src="asset/try.png" alt="演示场景" width="75%" /></p>

---

## 演示

**说明：** GitHub 仓库主页的 README **不会显示内嵌视频播放器**。请**点击下方图片或链接**，在文件页用内置播放器观看 MP4。

<p align="center">
  <a href="asset/TabClaw.mp4">
    <img src="asset/TabClaw_demo.png" alt="演示视频 — 点击播放" width="85%" />
  </a>
</p>

<p align="center"><a href="asset/TabClaw.mp4"><strong>▶ 演示视频（MP4）</strong></a></p>

<p align="center"><img src="asset/dark_demo.png" alt="TabClaw 界面（深色）" width="85%" /></p>

<p align="center"><img src="asset/infer.png" alt="智能体推理过程" width="85%" /></p>

---

## 文档

| | |
|---|---|
| [✨ 功能说明](docs/features.md) | 完整功能细节 |
| [⚙️ 配置](docs/configuration.md) | API 提供商、模型选择 |
| [🏗️ 架构](docs/architecture.md) | 系统设计、项目结构 |
| [🛠️ 技能参考](docs/skills.md) | 内置技能、自定义技能、沙箱 |

---

## 相关项目

### 🦞 [Claw-R1](https://agentr1.github.io/Claw-R1/) — 面向通用智能体的强化学习训练框架

来自同一团队。**Claw-R1** 在 Agentic RL 与新一代通用智能体（TabClaw、OpenClaw、Claude Code 等）之间架起桥梁。它引入**中间件层**作为智能体侧与训练侧之间的唯一通道，让白盒和黑盒智能体都能通过标准 HTTP 接入 RL 训练——这一范式在现有框架中尚属首创。

→ [项目页](https://agentr1.github.io/) · [文档](https://agentr1.github.io/Claw-R1/)

### 🧠 [TableMind++](https://arxiv.org/abs/2603.07528) — 不确定性感知的表格推理智能体

来自同一团队。**TableMind++** 采用两阶段训练（SFT 预热 + RAPO 强化微调）结合动态不确定性感知推理框架，专注解决多轮表格推理中的幻觉问题。推理阶段引入三种机制——记忆引导计划剪枝、基于置信度的动作细化、双权重轨迹聚合——协同压制认知与随机两类不确定性，在 WikiTQ、TabMWP、TabFact、HiTab、FinQA 等基准上均取得领先结果。

→ [论文](https://arxiv.org/abs/2603.07528) · [模型](https://huggingface.co/Jclennon/TableMind) · [数据集](https://huggingface.co/datasets/Jclennon/TableMind-data)

---
## 贡献者

**团队成员**：[Shuo Yu](https://fishsure.github.io/)、[Daoyu Wang](https://melmaphother.github.io/)、Qingchuan Li、Xiaoyu Tao、Qingyang Mao、Yitong Zhou

**指导老师**：[Mingyue Cheng](https://mingyue-cheng.github.io/)、[Qi Liu](http://staff.ustc.edu.cn/~qiliuql/)、Enhong Chen

**单位**：中国科学技术大学认知智能全国重点实验室

**联系方式**：Shuo Yu — [yu12345@mail.ustc.edu.cn](mailto:yu12345@mail.ustc.edu.cn) · Mingyue Cheng — [mycheng@ustc.edu.cn](mailto:mycheng@ustc.edu.cn)

---

## 致谢

TabClaw 的设计深受 [OpenClaw](https://github.com/openclaw/openclaw) 在个人 AI 助手方向上的开创性工作启发，其在智能体交互设计上的探索为我们构建对话式表格分析奠定了重要基础。同时感谢开源智能体社区提供的工具与灵感。

---

## 引用

如果 TabClaw 对你的研究或项目有帮助，欢迎引用：

```bibtex
@misc{tabclaw2026,
  title        = {TabClaw: A Local AI Agent for Conversational Table Analysis},
  author       = {Yu, Shuo and Wang, Daoyu and Li, Qingchuan and Tao, Xiaoyu and Mao, Qingyang and Zhou, Yitong and Cheng, Mingyue and Liu, Qi and Chen, Enhong},
  year         = {2026},
  howpublished = {\url{https://github.com/fishsure/TabClaw}}
}
```
