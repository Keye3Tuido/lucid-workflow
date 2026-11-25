# 默认推荐的 MCP 协调服务

## 当前状态

新项目和明确迁移的项目默认推荐使用 MCP。Lucid Workflow 已包含单实例 MCP 协调服务，提供真实 Streamable HTTP 工具、项目外事务状态、多 Agent 原子认领、租约接管和共享项目级回答。既有 CLI 项目保持原状，不自动迁移。

当前实现仍属于预览，不代表真实宿主已经注册、自动启动或获得自动唤醒能力。默认推荐不等于可以跳过宿主接入与验收。

## 架构

```text
Agent A／B／C → MCP → 用户级单实例服务 → 项目外 SQLite、会话状态和事件
```

请求显式携带项目、会话和执行者身份，不依赖 MCP 连接保存业务会话。协调服务由宿主或系统服务管理；模型不负责启动、保活或重启进程。

## 安装

默认安装入口会安装共享 Skill、准备隔离依赖，并复用或启动单实例服务：

```bash
python3 <解包目录>/lucid-workflow/scripts/coordination/install_mcp.py
```

已有健康实例直接复用；Skill 版本变化时先停止旧实例，再启动同版本新实例。依赖安装、服务状态和虚拟环境均在项目外。

## 依赖与手动管理

需要 Python 3.10 或更高版本，以及 `scripts/coordination/requirements.txt` 中固定的 `mcp==2.3.0`、`PyYAML==6.0.3`。默认入口会在服务专用虚拟环境安装依赖，不进入项目，也不污染全局 Python。需要分开管理时，仍可使用以下手动步骤。

macOS／Linux 示例：

```bash
SKILL="$HOME/.agents/skills/lucid-workflow"
STATE="$HOME/.local/state/agent-workflow/service"
python3 -m venv "$STATE/venv"
"$STATE/venv/bin/python" -m pip install \
  -r "$SKILL/scripts/coordination/requirements.txt"
```

Windows 使用虚拟环境的 `Scripts/python.exe`，服务状态根目录为 `%LOCALAPPDATA%\agent-workflow\service`。

## 服务生命周期

```bash
# 已健康则复用，不存在或失效才启动
<服务Python> <共享Skill>/scripts/coordination/service_control.py ensure

# 检查真实 MCP 身份、协议和端点
<服务Python> <共享Skill>/scripts/coordination/service_control.py status

# 核验身份后停止当前实例
<服务Python> <共享Skill>/scripts/coordination/service_control.py stop
```

默认状态目录为 `$HOME/.local/state/agent-workflow/service`；Windows 为 `%LOCALAPPDATA%\agent-workflow\service`。可用 `--state-dir` 指定隔离测试目录。

`ensure` 先读取并核验 `endpoint.json`，再使用 MCP 客户端检查协议与服务身份。健康实例直接复用；失效端点会触发有界启动与健康检查。停止前同样核验身份，不因陌生端口或 PID 直接终止进程。

服务进程持有 `service.lock` 的独占锁，直到退出或崩溃。重复或并发启动只能有一个实例监听；客户端退出不会停止服务。系统服务的 KeepAlive／Restart 策略需要在真实安装时另行配置和验收。

## MCP 工具

| 工具组 | 入口与职责 |
| --- | --- |
| 会话与检查点 | `workflow.bootstrap` 恢复要求、任务和开放问题；`workflow.status` 查询状态；`workflow.checkpoint` 按明确用户证据验收并清理对应任务 |
| 要求 | `requirements.list`、`requirements.upsert`、`requirements.remove`、`requirements.review`，复用现有 YAML 校验、锁和审计 |
| 任务 | `tasks.add`、`tasks.claim`、`tasks.claim_next`、`tasks.heartbeat`、`tasks.complete`、`tasks.set_blocked`、`tasks.accept`、`tasks.clean_accepted`、`tasks.list` |
| 问题 | `questions.ask`、`questions.answer`、`questions.list`；项目级完全重复提问复用原记录 |
| Forest | `forest.locate` 定向返回文件、模块和行号；`forest.validate` 执行现有结构检查 |
| 透明审计 | `events.list` 查询协调事件，不重复保存完整验收原话 |

所有工具使用明确的输入、输出类型和结构化结果。任务创建必须有非空 `acceptance_criteria`，完成必须有验证证据，验收必须有用户证据和明确任务范围。

任务只允许 `todo → working → done → accepted`。持有有效租约的执行者才能续期或完成；过期任务可被接管，旧执行者随后写入会被拒绝。依赖为 `done` 或 `accepted` 后，后续任务自动具备认领条件。相同非空 `write_scope` 的活跃任务互斥；当前只实现精确范围字符串，不是完整路径包含分析。

- 已知任务 ID 时，调用 `tasks.claim(project, task_id, agent, session_id)`。成功返回 `{"task_id": "TASK-002"}`；目标不存在、不可认领或该执行者在同一项目已有另一项有效认领时，返回 `{"task_id": null}`，不改领其他任务。执行者重试认领自己仍持有的同一任务时，返回该 ID，不延长租约。
- 按队列取任务时，调用 `tasks.claim_next(project, agent, session_id)`。执行者已有有效认领时返回该任务，否则按 ID 顺序认领第一项符合条件的任务；没有符合条件的任务时返回 `{"task_id": null}`。

## 项目内外文件边界

服务状态目录包含：

- `coordination.sqlite`：任务、依赖、问题、计数器和事件；
- `sessions/<会话哈希>/requirements.yml` 及现有要求审计和锁；
- `cache/<项目哈希>.sqlite`：Forest 派生缓存；
- `service.lock`、`endpoint.json`、`service.log`；
- 可选的服务专用虚拟环境。

项目内只保留 `.doc-forest.yml` 和 `.doc-forest/`。MCP 模式不会在项目根目录创建 `.todos.yml`、项目审计、待办锁或 `.cache/agent-workflow/`。

## 禁止在线混写

同一项目必须明确选择现有 CLI 模式或 MCP 模式。服务不可用时不得自动改用 CLI 写入另一套状态，否则会造成任务和要求分叉。CLI 可用于旧模式、管理和显式迁移；自动降级只允许只读查询。正式切换前需要完成导入、导出和迁移验收。

## 更新与异常

更新服务代码或依赖后，需要受控停止并重新启动，再让客户端重新发现；不能假设运行中的进程已经热更新。常规检查与协调成功保持静默，启动失败、健康检查失败、租约异常、范围冲突和需要用户决定时必须可见。

## 尚未覆盖

当前服务身份为 `lucid-workflow-coordination-poc`，用于可选预览。真实宿主注册、系统服务安装、宿主心跳、Agent 自动创建或唤醒、旧协议与其他 SDK、stdio、远程访问、身份认证、层级路径冲突、语义问题去重、生产迁移和灾难恢复均需另行验证。详细证据见 [mcp-validation.md](mcp-validation.md)。
