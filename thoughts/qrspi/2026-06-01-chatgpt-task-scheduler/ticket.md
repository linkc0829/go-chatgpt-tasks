Source: https://app.notion.com/p/ChatGPT-Task-Scheduler-36f8b5e3718e80809a76f587a0a86e67

# ChatGPT Task Scheduler

## System Requirements
Build a job scheduler with an MCP (Model Context Protocol) interface:
- Users schedule tasks for future execution via MCP tool calls
- A background watcher scans for due jobs and pushes them to a queue
- Workers pull jobs from the queue and execute them
- Support task creation, listing, status checking, and cancellation
- Tool naming follows namespace + action verb pattern (e.g., `task.create`)

## High Level Design
(external object instance / diagram — not captured in text)

## Deep Dive
### How to achieve 10K jobs/sec
1. when a job is created, create a JobRun with status pending
2. watch query for pending JobRun that start within 5 min periodically and push to queue
3. worker consume queue, for jobs
	1. execute success, set JobRun status to success
	2. execute fail, set JobRun status to retry and requeue and retry
	3. exceed max retry time, set JobRun status to failed  send to DLQ
	4. every time JobRun status change, insert a RunEvent corresponding
4. recurringJobWatcher query RunEvent periodically
	1. if a recurring job is terminated(success, failed), create a new JobRun

## How to achieve at-least-once
1. when a message is received by a worker, this message is invisible to other workers
2. if the worker is disconnect or timeout, make this message visible
3. after execute the job, delete the message
4. use job_run_id as idempotent key to prevent duplicate execution

```markdown
## Design Questions

Answer these before you start coding:

1. **Watcher vs Cron:** Why separate the watcher from the worker? What problems does a single cron job that both scans and executes have?

There maybe a job run pick and a single cronjob cannot execute all the job in time(10K jobs/sec)，so with a queue to decouple the producer and comsumer(worker), and make worker scalable to afford more jobs.


2. **Queue Layer:** Why put a queue between the watcher and worker instead of having the watcher call the worker directly? What are the benefits?

- once the worker fail to execute the job, worker could requeue the job and retry
- to achieve at-least-once promise: queue promise at-least-once, and we could use an idempotency key to acheive exactly one by best effort.
- after job failed exceed max retry times put it in DLQ for future review


3. **Time Bucket Partitioning:** Instead of `SELECT * WHERE scheduled_at <= now()`, why partition jobs by time bucket (e.g., hour)? What happens to query performance at 1M+ jobs without partitioning?

- the system expect to support 10K jobs/sec, which mean there will be 600K jobs/min, using time bucket as partition key so that the rows will in the same partition
- db has to collect jobs accross all partitions and result in slow query

4. **Tool Naming:** Why `task.create` instead of `createTask`? How does naming convention affect LLM tool selection accuracy?

- so that LLM could better understand the behavior of MCP server

5. **Registry vs If-Else:** Why use a dictionary registry to route tool calls instead of if-else chains? What happens when you need to add the 20th tool?

- LLM could quick select the tool it need in O(1) time complexity


## Verification

Your prototype is a real MCP server. Test it with the MCP inspector — no Claude needed.

### 1. Start the server (sanity check)

```bash
python -m app.mcp_server
```

The process should hang waiting on stdin (it's a stdio MCP server — that's correct). Ctrl+C to stop. If you see an `ImportError` or other crash, fix that first.

### 2. Run the MCP inspector

Requires Node.js (uses `npx`).

```bash
npx @modelcontextprotocol/inspector python -m app.mcp_server
```

This opens a browser GUI (usually `http://localhost:5173`).

Steps in the GUI:

1. Click **Connect** -> should show 4 tools: `task.create`, `task.list`, `task.status`, `task.cancel`
2. **task.create** -> fill `description="Summarize tech news"`, `scheduled_at="2025-01-01T00:00:00"` (past time so watcher picks it up immediately) -> **Run Tool** -> response should include `{"job_id": 1, "status": "pending", ...}`
3. Wait ~10 seconds, then **task.status** -> `job_id: 1` -> status should now be `"completed"`
4. **task.create** with future time `"2099-12-31T00:00:00"` -> get `job_id: 2`
5. **task.cancel** -> `job_id: 2` -> status `"cancelled"`
6. **task.list** -> see all your jobs

### 3. (Optional) Connect to Claude Desktop / Claude Code

Once the inspector tests pass, the server is ready. To talk to it through Claude:

**Claude Desktop**: edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) and add (use absolute paths):

```json
{
  "mcpServers": {
    "task-scheduler": {
      "command": "/absolute/path/to/scaffold/.venv/bin/python",
      "args": ["-m", "app.mcp_server"],
      "cwd": "/absolute/path/to/scaffold"
    }
  }
}
```

Restart Claude Desktop fully. The 🔨 icon in the chat input should show 4 tools.

**Claude Code**: edit `~/.claude.json` (top-level `mcpServers` for user scope) with the same block, or run `claude mcp add` from inside `scaffold/`.

Then chat:
> "Schedule a task to review PR #123 tomorrow at 9am."
> -> Claude calls `task.create` -> returns job_id
> "What's the status of that task?"
> -> Claude calls `task.status`
```
