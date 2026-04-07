This README.md was made by Grok.


# Claw V8 Autonomic Administrator

**An advanced autonomous AI agent for Discord**, powered by OpenAI's GPT-4o, equipped with dynamic concurrency control, background task orchestration, vector-based long-term memory, sandboxed code execution, web content retrieval, and full administrative capabilities over Discord servers.

Claw V8 operates as a self-tuning neuro-symbolic system that can handle both immediate queries and complex, multi-step tasks deferred to the background. It intelligently manages its own resource usage through autonomic tuning and maintains persistent memory across interactions.

## ✨ Key Features

- **Dynamic Concurrency Management**: Adaptive semaphores that automatically scale execution and web-fetch limits based on observed latency.
- **Background Task Pipeline**: Defer complex goals with step-by-step planning; the autonomous worker processes tasks asynchronously and reports progress directly in Discord.
- **Long-Term Vector Memory**: Store and semantically recall information using embeddings for improved context and continuity.
- **Sandbox Code Execution**: Secure Python code running inside isolated Docker containers with strict resource limits.
- **Web Content Retrieval**: Clean extraction of readable text from any webpage.
- **Discord Administration Tools**: Create/delete channels and manage member roles directly via natural language instructions.
- **Conversation Persistence**: SQLite-backed history per user with retrieval for coherent multi-turn interactions.
- **Autonomic Tuning**: Real-time performance monitoring and self-adjustment of concurrency limits.

## 🛠️ Technology Stack

- **Language**: Python 3
- **Discord Library**: `discord.py`
- **LLM Integration**: OpenAI Async Client (`gpt-4o`)
- **Database**: `aiosqlite` with vector embeddings (via `numpy`)
- **Web Parsing**: `beautifulsoup4`
- **Containerization**: Docker for sandboxed code execution
- **Concurrency**: Custom `DynamicSemaphore` with autonomic tuner

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Docker (required for secure code execution)
- A Discord bot token with the following **privileged intents** enabled:
  - Message Content
  - Server Members
  - Guilds
- Administrator permissions in your target Discord server (for channel/role management)
- OpenAI API key with access to `gpt-4o` and `text-embedding-3-small`

### Installation

1. Clone the repository (or save the provided script as `claw_v8.py`):

   ```bash
   git clone <repository-url>
   cd claw-v8
   ```

2. Install dependencies:

   ```bash
   pip install discord.py openai aiosqlite beautifulsoup4 numpy docker
   ```

3. Create a `.env` file or directly edit the script to set your credentials:

   ```python
   DISCORD_BOT_TOKEN = "your_discord_bot_token_here"
   OPENAI_API_KEY = "your_openai_api_key_here"
   ```

   **Note**: For security, it is recommended to use environment variables instead of hard-coding secrets.

4. Ensure Docker is running and the bot has the necessary permissions.

5. Run the bot:

   ```bash
   python claw_v8.py
   ```

The bot will initialize the database, start the autonomous worker and tuner loops, and log "CLAW V8 AUTONOMIC ADMINISTRATOR ONLINE".

### Discord Setup

- Invite the bot to your server using the OAuth2 URL generator in the Discord Developer Portal.
- Grant **Administrator** permissions if full administrative functionality is desired (or selectively enable `Manage Channels` and `Manage Roles`).

## 📖 Usage

Once online, interact with Claw by sending messages in any channel the bot can access. It supports:

- Direct questions and tool-assisted tasks (web search, code execution, memory operations).
- Deferring long-running tasks using `defer_to_background`.
- Administrative commands phrased naturally, such as:
  - "Create a new channel named project-discussion"
  - "Add the Moderator role to @user"
  - "Delete the channel with ID 1234567890"

The agent will automatically decide when to use tools, when to defer tasks, and when to report step completion.

**Background tasks** receive periodic updates in the original channel, allowing the bot to continue working without blocking the conversation.

## ⚙️ Configuration

Key constants at the top of the script:

- `EXEC_SEMAPHORE` and `WEB_SEMAPHORE` initial limits.
- Tuning thresholds in `autonomic_tuner` (`HIGH_THRESH`, `LOW_THRESH`).
- Maximum iterations and history length in `process_with_tools` and `get_history`.

Adjust these values according to your server's scale and API rate limits.

## 🛡️ Security Considerations

- Code execution occurs in a network-disabled Docker container with tight memory and CPU quotas.
- Administrative tools require valid guild context and bot permissions.
- API keys should never be committed to version control.
- Monitor logs for unexpected behavior, especially when granting administrative privileges.

## 📝 Limitations and Future Improvements

- Dynamic semaphore resizing can be further refined for queue-aware adjustments.
- Additional safety layers (e.g., command allow-lists, user authorization) are recommended for public deployments.
- Rate limiting and cost monitoring for OpenAI usage are not yet implemented.

## 🤝 Contributing

Contributions are welcome. Please ensure changes maintain the autonomic and self-contained nature of the agent.

## 📄 License

This project is provided as-is for educational and personal use. Use responsibly and in accordance with Discord and OpenAI terms of service.

---

**Claw V8** — Autonomous. Adaptive. Administrative.
