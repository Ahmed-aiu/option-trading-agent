# GitHub Publishing

This repository is published as `Ahmed-aiu/option-trading-agent`.

## Public Repo Safety

The repository is safe to publish only if local runtime and secret files stay ignored:

- `.env.local`
- `.env`
- `config/watcher.yaml`
- `data/*` except `data/.gitkeep`
- `logs/*` except `logs/.gitkeep`
- `launchd/*.local.plist`
- `__pycache__/`

Do not commit screenshots, Discord credentials, Discord private channel content, Telegram bot tokens, Alpaca keys, or runtime ledgers with real trading/account data.

## Preflight Before Push

Run:

```sh
git status -sb
git status --ignored -sb
python3 scripts/test_pipeline.py
python3 scripts/test_full_pipeline.py
python3 scripts/test_steve_options_mvp.py
python3 -m py_compile scripts/*.py
git diff --check
```

Confirm ignored files include `.env.local`, `config/watcher.yaml`, `data/`, and `logs/`.

## Commit Scope

Safe commit scope:

- Source code under `scripts/`.
- Sanitized config templates under `config/`.
- LaunchAgent templates under `launchd/`.
- Human docs under `README.md` and `docs/`.
- LLM/Codex operating docs under `AGENTS.md` and `SKILL.md`.
- Tests and static samples that do not contain private account data.

Unsafe commit scope:

- Local runtime ledgers.
- Local macOS or Telegram IDs unless intentionally documented as examples.
- Secrets or credentials.
- Any copied private Discord history.

## Recommended Repo Description

```text
Local paper-only Discord options alert validator with Telegram approval, Alpaca data enrichment, browser capture, health monitoring, and nightly source-of-truth reviews.
```

## Suggested Topics

```text
trading, options, paper-trading, telegram-bot, alpaca, discord-notifications, validation, jsonl, macos, codex
```

## License

No license is included yet. Keep it that way until a license is chosen intentionally.
