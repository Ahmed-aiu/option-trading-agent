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

## Auto-Push Policy

Do not let the live trading pipeline, browser watcher, health monitor, or nightly review push directly to git. Those processes can generate evidence, reports, and candidate changes, but publishing code is a separate gated operation.

Acceptable automation boundary:

- Prepare a patch.
- Run the required tests.
- Show `git status -sb`, `git diff --stat`, and the staged file list.
- Refuse to continue if unsafe files are staged.
- Commit and push only when an explicit operator action or explicit opt-in environment variable is present.

Required auto-push gates if a helper script is added later:

- `OPENCLAW_ALLOW_GIT_PUSH=true` or an equivalent one-run approval.
- Current branch has an upstream remote.
- No staged files matching `.env*`, `config/watcher.yaml`, `data/**`, `logs/**`, `*.local.plist`, screenshots, or copied Discord history.
- Required tests and `git diff --check` pass in the same run.
- Commit message includes the nightly report date or manual reason.
- Push target is the current branch's upstream, not an arbitrary remote URL.

## Commit Scope

Safe commit scope:

- Source code under `scripts/`.
- Sanitized config templates under `config/`.
- LaunchAgent templates under `launchd/`.
- Human docs under `README.md` and `docs/`.
- LLM/Codex operating docs under `AGENTS.md` and `SKILL.md`.
- Tests and deterministic fixtures that do not contain private Discord/account data.

Unsafe commit scope:

- Local runtime ledgers.
- Local macOS or Telegram IDs unless intentionally documented as examples.
- Secrets or credentials.
- Any copied private Discord history.
- Local screenshots, browser exports, or generated reports that include private channel text.

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
