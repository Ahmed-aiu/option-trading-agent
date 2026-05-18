# GitHub Publishing

This folder is not currently a Git repository. Before publishing, decide whether the repo should be private or public.

## Do Not Publish

These files are intentionally ignored and should stay local:

- `.env.local`
- `config/watcher.yaml`
- `data/*.jsonl`
- `logs/*`
- `.DS_Store`
- `__pycache__/`

`config/watcher.yaml` can expose private Discord channel IDs or testing usernames. Use `config/watcher.example.yaml` in the repository and keep the local file ignored.

## Preflight

```sh
python3 scripts/test_pipeline.py
python3 scripts/test_full_pipeline.py
python3 scripts/test_steve_options_mvp.py
python3 -m py_compile scripts/*.py
git status --ignored -sb
```

Confirm ignored files include `.env.local`, `data/`, `logs/`, and `config/watcher.yaml`.

## New Repo Flow

```sh
git init
git add .
git status -sb
git commit -m "initial steve options validation bot"
```

With GitHub CLI:

```sh
gh auth status
gh repo create <owner>/<repo-name> --private --source=. --remote=origin --push
```

Use `--public` only after reviewing whether any project names, screenshots, Discord server/channel IDs, or trading data should remain private.

## Suggested Repo Description

```text
Local paper-only Discord options alert validator with Telegram human approval, Alpaca data enrichment, and append-only audit ledgers.
```

## Suggested Topics

```text
trading, options, paper-trading, telegram-bot, alpaca, discord-notifications, validation, jsonl
```

## License

Do not add a license automatically. Pick one intentionally before publishing publicly. Private repos can defer this.
