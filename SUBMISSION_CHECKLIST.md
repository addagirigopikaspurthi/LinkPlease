# Submission Checklist

## Before Deploy

- Replace placeholder personal details in the PseudoGram `apply` command.
- Generate `PSEUDOGRAM_API_KEY`.
- Push this folder to a public GitHub repo.
- Confirm `FAILURES.md` is in the repo root.

## Deploy

- Use Render Blueprint from `render.yaml` or create one Python web service manually.
- Set `PSEUDOGRAM_API_KEY` as a secret environment variable.
- Confirm `/health` returns `{"status":"ok","api_key":"configured","workers":"enabled"}`.
- Confirm `/stats` returns all four required keys.

## Test With PseudoGram

- Create at least one rule:

```bash
curl -X POST https://YOUR-APP.example.com/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword":"PRICE","dm_message":"Here is the price list"}'
```

- Run a 500-event simulation.
- Wait long enough for the 10/minute DM limit to drain.
- Compare `/stats` with `/v1/simulate/{run_id}/truth`.

## Loom

- Use `LOOM_SCRIPT.md`.
- Keep it around 3 minutes.
- Mention the SQLite tradeoff clearly.
- Mention what you would change with one more week.

## Final Submit

- Submit with `parts_completed: "A+B+C"` only after a 500-event run behaves correctly.
- Keep the deployed app live for 7 days after the deadline.
