## MaxPreps ingestion

The ingestion pipeline runs independently rerunnable units keyed by ingestion type,
state, sport, gender, and season. Start with a dry run before writing to Snowflake:

```bash
python src/ingestion/ingest_games.py \
	--config config/maxpreps.example.json \
	--dry-run
```

Successful raw results are cached under `.cache/maxpreps` and progress is recorded in
`.cache/maxpreps/ingestion.jsonl`. Snowflake credentials are loaded from `.env`.
The first live run should use one state and one season, then expand the configuration.
