# ranker

generates a deduplicated graph across all CHAI package managers by URL, and publishes 
a tea_rank

## Dedupe

- [ ] Add a description here

## Ranking

- [ ] Add a description here

## Usage

1. First deduplicate

   ```bash
   LOAD=true PYTHONPATH=.. ./dedupe.py
   ```

2. Then rank

### With pkgx

```bash
chmod +x main.py
./main.py
```

### Without pkgx

```bash
uv run main.py
```