# tarsnap-old-archives

This allows old archives to be deleted, which might help save on storage costs.
More copies of more recent versions of the archive are saved.

### Example usage

```
tarsnap-old-archives --archive-regex='^.*$' --dry-run=true
```

This will go through your archives and tell you which old ones are likely to be
deleted. Note that this will take a long time to run. It's fine.
