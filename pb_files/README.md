# PB dataset files (.pb)

Place your PabuLib `.pb` files in this folder. These are plain-text, semicolon-separated files with three sections:

```
META
key;value
<k1>;<v1>
<k2>;<v2>
...

PROJECTS
project_id;cost;votes;score;name;category;selected
<id>;<cost>;<votes>;<score>;<name>;<category>;<0/1>
...

VOTES
voter_id;vote;points;age;sex
<id>;<project_ids_comma_separated>;<points_comma_separated>;<age>;<sex>
...
```

Required/commonly used META keys:
- `country`, `unit` (or `city`/`district`), `instance` (year), `subunit`
- `description`, `num_projects`, `num_votes`, `budget`, `vote_type`, `currency`
- `min_length`, `max_length` (if fixed-length ballots)

Example filename: `poland_katowice_2024_koszutka.pb`

This repository’s UI reads all `.pb` files in `pb_files/` and renders tiles, with a search and bulk ZIP download.
