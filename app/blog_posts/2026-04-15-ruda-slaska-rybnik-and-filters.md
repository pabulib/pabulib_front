---
title: Ruda Śląska, Rybnik, and a few filter improvements
slug: ruda-slaska-rybnik-filter-improvements
author: Ignacy Janiszewski
date: 2026-04-15
tags: updates
summary: We added Ruda Śląska and 28 files from Rybnik, introduced filtering by rule, added counts next to filter options, and made permanent download links preserve filter context.
---

Hi!

We are now at **1785 files** in Pabulib, so we are getting closer and closer to the magic number of **2000**.

![Pabulib now contains 1785 files](/static/images/blog/2026-04-15-pb-count-1785.png)

## New city: Ruda Śląska

We added a completely new city to Pabulib: **Ruda Śląska**.

There are currently **2 files**:

- one municipal large-project file
- one municipal small-project file

Together they contain **43 projects** and **24,451 votes**.

Both files use **ordinal voting** with up to **4 choices**, and both are described with the `greedy-exclusive` rule.

[https://pabulib.org/?city=ruda-slaska](https://pabulib.org/?city=ruda-slaska)

## 28 new files from Rybnik

We also added **28 files** from **Rybnik**:

- **1 citywide** file
- **27 district** files

[https://pabulib.org/?city=rybnik](https://pabulib.org/?city=rybnik)

Rybnik is a slightly unusual case.

All 28 files use the **`choose-1`** ballot format and are labeled with the **`greedy`** rule.

At the district level, **18 files contain only one project**, so they are trivially fully funded. In addition, **Radziejów** contains 3 projects and all 3 were funded, which means that overall **19 files are fully funded**.

The citywide file contains **5 projects**, and each project cost is very close to the full budget. That may look strange at first, but in this case it makes sense, because the election is effectively a **choose-one** contest.

## Filter by rule

We added a new filter by **rule**.

This is useful when you want to quickly isolate files that use a specific rule label such as `greedy`, `greedy-exclusive`, or one of the MES variants.

## Filter counts

We also added **counts next to filter options**.

So now the filters show not only what values are available, but also how many files match each option under the current selection.

For example, on the Rybnik page the rule filter shows:

- `greedy (28)`

This small change makes the filters much more informative, especially for cities with many related files.

![Filter counts in the Rybnik view](/static/images/blog/2026-04-15-filter-counts-rybnik.png)

## Filters in permanent links

Following a suggestion from **Andrzej Kaczmarczyk**, we improved **permanent download links**.

These links now preserve the **filter context** used when the download was created, so it is easier to understand what exact subset of files a permalink refers to.

![Permanent link text file with preserved filter context](/static/images/blog/2026-04-15-permalink-filter-context.png)

Cheers!
