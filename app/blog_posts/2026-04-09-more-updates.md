---
title: Gdynia, Checker 1.0, and a few format updates
slug: gdynia-checker-format-updates
author: Ignacy Janiszewski
date: 2026-04-09
tags: updates
summary: We added 261 new files from Gdynia, deployed Checker 1.0.0, clarified max_length and MES variants, and renamed target to beneficiaries.
---

Hi! Here is another short update:

## 261 new files from Gdynia

We added **261 new `.pb` files** from **Gdynia**.

It turned out that Gdynia publishes the data online, so we processed all available instances starting from **2021**.

## `max_length` clarification

We made one small but important clarification in the data format.

Now we explicitly say that `max_length` cannot be larger than `num_projects`.

So for the format we now use:

`max_length = min(city_max_length, num_projects)`

This is useful because `max_length` may differ across files from the same city or election. For example, the city-wide limit may be 5, but if one specific file contains only 3 projects, then the effective `max_length` is 3.

We also updated this for the files already uploaded on the website.

## Wieliczka and MES rules

In the case of the **Wieliczka** file, the previous rule label was not precise enough, so we updated it to better reflect the actual variant that was used.

At the same time, we added and briefly documented four variants of the Method of Equal Shares:

- `equalshares` – the basic MES rule
- `equalshares-comparison` – MES followed by a comparison step
- `equalshares/add1` – MES with Add1 completion
- `equalshares/add1-comparison` – MES with Add1 completion and a comparison step

This matters, because these variants can lead to different winning sets, so a generic `equalshares` label is sometimes just too vague.

## Checker 1.0.0

We deployed **Checker 1.0.0**.

So now we have the first production version of Checker. Of course, it is still under development and we will keep improving it, but this is an important step.

## `beneficiaries` instead of `target`

As of April 2026, the `PROJECTS` field `target` has been renamed to `beneficiaries`.

The reason is simple: `target` is too ambiguous in this context.

`beneficiaries` is more precise and much clearer.

The checker now treats `target` as invalid and reports an explicit migration error asking to update legacy files to `beneficiaries`.

Cheers!
