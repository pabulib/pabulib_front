---
title: A few recent updates in Pabulib
slug: updates-new-files-checker-snapshotrepo-greedy
author: Ignacy Janiszewski
date: 2026-04-02
tags: updates
summary: We recently added 112 new files, Checker and NEW labels, a mobile-friendly layout, and a snapshot repository for tracking changes in all .pb files.
---

Hi! Here is a short update on what we recently added to Pabulib.

## 112 new files!

We added **112 new `.pb` files** from recent participatory budgeting editions in:

- Katowice
- Kraków
- Łódź
- Poznań
- Wrocław

## New flag for freshly added files

Newly added datasets are now easier to find. We introduced a **`NEW` flag** on tiles and also a dedicated filter:

[https://pabulib.org/?require_new=true](https://pabulib.org/?require_new=true)

At the moment, the `NEW` label is shown for 30 days after a file is added, but maybe 60 days might make more sense?

## Checker coverage for older uploads

As always, we are constantly improving **Checker**. When the first files were uploaded, Checker did not exist yet, so we also ran it on the files that had already been uploaded earlier.

This led to two visible changes on the website.

First, checker results are now shown directly on dataset tiles, so you can immediately see whether a file is `valid`, `valid with warnings`, or `invalid`.

Second, each file preview page now includes a dedicated Checker section with the validation status, the number of errors and warnings, and the grouped checker messages.

So, Checker is no longer only something used during upload. It is now also visible for the already published files in the library.

At the moment, **5 files are invalid**. They are all older Warszawa datasets, and we already asked the city for clarification.

## More precise greedy rule variants

We also added several new **greedy rule variants** to the data format and the website.

The reason is simple: in practice, many PB elections use something that is broadly "greedy", but not always in exactly the same way. In some cases the process stops earlier, in some cases there is a threshold, in some cases there are additional constraints or special-case rules explained in comments. So using only one generic `greedy` label was often too imprecise.

The new variants are: `greedy-no-skip`, `greedy-threshold`, `greedy-exclusive`, and `greedy-custom`.

By adding these variants, we can describe the actual rule more faithfully, make the metadata cleaner, and show the differences better on the rules page.

See the actual usage: [https://pabulib.org/details?tab=rules](https://pabulib.org/details?tab=rules) .

## Mobile version improvements

The website should now look much nicer on mobile devices too.

## Snapshot repository with change history

Following a suggestion from **Dominik Peters**, we created a new repository:

[https://github.com/pabulib/pabulib_files](https://github.com/pabulib/pabulib_files)

The repository checks our `.pb` files directory once per hour. If any file is removed, updated, or added, the change is reflected there. So this is now our current snapshot, together with the full change history of all `.pb` files.

Dominik also added a `Copy as Markdown` button to the data format page. This is especially useful if you want to use the format description with an LLM.

## The Blog

We have also started this blog.

For now, we want to use it to share updates like this one and to show that the project is very much alive. But we also have bigger plans: in the future, we would like to publish analyses, longer articles, and perhaps also invite external authors to contribute.

## What comes next

We asked more cities from Poland and Europe for their data, so stay tuned.

We are also constantly changing the layout of the website. If you notice that something should be added or fixed, please reach out to us.

Cheers!
