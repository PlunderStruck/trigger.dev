---
area: webapp
type: fix
---

A run resuming after `triggerAndWait`, `batchTriggerAndWait` or another wait now keeps its place in the queue when its first resume attempt fails, instead of waiting behind runs triggered after it.
