---
area: webapp
type: fix
---

Fix a rare case where a run could stay stuck executing indefinitely after its task finished, if the result couldn't be recorded at the time.
