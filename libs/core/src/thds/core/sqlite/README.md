# Reading out of a SQLite table

This is the `tabularasa`-like interface. Not as fancy with the types, but will give you the same basic
behavior.

```python
from thds.core.sqlite import struct_table_from_source
from thds.ud.inference import type2_entity

type2s = struct_table_from_source(type2_entity.sqlite.defs.serde.from_item, type2_entity.material)

t2 = type2s.get(type2_npi=1003000233)
steves = type2s.list(name='STEVE')
for t2 in type2s.matching(primary_taxonomy='2084N0400X'):
    # .matching iterates instead of constructing the full list
    print(t2)
```
