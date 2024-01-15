# thds.core.config

This is an attempt at a be-everything-to-everybody configuration 'system'.

Highlights:

- Configuration is always accessible and configurable via normal Python code.
- Configuration is type-safe.
- All active configuration is 'registered' and therefore discoverable.
- Config can be temporarily overridden for the current thread.
- Config can be set via a known environment variable.
- Config can be set by combining one or more configuration objects - these may be loaded from files, but
  this system remains agnostic as to the format of those files or how and when they are actually loaded.

The basic usage is as follows:

### Defining the ConfigItem in module `foo` (and its associated usage)

```python
from thds.core import config

# the capitalization of the variable itself is purely a convention,
# intended to denote that this name should never be reassigned in the code.
BAR = config.item('bar', 42, parse=int)
# The lowercase-ness of the string name is intended to follow the convention
# of module and function names in Python being lowercase.

def barbarbar() -> int:  # just a random function that uses the config
    return BAR() * 3
```

### Setting the value from module `baz` (programmatically)

```python
import foo

assert foo.barbarbar() == 126
foo.BAR.set_global(100)  # set the value directly using the BAR ConfigItem object
assert foo.barbarbar() == 300
with foo.BAR.set_local(200):
    assert foo.barbarbar() == 600
assert foo.barbarbar() == 300  # back to the global value
```

### Setting the value with an environment variable

```bash
export FOO_BAR=27
python -c "from foo import BAR; assert BAR() == 27"
```

The naming of environment variables depends on your shell. Some shells will support arbitrary var names,
so you can use `export foo.bar=27`. Others have a more limited character set. To support this, we will
'search' for two alternative variable names, one that replaces `-` and `.` with `_`, and another that
does those replacements and also force-uppercases the config item string.

### Discoverability

```python
from thds.core import config

print(config.get_all_config())
# {'foo.bar': 42}
```

or

```bash
poetry install  # to ensure that the CLI is properly installed
poetry run show-thds-config your.root.module
```

### Loading config from a file programmatically

```python
from thds.core import config

# supports multiple configuration files. Later files override earlier ones.
config.set_global_defaults(tomli.load(open('config_a.toml', 'rb')))
config.set_global_defaults(json.load(open('config_b.json')))
```
