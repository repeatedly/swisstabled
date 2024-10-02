# Swiss Tables in D

D port of abseil's Swiss Tables.

# Install

Use dub to add it as a dependency:

```sh
% dub add swisstable
```

# Usage

## Map

Replace built-in AA with `swisstable.Map`.

```D
import std.conv;
import std.stdio;
import swisstable.map;

void main()
{
    auto map = Map!(int, string)();

    foreach (i; 0..10)
        map[i] = text("value", i);

    auto p = 5 in map;  // map[5] also supported
    if (p)
        *p = "new value";

    map.remove(2);
    foreach (e; map.byKeyValue())
        writeln("key: ", e.key, ", value: ", e.value);
}
```

## Set

```D
import std.stdio;
import swisstable.set;

void main()
{
    auto set = Set!(int)();

    foreach (i; 0..10)
        set.insert(i);

    if (5 in set)
        // do something

    set.remove([2, 4]);
    foreach (ref k; set.byKey())
        writeln("key: ", k);
}
```

# TODO

- Support Hasher object if needed
- Use LDC feature for performance optimization
- Improve internal data structure, e.g. remove Control.Sentinel

# Links

* [abseil-cpp](https://github.com/abseil/abseil-cpp/tree/master/absl/container)

  Original implementation by C++

* [rapidhash](https://github.com/Nicoshev/rapidhash)

  Use rapidhash for string/integer/pointer hashing in LDC compiler

# Copyright

    Copyright (c) 2024- Masahiro Nakagawa

# License

Library code are distributed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
Benchmark code are Boost License because these code are copied from druntime's benchmark.
