// Written in the D programming language.

module swisstable.map;

import swisstable.group;
import swisstable.hash;
import swisstable.table;

import std.experimental.allocator.mallocator : Mallocator;

/**
Swiss tables hash map

Examples:
---
// Initialize Map with capacity
auto map = Map!(string, string)(10);  // Default capacity is 0
map["key"] = "value";
auto p = "key" in map;
if (p)
  *p = "new value";
writeln(map["key"]);  // Print "new value"

// Support AA properties
map.length;
map.clear;
map.rehash;
map.dup;
K[] keys = map.keys;
V[] values = map.values;
foreach (k; map.byKey()) {}
foreach (v; map.byValue()) {}
foreach (e; map.byKeyValue()) {}  // e has key and value properties
---

Params:
 K = key type
 V = value type
 Allocator = Allocator type to use internal allocation. Default is "shared const Mallocator"
 initSlotInIndex = Assign V.init instead of RangeError when key not found. Default is "true"

`initSlotInIndex` is used for `++map[key]` use-cases.

NOTE: Map doesn't guarantee pointer stability. Range and returned pointers are invalidated after insert/rehash.

Copyright: Copyright 2024- Masahiro Nakagawa
Authros:   Masahiro Nakagawa
License:   $(HTTP www.apache.org/licenses/LICENSE-2.0, Apache License Version 2.0)
 */
struct Map(K, V, Allocator = shared const Mallocator, bool initSlotInIndex = true)
{
    import std.traits : hasIndirections;
    import std.typecons : Tuple;

    // These string enums are used in Table template
    enum CompCondition = "key == _slots[offset].key";
    enum HashKeyArg = "slot.key";

    alias SlotType = Tuple!(K, "key", V, "value");
    mixin Table!(Map, K, SlotType, Allocator, hasIndirections!K || hasIndirections!V);

  public:
    static if (initSlotInIndex) {
        /**
         * $(B map[key]) syntax support.
         *
         * With initSlotInIndex=true, $(B key)'s slot will be initialized if key doesn't exist.
         */
        ref V opIndex(in K key)
        {
            import std.traits : Unqual;

            bool found;
            size_t pos = findOrPrepareInsert(key, found);
            if (!found)
                _slots[pos] = SlotType(cast(Unqual!K)(key), V.init);

            return _slots[pos].value;
        }
    } else {
        ref inout(V) opIndex(in K key) inout
        {
            import core.exception : onRangeError;

            bool found;
            size_t pos = find(key, found);
            if (found)
                return _slots[pos].value;

            onRangeError();
        }
    }

    /**
     * $(B key in map) syntax support.
     *
     * Returns: pointer to value corresponding to key
     */
    inout(V)* opBinaryRight(string op)(in K key) inout if (op == "in")
    {
        bool found;
        size_t pos = find(key, found);
        if (found)
            return &_slots[pos].value;

        return null;
    }

    /**
     * $(B map[key] = value) syntax support.
     */    
    void opIndexAssign(in V value, in K key)
    {
        import std.traits : Unqual;

        bool found;
        size_t pos = findOrPrepareInsert(key, found);
        if (found)
            _slots[pos].value = cast(Unqual!V)value;
        else
            _slots[pos] = SlotType(cast(Unqual!K)key, cast(Unqual!V)value);
    }

    /**
     * Remove key from the map.
     *
     * Params:
     *  key = key to search for
     *
     * Returns: true if given key does exist. otherwise false.
     */
    bool remove(in K key)
    {
        if (_size == 0)
            return false;

        bool found;
        size_t pos = find(key, found);
        if (found) {
            // destroy(_slots[pos]) is better?
            destroy!(false)(_slots[pos]);

            // erase_meta_only in original implementation
            --_size;
            if (wasNeverFull(_control, _capacity, pos)) {
                setCtrl(_control, _capacity, pos, Control.Empty);
                growthInfo.overwriteFullAsEmpty();
                return true;
            }

            growthInfo.overwriteFullAsTombstone();
            setCtrl(_control, _capacity, pos, Control.Tombstone);
            return true;
        }

        return false;
    }

    /// Returns: a GC allocated dynamic array, the elements of which are the keys in the map.
    @property K[] keys() const nothrow
    {
        return funcKeysValuesBody!(K, "slot.key")();
    }

    /// Returns: a GC allocated dynamic array, the elements of which are the values in the map.
    @property V[] values() const nothrow
    {
        return funcKeysValuesBody!(V, "slot.value")();
    }

    /// Returns: a forward range suitable for use as a $(B ForeachAggregate) which will iterate over the keys of the map.
    Range!(IterMode.Key) byKey() nothrow
    {
        return typeof(return)(_control, _slots.ptr);
    }

    /// Returns: a forward range suitable for use as a $(B ForeachAggregate) which will iterate over the values of the map.
    Range!(IterMode.Value) byValue() nothrow
    {
        return typeof(return)(_control, _slots.ptr);
    }

    /// Returns: a forward range suitable for use as a $(B ForeachAggregate) which will iterate over the key-value pairs of the map.
    Range!(IterMode.Both) byKeyValue() nothrow
    {
        return typeof(return)(_control, _slots.ptr);
    }

    /// $(B foreach) support
    int opApply(scope int delegate(const ref K,  ref V) dg)
    {
        int result;
        auto ctrl = _control;
        auto slot = _slots.ptr;

        skipEmptyOrTombstoneSlots(ctrl, slot);
        while (*ctrl != Control.Sentinel) {
            result = dg(slot.key, slot.value);
            if (result)
                break;

            ++ctrl;
            ++slot;
            skipEmptyOrTombstoneSlots(ctrl, slot);
        }

        return result;
    }

  private @trusted:
    enum IterMode { Key, Value, Both }

    /// ForwardRange for byXXX functions
    static struct Range(IterMode Mode)
    {
      private:
        Control* _control = emptyGroup;
        SlotType* _slots;

      public nothrow @trusted:
        @disable this();

        this(Control* ctrl, SlotType* slots)
        {
            _control = ctrl;
            _slots = slots;
            skipEmptyOrTombstoneSlots(_control, _slots);
        }

        @property bool empty() const
        {
            return *_control == Control.Sentinel;
        }

        static if (Mode == IterMode.Both) {
            @property ref SlotType front()
            {
                return *_slots;
            }
        } else static if (Mode == IterMode.Key) {
            @property ref K front()
            {
                return _slots.key;
            }
        } else {
            @property ref V front()
            {
                return _slots.value;
            }
        }

        void popFront()
        {
            ++_control;
            ++_slots;
            skipEmptyOrTombstoneSlots(_control, _slots);
        }

        typeof(this) save()
        {
            return typeof(this)(_control, _slots);
        }
    }
}

unittest
{
    Map!(string, int) map;

    map["k0"] = 9;
    map["k1"]++;

    assert(map.length == 2);
    assert("k1" in map);
    assert(map["k0"] == 9);
    assert(map["k1"] == 1);
}

unittest
{
    // initSlotInIndex is false
    import core.exception : RangeError;

    Map!(int, int, shared const Mallocator, false) map;

    map[0] = 9;
    try {
        map[1]++;
        assert(false, "don't reach here");
    } catch (RangeError) {
        assert(map.length == 1);
        assert(0 in map);
    }
}

unittest
{
    // remove and rehash
    import std.algorithm : sort;
    import std.conv : text;

    enum N = 100;
    string[] keys = new string[](N);
    Map!(string, string) map;

    foreach (i; 0..N) {
        string k = text("k", i);
        keys[i] = k;
        map[k] = text("v", i);;
    }
    assert(map.length == 100);

    foreach (i; 0..N / 2)
        map.remove(keys[i]);
    assert(map.length == N / 2);
    assert(map.capacity == normalizeCapacity(N));

    map.rehash;

    assert(map.capacity == normalizeCapacity(N / 2));
    foreach (i; N / 2..N)
        assert(keys[i] in map);

    auto ks = map.keys.sort;
    auto vs = map.values.sort;
    assert(ks[0] == "k50");
    assert(vs[0] == "v50");
    assert(ks[$ - 1] == "k99");
    assert(vs[$ - 1] == "v99");
}

unittest
{
    // dup
    import std.algorithm : sort, equal;
    import std.conv : text;

    Map!(int, string) map;

    enum N = 100;
    foreach (i; 0..N)
        map[i] = text("v", i);

    auto nmap = map.dup;
    assert(nmap.length == map.length);
    assert(nmap._capacity == map.capacity);
    assert(equal(nmap.keys.sort, map.keys.sort));
    assert(equal(nmap.values.sort, map.values.sort));

    nmap[10] = "hey";
    assert(nmap[10] == "hey");
    assert(map[10]  == "v10");
}

unittest
{
    // clear
    import std.algorithm : sort, equal;
    import std.range : empty;
    import std.conv : text;

    Map!(int, string) map;

    enum N = 100;
    foreach (i; 0..N)
        map[i] = text("v", i);

    auto oldCap = map.capacity;
    map.clear;

    assert(map.length == 0);
    assert(map.capacity == oldCap);
    assert(map.keys.empty);
    assert(map.values.empty);

    foreach (i; 0..10)
        map[i] = text("v", i);
    map.clear(false);

    assert(map.length == 0);
    assert(map.capacity == 0);
    assert(map.keys.empty);
    assert(map.values.empty);
}

unittest
{
    // struct key-value test
    import std.conv : text;

    static struct Test
    {
        int a;
        double b;
        string c;

        this(ref return scope Test rhs) nothrow @safe
        {
            a = rhs.a;
            b = rhs.b;
            c = rhs.c;
        }
    }

    enum N = 100;
    Test[] tests;
    Map!(Test, Test) map;

    foreach (i; 0..N) {
        auto t = Test(i, cast(double)i + 0.1, text("v", i));
        tests ~= t;
        map[t] = t;
        assert(map[t] == t);
    }

    foreach (i; 0..N) {
        if (i % 2 == 0) {
            map.remove(tests[i]);
            assert(!(tests[i] in map));
        }
    }
    assert(map.length == N / 2);

    foreach (i; 0..N) {
        if (i % 2 == 1) {
            auto t = tests[i];
            auto p = t in map;
            assert(p);
            p.a = 100000;
            p.c = "test";
            assert(map[t].a == 100000);
            assert(map[t].c == "test");
        }
    }

    map.clear;

    // Test temporal objects
    foreach (i; 0..N) {
        map[Test(i, 0, text(i))] = Test(i, 0, text(i));
        assert(Test(i, 0, text(i)) in map);
        assert(map.remove(Test(i, 0, text(i))));
    }
    assert(map.length == 0);
}

unittest
{
    // byXXX
    import std.algorithm : canFind;
    import std.conv : text;
    import std.range : walkLength;

    enum N = 100;
    int[] keys = new int[](N);
    string[] vals = new string[](N);
    Map!(int, string) map;

    foreach (ref k; map.byKey()) assert(false);
    foreach (ref v; map.byValue()) assert(false);
    foreach (ref e; map.byKeyValue()) assert(false);

    foreach (i; 0..N) {
        string v = text("v", i);
        keys[i] = i;
        vals[i] = v;
        map[i] = v;
    }

    auto kr = map.byKey();
    foreach (ref k; kr.save) {
        assert(keys.canFind(k));
        assert(k in map);
    }
    assert(walkLength(kr) == N);

    auto vr = map.byValue();
    foreach (ref v; vr.save)
        assert(vals.canFind(v));
    assert(walkLength(vr) == N);

    auto kvr = map.byKeyValue();
    foreach (ref e; kvr.save) {
        assert(keys.canFind(e.key));
        assert(vals.canFind(e.value));
        assert(e.key in map);
        assert(map[e.key] == e.value);
    }
    assert(walkLength(kvr) == N);

    // opApply
    int count = 0;
    foreach (ref k, ref v; map) {
        count++;
        assert(keys.canFind(k));
        assert(vals.canFind(v));
    }
    assert(count == N);
}
