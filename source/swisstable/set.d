// Written in the D programming language.

module swisstable.set;

import swisstable.group;
import swisstable.hash;
import swisstable.table;

import std.experimental.allocator.mallocator : Mallocator;

/**
Swiss tables hash set

Examples:
---
// Initialize Set with capacity
auto set = Set!(string)(10);  // Default capacity is 0
set.insert("key");  // set.insert(["k1", "k2"]) also supported
if ("key" in set)
  // do something
set.remove("key");  // set.remove(["k1", "k2"]) also supported

set.length;
set.clear;
set.rehash;
set.dup;
K[] keys = set.keys;
foreach (k; set.byKey()) {}
---

Params:
 T = element type
 Allocator = Allocator type for internal allocation. Default is "shared const Mallocator"

NOTE: Set doesn't guarantee pointer stability. Range and returned pointers are invalidated after insert/rehash.

Copyright: Copyright 2024- Masahiro Nakagawa
Authros:   Masahiro Nakagawa
License:   $(HTTP www.apache.org/licenses/LICENSE-2.0, Apache License Version 2.0)
 */
struct Set(T, Allocator = shared const Mallocator)
{
    import std.range : isInputRange, ElementType;
    import std.traits : hasIndirections, isImplicitlyConvertible;

    // These string enums are used in Table template
    enum CompCondition = "key == _slots[offset]";
    enum HashKeyArg = "*slot";

    alias SlotType = T;
    mixin Table!(Set, T, SlotType, Allocator, hasIndirections!T);

  public:
    /**
     * $(B key in set) syntax support.
     *
     * Returns: true if key exists. Otherwise false.
     */
    bool opBinaryRight(string op)(in T key) inout if (op == "in")
    {
        bool found;
        size_t pos = find(key, found);

        return found;
    }

    /**
     * Insert a single element in the set.
     *
     * Returns: the number of elements inserted.
     */    
    size_t insert(E)(in E value) if (isImplicitlyConvertible!(E, SlotType))
    {
        import std.traits : Unqual;

        bool found;
        size_t pos = findOrPrepareInsert(value, found);
        if (found)
            return 0;

        _slots[pos] = value;
        return 1;
    }

    /**
     * Insert a range of element in the set.
     *
     * Returns: the number of elements inserted.
     */    
    size_t insert(E)(in E values) if (isInputRange!E && isImplicitlyConvertible!(ElementType!E, SlotType))
    {
        import std.traits : Unqual;

        size_t result;

        foreach (ref v; values) {
            bool found;
            size_t pos = findOrPrepareInsert(v, found);
            if (found)
                continue;

            _slots[pos] = v;
            ++result;
        }

        return result;
    }

    /**
     * Remove value from the map.
     *
     * Returns: the number of elements removed.
     */
    size_t remove(E)(in E value) if (isImplicitlyConvertible!(E, SlotType))
    {
        if (_size == 0)
            return 0;

        bool found;
        size_t pos = find(value, found);
        if (found) {
            // destroy(_slots[pos]) is better?
            destroy!(false)(_slots[pos]);

            // erase_meta_only in original implementation
            --_size;
            if (wasNeverFull(_control, _capacity, pos)) {
                setCtrl(_control, _capacity, pos, Control.Empty);
                growthInfo.overwriteFullAsEmpty();
                return 1;
            }

            growthInfo.overwriteFullAsTombstone();
            setCtrl(_control, _capacity, pos, Control.Tombstone);
            return 1;
        }

        return 0;
    }

    /**
     * Remove values from the map.
     *
     * Returns: the number of elements removed.
     */
    size_t remove(E)(in E values) if (isInputRange!E && isImplicitlyConvertible!(ElementType!E, SlotType))
    {
        if (_size == 0)
            return 0;

        size_t result;
        foreach (ref v; values)
            result += remove(v);

        return result;
    }

    /// Returns: a GC allocated dynamic array, the elements of which are the keys in the set.
    @property T[] keys() const nothrow
    {
        return funcKeysValuesBody!(T, "*slot")();
    }

    /// Returns: a forward range suitable for use as a $(B ForeachAggregate) which will iterate over the keys of the set.
    Range byKey() nothrow
    {
        return typeof(return)(_control, _slots.ptr);
    }

    /// $(B foreach) support
    int opApply(scope int delegate(ref T) dg)
    {
        int result;
        auto ctrl = _control;
        auto slot = _slots.ptr;

        skipEmptyOrTombstoneSlots(ctrl, slot);
        while (*ctrl != Control.Sentinel) {
            result = dg(*slot);
            if (result)
                break;

            ++ctrl;
            ++slot;
            skipEmptyOrTombstoneSlots(ctrl, slot);
        }

        return result;
    }

  private @trusted:
    /// ForwardRange for byXXX functions
    static struct Range
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

        @property ref SlotType front()
        {
            return *_slots;
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
    Set!(string) set;

    assert(set.insert("k0") == 1);
    assert(set.insert(["k1", "k2"]) == 2);

    assert(set.length == 3);
    foreach (k; ["k0", "k1", "k2"])
        assert(k in set);

    assert(set.remove("k0") == 1);
    assert(set.remove(["k1", "k2"]) == 2);
    assert(set.length == 0);
}

unittest
{
    // rehash
    import std.algorithm : sort;
    import std.conv : text;

    enum N = 100;
    string[] keys = new string[](N);
    Set!(string) set;

    foreach (i; 0..N) {
        string k = text("k", i);
        keys[i] = k;
        set.insert(k);
    }
    assert(set.length == 100);

    foreach (i; 0..N / 2)
        set.remove(keys[i]);
    assert(set.length == N / 2);
    assert(set.capacity == normalizeCapacity(N));

    set.rehash;

    assert(set.capacity == normalizeCapacity(N / 2));
    foreach (i; N / 2..N)
        assert(keys[i] in set);

    auto ks = set.keys.sort;
    assert(ks[0] == "k50");
    assert(ks[$ - 1] == "k99");
}

unittest
{
    // dup
    import std.algorithm : sort, equal;
    import std.conv : text;

    Set!(int) set;

    enum N = 100;
    foreach (i; 0..N)
        set.insert(i);

    auto nset = set.dup;
    assert(nset.length == set.length);
    assert(nset._capacity == set.capacity);
    assert(equal(nset.keys.sort, set.keys.sort));

    nset.remove(10);
    assert(10 !in nset);
    assert(10 in set);
}

unittest
{
    // clear
    import std.algorithm : sort, equal;
    import std.range : empty;
    import std.conv : text;

    Set!(int) set;

    enum N = 100;
    foreach (i; 0..N)
        set.insert(i);

    auto oldCap = set.capacity;
    set.clear;

    assert(set.length == 0);
    assert(set.capacity == oldCap);
    assert(set.keys.empty);

    foreach (i; 0..10)
        set.insert(i);
    set.clear(false);

    assert(set.length == 0);
    assert(set.capacity == 0);
    assert(set.keys.empty);
}

unittest
{
    import std.algorithm : canFind;
    import std.conv : text;
    import std.range : walkLength;

    enum N = 100;
    int[] keys = new int[](N);
    Set!(int) set;

    foreach (ref k; set.byKey()) assert(false);

    foreach (i; 0..N) {
        keys[i] = i;
        set.insert(i);
    }

    auto kr = set.byKey();
    foreach (ref k; kr.save) {
        assert(keys.canFind(k));
        assert(k in set);
    }
    assert(walkLength(kr) == N);

    // opApply
    int count = 0;
    foreach (ref k; set) {
        count++;
        assert(keys.canFind(k));
    }
    assert(count == N);
}
