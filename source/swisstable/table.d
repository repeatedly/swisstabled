// Written in the D programming language.

module swisstable.table;

import swisstable.group;
import swisstable.hash;

/**
Swiss tables internal data structure and functions

This mixin template is used in container struct.

Params:
 Container = Container type which uses this template
 KeyType = Key type for hash.
 SlotType = Slot type, e.g. Tuple in Map
 Allocator = Allocator type for internal allocation. Default is "shared const Mallocator"
 CallGCRange = If true, call GC's addRange/removeRange for stored data

NOTE: This table based containers don't guarantee pointer stability. Range and returned pointers are invalidated after insert/rehash.

Copyright: Copyright 2024- Masahiro Nakagawa
Authros:   Masahiro Nakagawa
License:   $(HTTP www.apache.org/licenses/LICENSE-2.0, Apache License Version 2.0)
Credits:   Internal design and logic are based on abseil's hash map/set. See also
           $(HTTP abseil.io/about/design/swisstables, Swiss Tables Design Notes) and $(HTTP github.com/abseil/abseil-cpp/blob/master/absl/container/internal/raw_hash_set.h, abseil's source)
 */
mixin template Table(Container, KeyType, SlotType, Allocator, alias CallGCRange)
{
    import core.memory : GC;
    import std.algorithm.comparison : max;
    import std.experimental.allocator.common : stateSize;

    enum HeapAlignment = max(GrowthInfo.alignof, SlotType.alignof);

  private:
    /*
     * Internal table is consists of growth info, control bytes and actual data slots.
     * Container allocates one large table to store these information.
     *
     * Here is a layout of the table:
     * {
     *     // The available slots before growing the capacity. Managed by GrowthInfo object.
     *     size_t growthInfo;
     *     // The control bytes corresponding to actual data `slots`.
     *     // `_control` member variable points to the first address of this member. 
     *     Control[capacity] controls;
     *     // Sentinel for control bytes. Range / iterate operations use this marker as a stopper
     *     // TODO: We can remove this sentinel by accurate offset control.
     *     Control Control.Sentinel;
     *     // Copy of 'Group.Width - 1' bytes of above `controls`.
     *     // Group can safely scan control bytes by this member.
     *     Control[Group.Width - 1] clonedControls
     *     // Data slots to store actual key-value pairs.
     *     // `_slots` member variable is same as this member.
     *     SlotType[capacity] slots;
     * }
     */
    size_t _capacity, _size;
    Control* _control = emptyGroup();
    SlotType[] _slots;
    Allocator _allocator;
    size_t _hashSeed = RapidSeed; // Use hahser object for various hash support?

    invariant
    {
        assert(_capacity == 0 || isValidCapacity(_capacity));
    }

  public @trusted:
    static if (stateSize!Allocator > 0) {
        // If Allocator has an internal state, Container must be initialized with an allocator object.
        @disable this();

        /// Constructor with the Allocator.
        this(Allocator allocator) nothrow
        {
            _allocator = allocator;
        }

        /// Constructor taking an initial capacity and the Allocator. Given capacity is automatically normalized to proper value.
        this(Allocator allocator, size_t capacity) nothrow
        {
            _allocator = allocator;
            resize(normalizeCapacity(capacity));
        }
    } else {
        /// Constructor taking an initial capacity. Given capacity is automatically normalized to proper value.
        this(size_t capacity) nothrow
        {
            resize(normalizeCapacity(capacity));
        }
    }

    ~this()
    {
        clear(false);
    }

    /// Returns: the number of stored slots in the container.
    @property size_t length() const nothrow { return _size; }

    /// Returns: the number of current capacity.
    @property size_t capacity() const nothrow { return _capacity; }

    /// Setter for hash seed.
    @property void hashSeed(size_t seed)
    {
        import std.exception : enforce;

        enforce(_size == 0, "Can't set new hash seed for non-empty table");
        _hashSeed = seed;
    }

    /**
     * Returns: a new Container of the same size and copy the contents of Container into it.
     */
    Container dup() const nothrow
    {
        import core.stdc.string : memcpy;

        static if (stateSize!Allocator)
            Container result = Container(_allocator, _capacity);
        else
            Container result = Container(_capacity);
        auto layout = TableLayout(_capacity, HeapAlignment);

        result._size = _size;
        result._hashSeed = _hashSeed;
        memcpy(result._control - layout.controlOffset, _control - layout.controlOffset,
               layout.allocSize(SlotType.sizeof));

        return result;
    }

    /**
     * Reorganizes the container in place.
     *
     * Params:
     *  cap = new capacity for rehash. Callers can force rehash/resize by specifying 0.
     */
    void rehash(size_t cap = 0) nothrow
    {
        if (cap == 0) {
            if (_capacity == 0 || _size == 0)
                return;
        }

        auto newCap = normalizeCapacity(max(cap, growthToLowerboundCapacity(_size)));
        if (cap == 0 || newCap > _capacity)
            resize(newCap);
    }

    /**
     * Removes all remaining slots from a container.
     *
     * Params:
     *  reuse = If true, keep allocated memory and reset metadata. Otherwise all resources are disposed.
     */
    void clear(bool reuse = true) nothrow
    {
        import std.experimental.allocator : dispose;

        if (_capacity > 0) {
            destroySlots();

            if (reuse) {
                resetCtrl(_control, _capacity);
                _size = 0;
            } else {
                auto layout = TableLayout(_capacity, HeapAlignment);
                auto allocSize = layout.allocSize(SlotType.sizeof);
                auto data = cast(ubyte[])(_control - layout.controlOffset)[0..allocSize];

                static if (CallGCRange)
                    GC.removeRange(data.ptr);
                _allocator.dispose(data);

                _size = _capacity = 0;
                _control = emptyGroup();
                _slots = null;
            }
        }
    }

  private @trusted:
    // Actual collect code for `keys()` and `values()`
    auto funcKeysValuesBody(Elem, string putBody)() const nothrow
    {
        // import std.algorithm.mutation : move; This move does't support void[0]
        import core.lifetime : move;
        import std.array : appender;

        auto ctrl = cast(Control*)_control;
        auto slot = cast(SlotType*)_slots.ptr;
        auto res = appender!(Elem[])();

        skipEmptyOrTombstoneSlots(ctrl, slot);
        while (*ctrl != Control.Sentinel) {
            res.put(move(mixin(putBody)));
            ++ctrl;
            ++slot;
            skipEmptyOrTombstoneSlots(ctrl, slot);
        }

        return res.data;
    }

    // Called by Range and iterate functions
    static void skipEmptyOrTombstoneSlots(ref Control* ctrl, ref SlotType* slot) nothrow
    {
        while ((*ctrl).isEmptyOrTombstone) {
            uint shift = Group(ctrl).countLeadingEmptyOrTombstone();
            ctrl += shift;
            slot += shift;
        }
    }

    /// Helper class for calculating table layout and offset
    static struct TableLayout
    {
      private:
        size_t _capacity, _slotOffset;

      public nothrow @safe:
        this(size_t capacity, size_t slotAlign)
        {
            _capacity = capacity;
            _slotOffset = (controlOffset() + numControlBytes(capacity) + slotAlign - 1) & (~slotAlign + 1);
        }

        @property size_t capacity() const { return _capacity; }
        @property size_t slotOffset() const { return _slotOffset; }
        @property size_t controlOffset() const { return GrowthInfo.sizeof; }
        @property size_t allocSize(size_t slotSize) const { return _slotOffset + (_capacity * slotSize); }
    }

    unittest
    {
        auto layout = TableLayout(7, 8);
        assert(layout.capacity == 7);
        assert(layout.controlOffset == GrowthInfo.sizeof);
        // GroupSSE2 and GroupPortable have diffrent width
        version (D_SIMD)
        {
            assert(layout.slotOffset == 32);
            assert(layout.allocSize(16) == 144);
        }
        else
        {
            assert(layout.slotOffset == 24);
            assert(layout.allocSize(16) == 136);
        }
    }

    /**
     * Destroy existing slots for cleanup resources.
     */
    void destroySlots() nothrow
    {
        // TODO : Skip when K and V don't have destructors
        auto ctrl = _control;
        auto slots = _slots.ptr;

        if (_capacity < (Group.Width - 1)) {  // Small table case
            auto m = GroupPortable(ctrl + _capacity).maskFull();  // use GroupPortable for small table
            --slots;
            foreach (i; m)
                destroy(slots[i]);

            return;
        } else {
            auto remaining = _size;
            while (remaining != 0) {
                foreach (uint i; Group(ctrl).maskFull()) {
                    destroy(slots[i]);
                    --remaining;
                }
                ctrl += Group.Width;
                slots += Group.Width;
            }
        }
    }

    // helper for remove function
    static bool wasNeverFull(const Control* ctrl, size_t cap, size_t index) nothrow
    in
    {
        assert(isValidCapacity(cap));
    }
    do
    {
        const size_t indexBefore = (index - Group.Width) & cap;
        const emptyAfter = Group(ctrl + index).maskEmpty();
        const emptyBefore = Group(ctrl + indexBefore).maskEmpty();

        return emptyBefore && emptyAfter &&
            (emptyAfter.trailingZeros() + emptyBefore.leadingZeros()) < Group.Width;
    }

    /**
     * Attempt to find key.
     *
     * Params:
     *  key = key to search for
     *  found = indicate whether key is found or not
     *
     * Returns: slot index
     */
    size_t find(const ref KeyType key, out bool found) const
    {
        auto hash = rapidhashOf(key, _hashSeed);
        auto seq = probe(_capacity, hash);

        found = false;
        while (true) {
            auto g = Group(_control + seq.offset);
            foreach (i; g.match(calcH2(hash))) {
                auto offset = seq.offset(i);
                if (mixin(CompCondition)) {  // why Object.opEquals is not 'nothrow'...
                    found = true;
                    return offset;
                }
            }
            if (g.maskEmpty())
                return 0;

            seq.next();
        }

        assert(0);
    }

    /**
     * Attempt to find key. If key not found, prepare new slot and update control bytes.
     *
     * Params:
     *  key = key to search for
     *  found = indicate whether key is found or not
     *
     * Returns: insertable/updatable slot index
     */
    size_t findOrPrepareInsert(const ref KeyType key, out bool found)
    {
        auto hash = rapidhashOf(key, _hashSeed);
        auto seq = probe(_capacity, hash);

        found = false;
        while (true) {
            auto g = Group(_control + seq.offset);
            foreach (i; g.match(calcH2(hash))) {
                auto offset = seq.offset(i);
                if (mixin(CompCondition)) {
                    found = true;
                    return offset;
                }
            }
            auto maskEmpty = g.maskEmpty();
            if (maskEmpty) {
                auto target = seq.offset(maskEmpty.lowestBitSet());
                return prepareInsert(hash, target);
            }

            seq.next();
        }

        assert(0);
    }

    /**
     * Find slot index to insert. If there is no available space, the table can be resized.
     *
     * Returns: insertable, empty or Tombstone, slot index
     */
    size_t prepareInsert(size_t hash, size_t target) nothrow
    {
        auto gi = growthInfo;
        if (gi.hasNoTombstoneAndGrowthLeft()) {
            if (gi.hasNoGrowthLeftAndNoTombstone()) {
                resize(nextCapacity(_capacity));
                target = findFirstNonFull(hash);
            }
        } else {
            if (gi.growthLeft > 0)
                target = findFirstNonFull(hash);
            else
                target = findInsertPositionWithGrowthOrRehash(hash);
        }

        ++_size;
        growthInfo.overwriteControlAsFull(_control[target]);
        setCtrl(_control, _capacity, target, cast(Control)calcH2(hash));

        return target;
    }

    size_t findInsertPositionWithGrowthOrRehash(size_t hash) nothrow
    {
        const cap = _capacity;

        if (cap > Group.Width && (_size * 32UL) <= (cap * 25UL))
            dropTomstonesWithoutResize();
        else
            resize(nextCapacity(cap));

        return findFirstNonFull(hash);
    }

    void dropTomstonesWithoutResize() nothrow
    {
        import core.stdc.string : memcpy;

        // See DropDeletesWithoutResize of abseil's raw_hash_set for the internal algorithm

        SlotType* slots = _slots.ptr;
        Control* ctrl = _control;
        const cap = _capacity;

        // Convert Tombstone to Empty and Full to Tombstone
        for (Control* pos = ctrl; pos < ctrl + cap; pos += Group.Width)
            Group(pos).convertSpecialToEmptyAndFullToTombstone(pos);
        memcpy(ctrl + cap + 1, ctrl, numClonedBytes());
        ctrl[cap] = Control.Sentinel;

        enum UnknownId = size_t.max;
        size_t tmpId = UnknownId;
        SlotType* slot = _slots.ptr;

        for (size_t i; i != cap; i++, slot++) {
            if (ctrl[i].isEmpty) {
                tmpId = i;
                continue;
            }
            if (!ctrl[i].isTombstone)
                continue;

            const hash = rapidhashOf(mixin(HashKeyArg), _hashSeed);
            const newI = findFirstNonFull(hash);
            const probeOffseet = probe(cap, hash).offset;
            size_t probeIndex(size_t pos) {
                return ((pos - probeOffseet) & cap) / Group.Width;
            }

            if (probeIndex(newI) == probeIndex(i)) {
                setCtrl(ctrl, cap, i, cast(Control)calcH2(hash));
                continue;
            }

            SlotType* newSlotPtr = slots + newI;
            if (ctrl[newI].isEmpty) {
                setCtrl(ctrl, cap, newI, cast(Control)calcH2(hash));
                memcpy(newSlotPtr, slot, SlotType.sizeof);
                setCtrl(ctrl, cap, i, Control.Empty);
                tmpId = i;
            } else {
                setCtrl(ctrl, cap, newI, cast(Control)calcH2(hash));

                if (tmpId == UnknownId)
                    tmpId = findEmptySlot(ctrl, i + 1, cap);

                SlotType* tmpSlotPtr = slots + tmpId;

                // Swap i and newI slot
                memcpy(tmpSlotPtr, newSlotPtr, SlotType.sizeof);
                memcpy(newSlotPtr, slot, SlotType.sizeof);
                memcpy(slot, tmpSlotPtr, SlotType.sizeof);

                // repeat i postition again
                i--;
                slot--;
            }
        }

        resetGrowthLeft();
    }

    // Returns: slot index
    size_t findEmptySlot(const Control* ctrl, size_t start, size_t end) nothrow
    {
        import std.exception : enforce;

        for (size_t i = start; i < end; i++) {
            if (ctrl[i].isEmpty)
                return i;
        }

        assert(false, "No empty slots. Caller must guarantee empty slot in given controls");
        return size_t.max;
    }

    // Set ctrl[index] to hash.
    static void setCtrl(Control* ctrl, size_t cap, size_t index, Control hash) nothrow
    {
        ctrl[index] = hash;
        ctrl[((index - numClonedBytes()) & cap) + (numClonedBytes() & cap)] = hash; // Update mirror position together.
    }

    static void resetCtrl(Control* ctrl, size_t cap) nothrow
    {
        import core.stdc.string : memset;

        memset(ctrl, cast(byte)Control.Empty, numControlBytes(cap));
        ctrl[cap] = Control.Sentinel;
    }

    /**
     * Probe a control bytes using ProbeSeq derived from given hash,
     * and return the offset of first tombstone or empty slot.
     *
     * NOTE: If the entire table is full, the behavior is undefined.
     */
    size_t findFirstNonFull(size_t hash) nothrow
    {
        auto seq = probe(_capacity, hash);
        auto offset = seq.offset;

        if (_control[offset].isEmptyOrTombstone)
            return offset;

        while (true) {
            auto g = Group(_control + seq.offset);
            auto m = g.maskEmptyOrTombstone();
            if (m)
                return seq.offset(m.lowestBitSet());

            seq.next();
        }
    }

    /**
     * Resize internal teble with given capacity.
     */
    void resize(size_t newCap) nothrow
    in
    {
        assert(isValidCapacity(newCap));
    }
    do
    {
        import core.stdc.string : memcpy;
        import std.experimental.allocator : dispose;

        if (_capacity == 0) {
            initTable(newCap);
        } else {
            // initTable updates following members with allocated slots.
            auto oldCap = _capacity;
            auto layout = TableLayout(oldCap, HeapAlignment);
            auto oldCtrl = _control;
            auto oldSlots = _slots.ptr;

            initTable(newCap);

            // Move data from old slots to new slots
            void insertSlot(SlotType* slot) {
                size_t h = rapidhashOf(mixin(HashKeyArg), _hashSeed);
                size_t offset = findFirstNonFull(h);
                setCtrl(_control, _capacity, offset, cast(Control)calcH2(h));
                memcpy(_slots.ptr + offset, slot, SlotType.sizeof);
            }
            for (size_t i; i != oldCap; i++) {
                if (oldCtrl[i].isFull)
                    insertSlot(oldSlots + i);
            }

            // Bye old slots
            auto data = cast(ubyte[])(oldCtrl - layout.controlOffset)[0..layout.allocSize(SlotType.sizeof)];
            static if (CallGCRange)
                GC.removeRange(data.ptr);
            _allocator.dispose(data);
        }
    }

    /**
     * Allocate and initialize a new table with new capacity. This also initializes control and growth info.
     */
    void initTable(size_t newCap) nothrow
    in
    {
        assert(isValidCapacity(newCap));
    }
    do
    {
        import core.stdc.string : memset;

        auto layout = TableLayout(newCap, HeapAlignment);
        auto size = layout.allocSize(SlotType.sizeof);
        auto data = cast(ubyte[])_allocator.allocate(size);
        static if (CallGCRange)
            GC.addRange(data.ptr, size);

        // Initialize control and data slots
        _capacity = newCap;
        _control = cast(Control*)(data.ptr + layout.controlOffset());
        _slots = cast(SlotType[])(data.ptr + layout.slotOffset())[0..size - layout.slotOffset()];
        resetGrowthLeft();

        // Reset control array
        resetCtrl(_control, _capacity);
    }

    // Reset growth info with current capacity and size.
    void resetGrowthLeft() nothrow @safe
    {
        growthInfo.initializeWith(capacityToGrowth(_capacity) - _size);
    }

    // getter for growth info in the table.
    GrowthInfo* growthInfo() nothrow
    {
        return cast(GrowthInfo*)(_control) - 1;
    }
}

// TODO : Add SwissTable internal tests

package:

nothrow pure @safe @nogc
{

bool isValidCapacity(size_t cap) { return ((cap + 1) & cap) == 0 && cap > 0; }
size_t numClonedBytes() { return Group.Width - 1; }
size_t numControlBytes(size_t cap) { return cap + 1 + numClonedBytes(); }
// Returns: the next valid capacity
size_t nextCapacity(size_t cap) { return cap * 2 + 1; }
// Returns: Convert `num` into next valid capacity
size_t normalizeCapacity(size_t num) { return num ? size_t.max >> countLeadingZeros(num) : 1; }

unittest
{
    // normalizeCapacity
    assert(normalizeCapacity(0) == 1);
    assert(normalizeCapacity(1) == 1);
    assert(normalizeCapacity(2) == 3);
    assert(normalizeCapacity(3) == 3);
    assert(normalizeCapacity(4) == 7);
    assert(normalizeCapacity(7) == 7);
    assert(normalizeCapacity(15 + 1) == 15 * 2 + 1);
    assert(normalizeCapacity(15 + 2) == 15 * 2 + 1);
}

/*
 * Swiss Tables uses 0.875, 7 / 8, as max load factor. `capacityToGrowth` and `growthToLowerboundCapacity` follow it
 */

// Calculate the number of available slots based on load factor.
size_t capacityToGrowth(size_t cap)
in
{
    assert(isValidCapacity(cap));
}
do
{
    if (Group.Width == 8 && cap == 7)
        return 6;
    return cap - (cap / 8);
}

// This function doesn't gurantee the result is valid capacity. Apply `normalizeCapacity` to function result.
size_t growthToLowerboundCapacity(size_t growth)
{
    if (Group.Width == 8 && growth == 7)
        return 8;
    
    return growth + cast(size_t)((growth - 1) / 7);
}

} // nothrow pure @safe @nogc

unittest
{
    // capacityToGrowth and growthToLowerboundCapacity
    foreach (growth; 0..10000) {
        size_t cap = normalizeCapacity(growthToLowerboundCapacity(growth));
        assert(capacityToGrowth(cap) >= growth);
        // For (capacity + 1) < Group.Width case, growth should equal capacity.
        if (cap + 1 < Group.Width)
            assert(capacityToGrowth(cap) == cap);
        else
            assert(capacityToGrowth(cap) < cap);
        if (growth != 0 && cap > 1)
            assert(capacityToGrowth(cap / 2) < growth); // There is no smaller capacity that works.
    }

    for (size_t cap = Group.Width - 1; cap < 10000; cap = cap * 2 + 1) {
        size_t growth = capacityToGrowth(cap);
        assert(growth < cap);
        assert(growthToLowerboundCapacity(growth) <= cap);
        assert(normalizeCapacity(growthToLowerboundCapacity(growth)) == cap);
    }
}

/**
 * Store the information regarding the number of slots we can still fill without rehash.
 */
struct GrowthInfo
{
    enum size_t GrowthLeftMask = size_t.max >> 1;
    enum size_t TombstoneBit = ~GrowthLeftMask;

  private:
    size_t _growthLeft;

  public nothrow @safe @nogc:
    this(size_t gl) { _growthLeft = gl; }
    void initializeWith(size_t gl) { _growthLeft = gl; }
    void overwriteFullAsEmpty() { ++_growthLeft; }
    void overwriteEmptyAsFull() { --_growthLeft; }
    void overwriteManyEmptyAsFull(size_t count) { _growthLeft -= count; }
    void overwriteControlAsFull(Control c) { _growthLeft -= cast(size_t)c.isEmpty; }
    void overwriteFullAsTombstone() { _growthLeft |= TombstoneBit; }
    bool hasNoTombstoneAndGrowthLeft() const { return cast(ptrdiff_t)_growthLeft > 0; }
    bool hasNoGrowthLeftAndNoTombstone() const { return _growthLeft == 0; }
    bool hasNoTombstone() const { return cast(ptrdiff_t)_growthLeft >= 0; }
    size_t growthLeft() const { return _growthLeft & GrowthLeftMask; }
}

unittest
{
    // growthLeft
    {
        GrowthInfo gi;
        gi.initializeWith(5);
        assert(gi.growthLeft == 5);
        gi.overwriteFullAsTombstone();
        assert(gi.growthLeft == 5);
    }
    // hasNoTombstone
    {
        GrowthInfo gi;
        gi.initializeWith(5);
        assert(gi.hasNoTombstone());
        gi.overwriteFullAsTombstone();
        assert(!gi.hasNoTombstone());
        gi.initializeWith(5); // after re-initialization, no deleted
        assert(gi.hasNoTombstone());
    }
    // hasNoTombstoneAndGrowthLeft
    {
        GrowthInfo gi;
        gi.initializeWith(5);
        assert(gi.hasNoTombstoneAndGrowthLeft());
        gi.overwriteFullAsTombstone();
        assert(!gi.hasNoTombstoneAndGrowthLeft());
        gi.initializeWith(0);
        assert(!gi.hasNoTombstoneAndGrowthLeft());
        gi.overwriteFullAsTombstone();
        assert(!gi.hasNoTombstoneAndGrowthLeft());
        gi.initializeWith(5); // after re-initialization, no deleted
        assert(gi.hasNoTombstoneAndGrowthLeft);
    }
    // hasNoGrowthLeftAndNoTombstone
    {
        GrowthInfo gi;
        gi.initializeWith(1);
        assert(!gi.hasNoGrowthLeftAndNoTombstone());
        gi.overwriteEmptyAsFull();
        assert(gi.hasNoGrowthLeftAndNoTombstone());
        gi.overwriteFullAsTombstone();
        assert(!gi.hasNoGrowthLeftAndNoTombstone());
        gi.overwriteFullAsEmpty();
        assert(!gi.hasNoGrowthLeftAndNoTombstone());
        gi.initializeWith(0);
        assert(gi.hasNoGrowthLeftAndNoTombstone());
        gi.overwriteFullAsEmpty();
        assert(!gi.hasNoGrowthLeftAndNoTombstone());
    }
    // overwriteFullAsEmpty
    {
        GrowthInfo gi;
        gi.initializeWith(5);
        gi.overwriteFullAsEmpty();
        assert(gi.growthLeft == 6);
        gi.overwriteFullAsTombstone();
        assert(gi.growthLeft == 6);
        gi.overwriteFullAsEmpty();
        assert(gi.growthLeft == 7);
        assert(!gi.hasNoTombstone());
    }
    // overwriteEmptyAsFull
    {
        GrowthInfo gi;
        gi.initializeWith(5);
        gi.overwriteEmptyAsFull();
        assert(gi.growthLeft == 4);
        gi.overwriteFullAsTombstone();
        assert(gi.growthLeft == 4);
        gi.overwriteEmptyAsFull();
        assert(gi.growthLeft == 3);
        assert(!gi.hasNoTombstone());
    }
    // overwriteControlAsFull
    {
        GrowthInfo gi;
        gi.initializeWith(5);
        gi.overwriteControlAsFull(Control.Empty);
        assert(gi.growthLeft == 4);
        gi.overwriteControlAsFull(Control.Tombstone);
        assert(gi.growthLeft == 4);
        gi.overwriteFullAsTombstone();
        gi.overwriteControlAsFull(Control.Tombstone);
        assert(!gi.hasNoTombstoneAndGrowthLeft());
        assert(!gi.hasNoTombstone());
    }
}

/**
 * Triangular based Probe sequence.
 *
 * Use `Group.Width` to ensure each probing step doesn't overlap groups.
 */
struct ProbeSeq
{
  private:
    size_t _index, _mask, _offset;

  public nothrow @safe:
    this(H1 h, size_t m)
    {
        _mask = m;
        _offset = h & m;
    }

    size_t index() const { return _index; }
    size_t offset() const { return _offset; }
    size_t offset(size_t i) const { return (_offset + i) & _mask; }

    void next()
    {
        _index += Group.Width;
        _offset += _index;
        _offset &= _mask;
    }
}

/// Construct ProbeSeq with calculated H1 hash and capacity
ProbeSeq probe(size_t capacity, size_t hash) nothrow @safe
{
    return ProbeSeq(calcH1(hash), capacity);
}

unittest
{
    // ProbeSeq functions
    import std.algorithm : equal;
    import std.range : generate, take;

    ProbeSeq seq = ProbeSeq(0, 127);
    size_t genOffset() {
        size_t res = seq.offset;
        seq.next();
        return res;
    }

    static if (Group.Width == 16)
        int[] ans = [0, 16, 48, 96, 32, 112, 80, 64];
    else
        int[] ans = [0, 8, 24, 48, 80, 120, 40, 96];
    
    assert(equal(generate(&genOffset).take(8), ans));
    seq = ProbeSeq(128, 127);
    assert(equal(generate(&genOffset).take(8), ans));
}
