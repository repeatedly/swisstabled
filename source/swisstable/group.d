// Written in the D programming language.

/**
Group and related families for Map

Copyright: Copyright 2024- Masahiro Nakagawa
Authros:   Masahiro Nakagawa
License:   $(HTTP www.apache.org/licenses/LICENSE-2.0, Apache License Version 2.0)
 */
module swisstable.group;

import core.bitop : bsr, bsf, bswap;

nothrow pure @safe @nogc
{

alias H1 = ulong;
alias H2 = ubyte;

/// Extract H1 portion from given hash. This value is used for slot index.
pragma(inline, true) H1 calcH1(size_t h) { return (h & 0xFFFF_FFFF_FFFF_FF80) >> 7; }
/// Extract H2 portion from given hash. This value is used for Control byte.
pragma(inline, true) H2 calcH2(size_t h) { return h & 0x7F; }

enum Control : byte
{
    Empty     = -128,
    Tombstone = -2,  // kDeleted in original implementation
    Sentinel  = -1
}

// Helpers for Control state check
pragma(inline, true) bool isEmpty(Control c) { return c == Control.Empty; }
pragma(inline, true) bool isFull(Control c) { return c >= 0; }
pragma(inline, true) bool isTombstone(Control c) { return c == Control.Tombstone; }
pragma(inline, true) bool isEmptyOrTombstone(Control c) { return c < Control.Sentinel; }

// TODO: Use ldc.intrinsics in LDC.
pragma(inline, true) uint countTrailingZeros(T)(T v)
{
     return v ? cast(uint)bsf(v) : cast(uint)(8 * T.sizeof);
}

unittest
{
    assert(countTrailingZeros(0U) == 32);
    assert(countTrailingZeros(0UL) == 64);
    assert(countTrailingZeros(0b0010) == 1);
    assert(countTrailingZeros(0b01000000) == 6);
}

pragma(inline, true) uint countLeadingZeros(T)(T v)
{
    static if (is(T == ubyte)) {
        return v ? 7 - bsr(v) : 8;
    } else static if (is(T == ushort)) {
        return v ? 15 - bsr(v) : 16;
    } else static if (is(T == uint)) {
        return v ? 31 - bsr(v) : 32;
    } else {
        return v ? 63 - bsr(v) : 64;
    }
}

unittest
{
    assert(countLeadingZeros(0U) == 32);
    assert(countLeadingZeros(0UL) == 64);
    assert(countLeadingZeros(cast(ubyte)0b01000000) == 1);
    assert(countLeadingZeros(0x0080_0000U) == 8);
}

} // nothrow pure @safe @nogc

Control* emptyGroup() nothrow @trusted
{
    static Control zero() { return cast(Control)0; }
    static immutable Control[32] EmptyGroup = [
        zero, zero, zero, zero, zero, zero, zero, zero, zero, zero, zero, zero, zero, zero, zero, zero,
        Control.Sentinel, Control.Empty, Control.Empty, Control.Empty, Control.Empty, Control.Empty, Control.Empty, Control.Empty,
        Control.Empty, Control.Empty, Control.Empty, Control.Empty, Control.Empty, Control.Empty, Control.Empty, Control.Empty
    ];

    return cast(Control*)EmptyGroup.ptr + 16;
}

private enum ulong LSBs = 0x0101010101010101UL;
private enum ulong MSBs = 0x8080808080808080UL;

/**
 * Iterable Mask for bits operation. This object is used in Map's lookup operation with Group.
 *
 * Bits and Shift are depend on Group implementation(SSE2 / Porable).
 */
struct BitMask(T, int Bits, int Shift = 0)
{
  private:
    T _mask;

  public nothrow @safe:
    this(T mask) { _mask = mask; }
    bool opCast(T : bool)() const { return _mask != 0; }
    bool empty() const { return _mask == 0; }
    uint front() const { return (countTrailingZeros(_mask) >> Shift); }
    void popFront() { _mask &= (_mask - 1); }
    uint highestBitSet() const { return cast(uint)(bsr(_mask) >> Shift); }
    uint leadingZeros() const
    {
        enum int extraBits = cast(int)((T.sizeof * 8) - (Bits << Shift));
        return cast(uint)(countLeadingZeros(cast(T)(_mask << extraBits))) >> Shift;
    }
    alias lowestBitSet = front;
    alias trailingZeros = front;
}

unittest
{
    import std.algorithm : equal;

    alias BitMaskUbyte = BitMask!(ubyte, 8);
    assert(equal(BitMaskUbyte(0x1), [0]));
    assert(equal(BitMaskUbyte(0x5), [0, 2]));
    assert(equal(BitMaskUbyte(0x55), [0, 2, 4, 6]));
    assert(equal(BitMaskUbyte(0xaa), [1, 3, 5, 7]));

    alias BitMaskUlong = BitMask!(ulong, 8, 3);
    assert(equal(BitMaskUlong(0x0000000080800000UL), [2, 3]));
    assert(equal(BitMaskUlong(MSBs), [0, 1, 2, 3, 4, 5, 6, 7]));
    assert(equal(BitMaskUlong(0x8000808080008000UL), [1, 3, 4, 5, 7]));
}

version (D_SIMD)
{

/**
 * Process a group of control bytes using SIMD/SSE2
 */
struct GroupSSE2
{
    import core.simd;

    enum size_t Width = 16;
    alias BitMaskType = BitMask!(ushort, Width);

  private:
    byte16 ctrl;

  public nothrow @trusted:
    this(in Control* c)
    {
        ctrl = loadUnaligned!(byte16)(cast(const byte16*)c);
    }

    /// Returns: a BitMask representing the positions of `hash` matched slots
    pragma(inline, true) BitMaskType match(in H2 hash) const
    {
        auto m = mm_set1_epi8(hash);
        return BitMaskType(cast(ushort)mm_movemask_epi8(cast(byte16)__simd(XMM.PCMPEQB, m, ctrl)));
    }

    /// Returns: a BitMask representing the positions of empty slots
    pragma(inline, true) BitMaskType maskEmpty() const
    {
        return BitMaskType(cast(ushort)mm_movemask_epi8(cast(byte16)__simd(XMM.PSIGNB, ctrl, ctrl)));
    }

    /// Returns: a BitMask representing the positions of full slots
    BitMaskType maskFull() const
    {
        return BitMaskType(cast(ushort)mm_movemask_epi8(ctrl) ^ 0xffff);
    }

    /// Returns: a BitMask representing the positions of non-full slots
    BitMaskType maskNonFull() const
    {
        return BitMaskType(cast(ushort)mm_movemask_epi8(ctrl));
    }

    /// Returns: a BitMask representing the positions of empty or tombstone slots
    BitMaskType maskEmptyOrTombstone() const
    {
        auto m = mm_set1_epi8(Control.Sentinel);
        return BitMaskType(cast(ushort)mm_movemask_epi8(cast(byte16)__simd(XMM.PCMPGTB, m, ctrl)));
    }

    /// Returns: the number of trailing empty or tombstone slots in the group.
    uint countLeadingEmptyOrTombstone() const
    {
        auto m = mm_set1_epi8(Control.Sentinel);
        return countTrailingZeros(cast(uint)mm_movemask_epi8(cast(byte16)__simd(XMM.PCMPGTB, m, ctrl)) + 1);
    }

    /// Convert empty/tombstone to empty and full to tombstone in the group..
    void convertSpecialToEmptyAndFullToTombstone(Control* dst) const
    {
        const msbs = mm_set1_epi8(-128);
        const x126 = mm_set1_epi8(126);
        auto res = cast(byte16)__simd(XMM.POR, __simd(XMM.PSHUFB, x126, ctrl), msbs);
        storeUnaligned!(byte16)(cast(byte16*)dst, res);
    }

  private:
    pragma(inline, true) byte16 mm_set1_epi8(in byte v) pure const
    {
        return byte16(v);
    }

    // core.simd doesn't support XMM.PMOVMSKB
    pragma(inline, true) int mm_movemask_epi8(in byte16 v) pure const
    {
        asm nothrow pure
        {
            naked;
            pmovmskb EAX, XMM0;
            ret;
        }
    }
}

unittest
{
    import std.algorithm : equal;

    Control[16] group = cast(Control[16])[
        Control.Empty,  1, Control.Tombstone, 3, Control.Empty, 5, Control.Sentinel, 7,
        7, 5, 3, 1, 1, 1, 1, 1];

    auto g = GroupSSE2(group.ptr);
    // match
    assert(g.match(0).empty);
    assert(equal(g.match(1), [1, 11, 12, 13, 14, 15]));
    assert(equal(g.match(3), [3, 10]));
    assert(equal(g.match(5), [5, 9]));
    assert(equal(g.match(7), [7, 8]));
    // maskEmpty
    assert(g.maskEmpty.lowestBitSet == 0);
    assert(g.maskEmpty.highestBitSet == 4);
    // maskEmptyOrTombstone
    assert(g.maskEmptyOrTombstone.lowestBitSet == 0);
    assert(g.maskEmptyOrTombstone.highestBitSet == 4);
}

unittest
{
    // GroupSSE2 maskFull / maskNonFull
    import std.algorithm : equal;

    Control[16] group = cast(Control[16])[
        Control.Empty,  1, Control.Tombstone, 3, Control.Empty, 5, Control.Sentinel, 7,
        7, 5, Control.Tombstone, 1, 1, Control.Sentinel, Control.Empty, 1];

    auto g = GroupSSE2(group.ptr);
    assert(equal(g.maskFull, [1, 3, 5, 7, 8, 9, 11, 12, 15]));
    assert(equal(g.maskNonFull, [0, 2, 4, 6, 10, 13, 14]));
}

} // D_SIMD

/**
 * Process a group of control bytes using portable bit operation
 */
struct GroupPortable
{
    enum size_t Width = 8;

    alias BitMaskType = BitMask!(ulong, Width, 3);

  private:
    ulong ctrl;

  public nothrow @safe:
    this(in Control* ctrl)
    {
        this.ctrl = load64bits(ctrl);
    }

    pragma(inline, true) BitMaskType match(H2 hash) const
    {
        auto x = ctrl ^ (LSBs * hash);
        return BitMaskType((x - LSBs) & ~x & MSBs);
    }

    pragma(inline, true) BitMaskType maskEmpty() const
    {
        return BitMaskType((ctrl & ~(ctrl << 6)) & MSBs);
    }

    BitMaskType maskFull() const
    {
        return BitMaskType((ctrl ^ MSBs) & MSBs);
    }

    BitMaskType maskNonFull() const
    {
        return BitMaskType(ctrl & MSBs);
    }

    BitMaskType maskEmptyOrTombstone() const
    {
        return BitMaskType((ctrl & ~(ctrl << 7)) & MSBs);
    }

    uint countLeadingEmptyOrTombstone() const
    {
        return cast(uint)(countTrailingZeros((ctrl | ~(ctrl >> 7)) & LSBs) >> 3);
    }

    void convertSpecialToEmptyAndFullToTombstone(Control* dst) const
    {
        auto x = ctrl & MSBs;
        store64bits(dst, (~x + (x >> 7)) & ~LSBs);
    }
}

version (D_SIMD)
    alias Group = GroupSSE2;
else
    alias Group = GroupPortable;

unittest
{
    import std.algorithm : equal;

    Control[8] group = cast(Control[8])[Control.Empty,  1, 2, Control.Tombstone, 2, 1, Control.Sentinel, 1];

    // match
    assert(GroupPortable(group.ptr).match(0).empty);
    assert(equal(GroupPortable(group.ptr).match(1), [1, 5, 7]));
    assert(equal(GroupPortable(group.ptr).match(2), [2, 4]));

    // maskEmpty
    assert(GroupPortable(group.ptr).maskEmpty().lowestBitSet == 0);
    assert(GroupPortable(group.ptr).maskEmpty().highestBitSet == 0);

    // maskEmptyOrTombstone
    assert(GroupPortable(group.ptr).maskEmptyOrTombstone().lowestBitSet == 0);
    assert(GroupPortable(group.ptr).maskEmptyOrTombstone().highestBitSet == 3);

    Control[8] group2 = cast(Control[8])[Control.Empty,  1, Control.Empty, Control.Tombstone, 2, Control.Sentinel, Control.Sentinel, 1];

    // maskFull / maskNonFull
    assert(equal(GroupPortable(group2.ptr).maskFull(), [1, 4, 7]));
    assert(equal(GroupPortable(group2.ptr).maskNonFull(), [0, 2, 3, 5, 6]));
}

unittest
{
    import std.meta;

    version (D_SIMD)
        alias TestTypes = AliasSeq!(GroupPortable, GroupSSE2);
    else
        alias TestTypes = AliasSeq!(GroupPortable);

    // Group's convertSpecialToEmptyAndFullToTombstone test for dropDeletesWithoutResize()
    foreach (TestGroup; TestTypes) {
        enum size_t Cap = 63;
        enum size_t GW = TestGroup.Width;

        Control[] ctrl = new Control[](Cap + 1 + GW);
        ctrl[Cap] = Control.Sentinel;
        Control[] pattern = cast(Control[])[Control.Empty, 2, Control.Tombstone, 2, Control.Empty, 1, Control.Tombstone];
        for (size_t i; i != Cap; i++) {
            ctrl[i] = pattern[i % pattern.length];
            if (i < GW - 1)
                ctrl[i + Cap + 1] = pattern[i % pattern.length];
        }
        Control* ctrlPtr = ctrl.ptr;
        for (Control* pos = ctrlPtr; pos < ctrlPtr + Cap; pos += GW)
            TestGroup(pos).convertSpecialToEmptyAndFullToTombstone(pos);
        memcpy(ctrlPtr + Cap + 1, ctrlPtr, GW - 1);
        ctrl[Cap] = Control.Sentinel;

        for (size_t i; i < Cap + GW; i++) {
            Control expected = pattern[i % (Cap + 1) % pattern.length];
            if (i == Cap)
                expected = Control.Sentinel;
            if (expected == Control.Tombstone)
                expected = Control.Empty;
            if (expected.isFull)
                expected = Control.Tombstone;
            assert(ctrl[i] == expected);
        }
    }
}

unittest
{
    import std.meta :  AliasSeq;
    import std.array : array;
    import std.range : repeat;

    version (D_SIMD)
        alias TestTypes = AliasSeq!(GroupPortable, GroupSSE2);
    else
        alias TestTypes = AliasSeq!(GroupPortable);

    // Group's countLeadingEmptyOrTombstone test
    foreach (TestGroup; TestTypes) {
        Control[] empties = [Control.Empty, Control.Tombstone];
        Control[] fulls = cast(Control[])[0, 1, 2, 3, 5, 9, 126, Control.Sentinel];

        foreach (empty; empties) {
            Control[] e = empty.repeat(TestGroup.Width).array;
            assert(TestGroup.Width == TestGroup(e.ptr).countLeadingEmptyOrTombstone());

            foreach (full; fulls) {
                foreach (i; 0..TestGroup.Width) {
                    Control[] f = empty.repeat(TestGroup.Width).array;
                    f[i] = full;
                    assert(i == TestGroup(f.ptr).countLeadingEmptyOrTombstone);
                }
                Control[] f = empty.repeat(TestGroup.Width).array;
                f[TestGroup.Width * 2 / 3] = full;
                f[TestGroup.Width / 2] = full;
                assert((TestGroup.Width / 2) == TestGroup(f.ptr).countLeadingEmptyOrTombstone);
            }
        }
    }
}

import core.stdc.string : memcpy;

nothrow @trusted @nogc:

ulong load64bits(in Control* src)
{
    ulong r;
    memcpy(&r, src, ulong.sizeof);

    version (LittleEndian)
    {
        return r;
    }
    else
    {
        return bswap(r);        
    }
}

void store64bits(Control* dst, ulong src)
{
    version (LittleEndian)
    {
        memcpy(dst, &src, ulong.sizeof);
    }
    else
    {
        ulong v = bswap(src);
        memcpy(dst, &v, ulong.sizeof);
    }
}
