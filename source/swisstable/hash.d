// Written in the D programming language.

/**
The D port of rapidhash and adapted for SwissTabled

Copyright: Copyright 2024- Masahiro Nakagawa
Authros:   Masahiro Nakagawa
License:   $(HTTP www.apache.org/licenses/LICENSE-2.0, Apache License Version 2.0)
Credits:   Core algorithm is based on C's rapidhash. See $(HTTP github.com/Nicoshev/rapidhash, rapidhash - Very fast, high quality, platform-independent) by Nicolas De Carli
 */
module swisstable.hash;

/*
 * This version affects rapidMul multiplication function.
 *
 * RAPIDHASH_FAST: Normal behavior, max speed.
 * RAPIDHASH_PROTECTED: Extra protection against entropy loss.
 */
version (RAPIDHASH_PROTECTED) {} else { version = RAPIDHASH_FAST; }

// Drop RAPIDHASH_COMPACT support

///  Default seed
enum size_t RapidSeed = 0xbdd89aa982704029;

/**
 * rapidhash seeded hash for string, integer and pointer types in LDC.
 * For other types and DMD, use D's built-in hashOf instead.
 *
 * Params:
 *  key = buffer to be hashed
 *  len = key length in bytes
 *  seed = 64-bit seed used to alter the hash result predictably
 *
 * Returns: a 64-bit hash.
 */
pragma(inline, true) ulong rapidhashOf(K)(const auto ref K key, size_t seed = RapidSeed) nothrow @trusted
{
    version (LDC)
    {
        import std.traits : isSomeString, ForeachType, isIntegral, isPointer;

        static if (isSomeString!K)
            return rapidhashInternal(cast(const(ubyte)*)key.ptr, key.length * (ForeachType!K).sizeof, seed);
        else static if (isIntegral!K)
            return rapidhashInternal(cast(const(ubyte)*)&key, K.sizeof, seed);
        else static if (isPointer!K) {
            size_t k = cast(size_t)key; // Use ptr address, not target value
            return rapidhashInternal(cast(const(ubyte)*)&key, size_t.sizeof, seed);
        } else
            return .hashOf(key, seed); // Fallback to dlang's hashOf for other types
    }
    else
    {
        return .hashOf(key, seed); // DMD can't generate fast code for rapidhash
    }
}

unittest
{
    version (LDC)
    {
        assert(rapidhashOf("test") == 945056766619593);
        assert(rapidhashOf("0123456789abcdefghijklmnopqrstuvwxyz") == 17012243428125851551);
        assert(rapidhashOf(1000U) == 10318066423548077317);
        assert(rapidhashOf(uint.max) == 9968829716010863328);
    }
    else
    {
        assert(rapidhashOf("test") == .hashOf("test", RapidSeed));
        assert(rapidhashOf(1000U) == .hashOf(1000U, RapidSeed));
    }
}

private nothrow @trusted @nogc pure:

/**
 * 64 * 64 -> 128bit multiply function. Calculates 128-bit C = *A * *B.
 *
 * When RAPIDHASH_FAST is defined:
 * Overwrites A contents with C's low 64 bits.
 * Overwrites B contents with C's high 64 bits.
 *
 * When RAPIDHASH_PROTECTED is defined:
 * Xors and overwrites A contents with C's low 64 bits.
 * Xors and overwrites B contents with C's high 64 bits.
 *
 * Params:
 *  A = Address of 64-bit number
 *  B = Address of 64-bit number
 */
pragma(inline, true) void rapidMul(ulong* A, ulong* B)
{
    version (RAPIDHASH_USE_CENT)
    {
        // Cent version is now too slow.
        import core.int128;

        Cent r = {lo:*A};
        r = mul(r, Cent(0, *B));
        version (RAPIDHASH_PROTECTED) {
            *A ^= cast(ulong)r.lo;
            *B ^= cast(ulong)r.hi;
        } else {
            *A = cast(ulong)r.lo;
            *B = cast(ulong)r.hi;
        }
        /*
    } else version(LDC) {
        // From https://forum.dlang.org/post/nydunscuwdcinamqails@forum.dlang.org
        import ldc.intrinsics;
        pragma(LDC_inline_ir)
            R inlineIR(string s,  R,  P...)(P);

        ulong[2] result;
        inlineIR!(`
    %a = zext i64 %0 to i128
    %b = zext i64 %1 to i128
    %c = mul i128 %a, %b
    %d = bitcast [2 x i64]* %2 to i128*
    store i128 %c, i128* %d
    ret void`, void)(*A, *B, &result);

        version (RAPIDHASH_PROTECTED) {
            *A ^= result[0]; *B ^= result[1];
        } else {
            *A = result[0]; *B = result[1];
        }
        */
    }
    else
    {
        ulong ha = *A >> 32, hb = *B >> 32, la = cast(uint)*A, lb = cast(uint)*B, hi = void, lo = void;
        ulong rh = ha * hb, rm0 = ha * lb, rm1 = hb * la, rl = la * lb, t = rl + (rm0 << 32), c = t < rl;
        lo = t + (rm1 << 32);
        c += lo < t;
        hi = rh + (rm0 >> 32) + (rm1 >> 32) + c;
        version (RAPIDHASH_PROTECTED) {
            *A ^= lo;
            *B ^= hi;
        } else {
            *A = lo;
            *B = hi;
        }
    }
}

/**
 * Multiply and xor mix function.
 *
 * Params:
 *  A = 64-bit number
 *  B = 64-bit number
 *
 * Returns: calculates 128-bit C = A * B. 64-bit xor between high and low 64 bits of C.
 */
pragma(inline, true) ulong rapidMix(ulong A, ulong B) { rapidMul(&A, &B); return A ^ B; }

import core.stdc.string : memcpy;
import core.bitop : bswap;

version (LittleEndian)
{
    pragma(inline, true) ulong rapidRead64(const(ubyte)* p) { ulong v = void; memcpy(&v, p, 8); return v; }
    pragma(inline, true) ulong rapidRead32(const(ubyte)* p) { uint  v = void; memcpy(&v, p, 4); return v; }
}
else
{
    pragma(inline, true) ulong rapidRead64(const(ubyte)* p) { ulong v = void; memcpy(&v, p, 8); return bswap(v); }
    pragma(inline, true) ulong rapidRead32(const(ubyte)* p) { uint  v = void; memcpy(&v, p, 4); return bswap(v); }
}

/**
 * Reads and combines 3 bytes of input. Reads and combines 3 bytes from given buffer.
 * Guarantees to read each buffer position at least once.
 *
 * Params:
 *  p = buffer to read from
 *  k = length of p in bytes
 *  
 * Returns: a 64-bit value containing all three bytes read. 
 */
pragma(inline, true) ulong rapidReadSmall(const(ubyte)* p, size_t k)
{
    return (cast(ulong)p[0] << 56) | (cast(ulong)p[k >> 1] << 32) | p[k - 1];
}

/**
 * main routine
 *
 * Params:
 *  key = buffer to be hashed
 *  len = key length in bytes
 *  seed = 64-bit seed used to alter the hash result predictably
 *  secret = triplet of 64-bit secrets used to alter hash result predictably
 *
 * Returns: a 64-bit hash.
 */
pragma(inline, true) ulong rapidhashInternal(const(void)* key, size_t len, ulong seed)
{
    // Default secret parameters
    static immutable ulong[3] secret = [0x2d358dccaa6c78a5UL, 0x8bb84b93962eacc9UL, 0x4b33a62ed433d4a3UL];

    const(ubyte)* p = cast(const(ubyte)*)key;
    seed ^= rapidMix(seed ^ secret[0], secret[1]) ^ len;
    ulong a = void, b = void;
    if (len <= 16) {
        if (len >= 4) {
            const(ubyte)* plast = p + len - 4;
            a = (rapidRead32(p) << 32) | rapidRead32(plast);
            const(ulong) delta = (len & 24) >> (len >> 3);
            b = (rapidRead32(p + delta) << 32) | rapidRead32(plast - delta);
        } else if (len > 0) {
            a = rapidReadSmall(p, len);
            b = 0;
        } else {
            a = b = 0;
        }
    } else {
        size_t i = len; 
        if (i > 48) {
            ulong see1 = seed, see2 = seed;
            while (i >= 96) {
                seed = rapidMix(rapidRead64(p)      ^ secret[0], rapidRead64(p + 8)  ^ seed);
                see1 = rapidMix(rapidRead64(p + 16) ^ secret[1], rapidRead64(p + 24) ^ see1);
                see2 = rapidMix(rapidRead64(p + 32) ^ secret[2], rapidRead64(p + 40) ^ see2);
                seed = rapidMix(rapidRead64(p + 48) ^ secret[0], rapidRead64(p + 56) ^ seed);
                see1 = rapidMix(rapidRead64(p + 64) ^ secret[1], rapidRead64(p + 72) ^ see1);
                see2 = rapidMix(rapidRead64(p + 80) ^ secret[2], rapidRead64(p + 88) ^ see2);
                p += 96;
                i -= 96;
            }
            if (i >= 48) {
                seed = rapidMix(rapidRead64(p)      ^ secret[0], rapidRead64(p + 8)  ^ seed);
                see1 = rapidMix(rapidRead64(p + 16) ^ secret[1], rapidRead64(p + 24) ^ see1);
                see2 = rapidMix(rapidRead64(p + 32) ^ secret[2], rapidRead64(p + 40) ^ see2);
                p += 48;
                i -= 48;
            }
            seed ^= see1 ^ see2;
        }
        if (i > 16) {
            seed = rapidMix(rapidRead64(p) ^ secret[2], rapidRead64(p + 8) ^ seed ^ secret[1]);
            if (i > 32)
                seed = rapidMix(rapidRead64(p + 16) ^ secret[2], rapidRead64(p + 24) ^ seed);
        }
        a = rapidRead64(p + i - 16); b = rapidRead64(p + i - 8);
    }
    a ^= secret[1];
    b ^= seed;
    rapidMul(&a, &b);

    return rapidMix(a ^ secret[0] ^ len, b ^ secret[1]);
}
