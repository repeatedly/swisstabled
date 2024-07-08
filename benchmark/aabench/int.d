/**
 * Benchmark int hashing.
 *
 * Copyright: Copyright Martin Nowak 2011 - 2015.
 * License:   $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost License 1.0)
 * Authors:   Martin Nowak
 */
import std.random;
import swisstable.map;

void main(string[] args)
{
    auto rnd = Xorshift32(33);

    auto aa = Map!(int, int)();
    //int[int] aa;
    foreach (_; 0 .. 10)
        foreach (__; 0 .. 100_000)
            ++aa[uniform(0, 100_000, rnd)];

    if (aa.length != 99998)
        assert(0);
}
