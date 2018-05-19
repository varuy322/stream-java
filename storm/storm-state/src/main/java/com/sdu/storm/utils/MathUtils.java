package com.sdu.storm.utils;

public class MathUtils {


    public static int longToIntWithBitMixing(long in) {
        in = (in ^ (in >>> 30)) * 0xbf58476d1ce4e5b9L;
        in = (in ^ (in >>> 27)) * 0x94d049bb133111ebL;
        in = in ^ (in >>> 31);
        return (int) in;
    }

    /**
     * This function hashes an integer value.
     *
     * <p>It is crucial to use different hash functions to partition data across machines and the internal partitioning of
     * data structures. This hash function is intended for partitioning across machines.
     *
     * @param code The integer to be hashed.
     * @return The non-negative hash code for the integer.
     */
    public static int murmurHash(int code) {
        code *= 0xcc9e2d51;
        code = Integer.rotateLeft(code, 15);
        code *= 0x1b873593;

        code = Integer.rotateLeft(code, 13);
        code = code * 5 + 0xe6546b64;

        code ^= 4;
        code = bitMix(code);

        if (code >= 0) {
            return code;
        }
        else if (code != Integer.MIN_VALUE) {
            return -code;
        }
        else {
            return 0;
        }
    }


    /**
     * Bit-mixing for pseudo-randomization of integers (e.g., to guard against bad hash functions). Implementation is
     * from Murmur's 32 bit finalizer.
     *
     * @param in the input value
     * @return the bit-mixed output value
     */
    public static int bitMix(int in) {
        in ^= in >>> 16;
        in *= 0x85ebca6b;
        in ^= in >>> 13;
        in *= 0xc2b2ae35;
        in ^= in >>> 16;
        return in;
    }

}
