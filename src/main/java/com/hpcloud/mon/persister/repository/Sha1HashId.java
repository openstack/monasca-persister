package com.hpcloud.mon.persister.repository;

import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;

public class Sha1HashId {
    private final byte[] sha1Hash;

    public Sha1HashId(byte[] sha1Hash) {
        this.sha1Hash = sha1Hash;
    }

    @Override
    public String toString() {
        return "Sha1HashId{" +
                "sha1Hash=" + Hex.encodeHexString(sha1Hash) +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Sha1HashId)) return false;

        Sha1HashId that = (Sha1HashId) o;

        if (!Arrays.equals(sha1Hash, that.sha1Hash)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(sha1Hash);
    }

    public byte[] getSha1Hash() {
        return sha1Hash;
    }
}
