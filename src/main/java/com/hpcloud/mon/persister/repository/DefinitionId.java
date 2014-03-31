package com.hpcloud.mon.persister.repository;

import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;

public class DefinitionId {
    private final byte[] definitionIdHash;

    public DefinitionId(byte[] definitionIdHash) {
        this.definitionIdHash = definitionIdHash;
    }

    @Override
    public String toString() {
        return "DefinitionId{" +
                "definitionIdHash=" + Hex.encodeHexString(definitionIdHash) +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DefinitionId)) return false;

        DefinitionId that = (DefinitionId) o;

        if (!Arrays.equals(definitionIdHash, that.definitionIdHash)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(definitionIdHash);
    }

    public byte[] getDefinitionIdHash() {
        return definitionIdHash;
    }
}
