/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
 *
 * Copyright (c) 2017 SUSE LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monasca.persister.repository;

import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Sha1HashId {
  private final byte[] sha1Hash;

  private final String hex;

  public Sha1HashId(byte[] sha1Hash) {
    this.sha1Hash = sha1Hash;
    hex = Hex.encodeHexString(sha1Hash);
  }

  @Override
  public String toString() {
    return "Sha1HashId{" + "sha1Hash=" + hex + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Sha1HashId))
      return false;

    Sha1HashId that = (Sha1HashId) o;

    if (!Arrays.equals(sha1Hash, that.sha1Hash))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(sha1Hash);
  }

  public byte[] getSha1Hash() {
    return sha1Hash;
  }

  public ByteBuffer getSha1HashByteBuffer() {
    return ByteBuffer.wrap(sha1Hash);
  }

  public String toHexString() {
    return hex;
  }
}
