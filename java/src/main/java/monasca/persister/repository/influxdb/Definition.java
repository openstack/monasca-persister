/*
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package monasca.persister.repository.influxdb;

public final class Definition {

  private static final int MAX_DEFINITION_NAME_LENGTH = 255;
  private static final int MAX_TENANT_ID_LENGTH = 255;
  private static final int MAX_REGION_LENGTH = 255;

  public final String name;
  public final String tenantId;
  public final String region;

  public Definition(String name, String tenantId, String region) {

    if (name.length() > MAX_DEFINITION_NAME_LENGTH) {
      name = name.substring(0, MAX_DEFINITION_NAME_LENGTH);
    }

    this.name = name;

    if (tenantId.length() > MAX_TENANT_ID_LENGTH) {
      tenantId = tenantId.substring(0, MAX_TENANT_ID_LENGTH);
    }

    this.tenantId = tenantId;

    if (region.length() > MAX_REGION_LENGTH) {
      region = region.substring(0, MAX_REGION_LENGTH);
    }

    this.region = region;
  }


  public String getName() {
    return name;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getRegion() {
    return region;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Definition)) {
      return false;
    }

    Definition that = (Definition) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!tenantId.equals(that.tenantId)) {
      return false;
    }
    return region.equals(that.region);

  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + tenantId.hashCode();
    result = 31 * result + region.hashCode();
    return result;
  }
}
