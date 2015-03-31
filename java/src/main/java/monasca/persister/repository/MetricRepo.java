/*
 * Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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

import java.util.Map;

public interface MetricRepo {
  void addMetricToBatch(Sha1HashId defDimsId, String timeStamp, double value, Map<String, String> valueMeta);

  void addDefinitionToBatch(Sha1HashId defId, String name, String tenantId, String region);

  void addDimensionsToBatch(Sha1HashId dimSetId, Map<String, String> dimMap);

  void addDefinitionDimensionToBatch(Sha1HashId defDimsId, Sha1HashId defId, Sha1HashId dimId);

  void flush();
}
