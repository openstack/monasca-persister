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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

public final class Measurement {

  private static final Logger logger = LoggerFactory.getLogger(Measurement.class);

  private final ObjectMapper objectMapper = new ObjectMapper();

  public final long time;
  public final double value;
  public final Map<String, String> valueMeta;


  public Measurement(final long time, final double value,
                     final @Nullable Map<String, String> valueMeta) {

    this.time = time;
    this.value = value;
    this.valueMeta = valueMeta == null ? new HashMap<String, String>() : valueMeta;

  }

  public String getISOFormattedTimeString() {

    DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTime();
    Date date = new Date(this.time);
    DateTime dateTime = new DateTime(date.getTime(), DateTimeZone.UTC);

    return dateFormatter.print(dateTime);
  }

  public long getTime() {
    return time;
  }

  public double getValue() {
    return value;
  }

  public Map<String, String> getValueMeta() {
    return valueMeta;
  }

  public String getValueMetaJSONString() {

    if (!this.valueMeta.isEmpty()) {

      try {

        return objectMapper.writeValueAsString(this.valueMeta);

      } catch (JsonProcessingException e) {

        logger.error("Failed to serialize value meta {}", this.valueMeta, e);

      }

    }

    return null;

  }

}
