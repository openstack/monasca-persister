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

import com.google.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import monasca.persister.configuration.PersisterConfig;
import monasca.persister.repository.influxdb.InfluxPoint;
import monasca.persister.repository.influxdb.InfluxWrite;

public class InfluxV9RepoWriter {

  private static final Logger logger = LoggerFactory.getLogger(InfluxV9RepoWriter.class);

  private final PersisterConfig config;

  private final String influxName;
  private final String influxUrl;
  private final String influxCreds;
  private final String influxUser;
  private final String influxPass;
  private final String influxRetentionPolicy;

  private final CloseableHttpClient httpClient = HttpClientBuilder.create().build();

  private final String baseAuthHeader;

  private final ObjectMapper objectMapper = new ObjectMapper();


  @Inject
  public InfluxV9RepoWriter(final PersisterConfig config) {

    this.config = config;

    this.influxName = config.getInfluxDBConfiguration().getName();
    this.influxUrl = config.getInfluxDBConfiguration().getUrl() + "/write";
    this.influxUser = config.getInfluxDBConfiguration().getUser();
    this.influxPass = config.getInfluxDBConfiguration().getPassword();
    this.influxCreds = this.influxUser + ":" + this.influxPass;
    this.influxRetentionPolicy = config.getInfluxDBConfiguration().getRetentionPolicy();

    this.baseAuthHeader = "Basic " + new String(Base64.encodeBase64(this.influxCreds.getBytes()));

  }

  protected void write(final InfluxPoint[] influxPointArry) throws Exception {

    HttpPost request = new HttpPost(this.influxUrl);

    request.addHeader("content-type", "application/json");
    request.addHeader("Authorization", this.baseAuthHeader);

    InfluxWrite
        influxWrite =
        new InfluxWrite(this.influxName, this.influxRetentionPolicy, influxPointArry,
                        new HashMap());
    StringEntity params = new StringEntity(this.objectMapper.writeValueAsString(influxWrite));
    request.setEntity(params);

    try {

      logger.debug("Writing {} points to influxdb database {} at {}",
                   influxPointArry.length, this.influxName, this.influxUrl);

      HttpResponse response = this.httpClient.execute(request);

      int rc = response.getStatusLine().getStatusCode();

      if (rc != HttpStatus.SC_OK) {

        HttpEntity entity = response.getEntity();
        String responseString = EntityUtils.toString(entity, "UTF-8");
        logger.error("Failed to write data to Influxdb: {}", String.valueOf(rc));
        logger.error("Http response: {}", responseString);

        throw new Exception(responseString);
      }

      logger.debug("Successfully wrote {} points to influxdb database {} at {}",
                   influxPointArry.length, this.influxName, this.influxUrl);

    } finally {

      request.releaseConnection();

    }
  }
}
