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
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  private final CloseableHttpClient httpClient;

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

    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(config.getInfluxDBConfiguration().getMaxHttpConnections());

    if (config.getInfluxDBConfiguration().getGzip()) {

      this.httpClient =
          HttpClients.custom().setConnectionManager(cm)
              .addInterceptorFirst(new HttpRequestInterceptor() {

                public void process(final HttpRequest request, final HttpContext context)
                    throws HttpException, IOException {
                  if (!request.containsHeader("Accept-Encoding")) {
                    request.addHeader("Accept-Encoding", "gzip");
                  }
                }
              }).addInterceptorFirst(new HttpResponseInterceptor() {

            public void process(final HttpResponse response, final HttpContext context)
                throws HttpException, IOException {
              HttpEntity entity = response.getEntity();
              if (entity != null) {
                Header ceheader = entity.getContentEncoding();
                if (ceheader != null) {
                  HeaderElement[] codecs = ceheader.getElements();
                  for (int i = 0; i < codecs.length; i++) {
                    if (codecs[i].getName().equalsIgnoreCase("gzip")) {
                      response.setEntity(new GzipDecompressingEntity(response.getEntity()));
                      return;
                    }
                  }
                }
              }
            }

          }).build();

    } else {

      this.httpClient = HttpClients.custom().setConnectionManager(cm).build();

    }

  }

  protected void write(final InfluxPoint[] influxPointArry) throws Exception {

    HttpPost request = new HttpPost(this.influxUrl);

    request.addHeader("Content-Type", "application/json");
    request.addHeader("Authorization", this.baseAuthHeader);

    InfluxWrite
        influxWrite =
        new InfluxWrite(this.influxName, this.influxRetentionPolicy, influxPointArry,
                        new HashMap());

    String json = this.objectMapper.writeValueAsString(influxWrite);

    if (this.config.getInfluxDBConfiguration().getGzip()) {

      HttpEntity
          requestEntity =
          EntityBuilder.create().setText(json).setContentType(ContentType.APPLICATION_JSON)
              .gzipCompress().build();

      request.setEntity(requestEntity);

      request.addHeader("Content-Encoding", "gzip");

    } else {

      StringEntity stringEntity = new StringEntity(json);

      request.setEntity(stringEntity);

    }

    try {

      logger.debug("Writing {} points to influxdb database {} at {}", influxPointArry.length,
                   this.influxName, this.influxUrl);

      HttpResponse response = this.httpClient.execute(request);

      int rc = response.getStatusLine().getStatusCode();

      if (rc != HttpStatus.SC_OK) {

        HttpEntity responseEntity = response.getEntity();
        String responseString = EntityUtils.toString(responseEntity, "UTF-8");
        logger.error("Failed to write data to influx database {} at {}: {}", this.influxName,
                     this.influxUrl, String.valueOf(rc));
        logger.error("Http response: {}", responseString);

        throw new Exception(rc + ":" + responseString);
      }

      logger
          .debug("Successfully wrote {} points to influx database {} at {}", influxPointArry.length,
                 this.influxName, this.influxUrl);

    } finally {

      request.releaseConnection();

    }
  }
}
