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

package monasca.persister.repository.influxdb;

import monasca.persister.configuration.PersisterConfig;
import monasca.persister.repository.RepoException;

import com.google.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
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

public class InfluxV9RepoWriter {

  private static final Logger logger = LoggerFactory.getLogger(InfluxV9RepoWriter.class);

  private final String influxName;
  private final String influxUrl;
  private final String influxCreds;
  private final String influxUser;
  private final String influxPass;
  private final String influxRetentionPolicy;
  private final boolean gzip;

  private final CloseableHttpClient httpClient;

  private final String baseAuthHeader;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Inject
  public InfluxV9RepoWriter(final PersisterConfig config) {

    this.influxName = config.getInfluxDBConfiguration().getName();
    this.influxUrl = config.getInfluxDBConfiguration().getUrl() + "/write";
    this.influxUser = config.getInfluxDBConfiguration().getUser();
    this.influxPass = config.getInfluxDBConfiguration().getPassword();
    this.influxCreds = this.influxUser + ":" + this.influxPass;
    this.influxRetentionPolicy = config.getInfluxDBConfiguration().getRetentionPolicy();
    this.gzip = config.getInfluxDBConfiguration().getGzip();

    this.baseAuthHeader = "Basic " + new String(Base64.encodeBase64(this.influxCreds.getBytes()));

    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(config.getInfluxDBConfiguration().getMaxHttpConnections());

    if (this.gzip) {

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

  protected int write(final InfluxPoint[] influxPointArry, String id) throws RepoException {

    HttpPost request = new HttpPost(this.influxUrl);

    request.addHeader("Content-Type", "application/json");
    request.addHeader("Authorization", this.baseAuthHeader);

    InfluxWrite
        influxWrite =
        new InfluxWrite(this.influxName, this.influxRetentionPolicy, influxPointArry,
                        new HashMap<String, String>());

    String jsonBody = getJsonBody(influxWrite);

    if (this.gzip) {

      logger.debug("[{}]: gzip set to true. sending gzip msg", id);

      HttpEntity
          requestEntity =
          EntityBuilder
              .create()
              .setText(jsonBody)
              .setContentType(ContentType.APPLICATION_JSON)
              .setContentEncoding("UTF-8")
              .gzipCompress()
              .build();

      request.setEntity(requestEntity);

      request.addHeader("Content-Encoding", "gzip");

    } else {

      logger.debug("[{}]: gzip set to false. sending non-gzip msg", id);

      StringEntity stringEntity = new StringEntity(jsonBody, "UTF-8");

      request.setEntity(stringEntity);

    }

    try {

      logger.debug("[{}]: sending {} points to influxdb {} at {}", id,
                   influxPointArry.length, this.influxName, this.influxUrl);

      HttpResponse response = null;

      try {

        response = this.httpClient.execute(request);

      } catch (IOException e) {

        throw new RepoException("failed to execute http request", e);
      }

      int rc = response.getStatusLine().getStatusCode();

      if (rc != HttpStatus.SC_OK && rc != HttpStatus.SC_NO_CONTENT) {

        logger.error("[{}]: failed to send data to influxdb {} at {}: {}", id,
                     this.influxName, this.influxUrl, String.valueOf(rc));

        HttpEntity responseEntity = response.getEntity();

        String responseString = null;

        try {

          responseString = EntityUtils.toString(responseEntity, "UTF-8");

        } catch (IOException e) {

         throw new RepoException("failed to read http response for non ok return code " + rc, e);

        }

        logger.error("[{}]: http response: {}", id, responseString);

        throw new RepoException("failed to execute http request to influxdb " + rc + " - " + responseString);

      } else {

        logger.debug("[{}]: successfully sent {} points to influxdb {} at {}", id,
                     influxPointArry.length, this.influxName, this.influxUrl);

        return influxPointArry.length;

      }

    } finally {

      request.releaseConnection();

    }
  }

  private String getJsonBody(InfluxWrite influxWrite) throws RepoException {

    String json = null;

    try {

      json = this.objectMapper.writeValueAsString(influxWrite);

    } catch (JsonProcessingException e) {

      throw new RepoException("failed to serialize json", e);
    }

    return json;
  }
}
