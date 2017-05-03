package com.andy.nasa.elasticsearch;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

/**
 * Create health check for elastic search
 * Created by awaldman on 4/19/17.
 */
public class ESHealthCheck extends HealthCheck {
    private final RestClient restClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ESHealthCheck(RestClient restClient) {
        this.restClient = restClient;
    }

    @Override
    protected Result check() throws Exception {

        final Response response = restClient.performRequest(
                "GET",
                "_cluster/health"
        );

        String body = EntityUtils.toString(response.getEntity());
        JsonNode jsonNode = objectMapper.readTree(body);
        String status = jsonNode.path("status").asText();

        if (StringUtils.equalsAny(status, "red", "yellow")) {
            return Result.unhealthy("Last status: %s", status);
        } else {
            return Result.healthy("Last status: %s", status);
        }
    }
}
