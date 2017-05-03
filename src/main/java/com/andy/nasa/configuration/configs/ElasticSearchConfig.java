package com.andy.nasa.configuration.configs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class is the builder for elastic search configuration with all the configuration defined
 * Created by awaldman on 4/19/17.
 */

public class ElasticSearchConfig {

    private final String host;
    private final Integer port;

    @JsonCreator
    private ElasticSearchConfig(
            @JsonProperty("host") String host,
            @JsonProperty("port") Integer port ) {
        this.host = host;
        this.port = port;
    }

    /**
     * gets the host
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * gets the port
     * @return nodes
     */
    public Integer getPort() {
        return port;
    }
}
