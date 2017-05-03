package com.andy.nasa.configuration;

import com.andy.nasa.configuration.configs.ElasticSearchConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

/**
 * Created by awaldman on 4/18/17.
 */
public class NasaConfig extends Configuration{
    private final ElasticSearchConfig elasticSearchConfig;
    /**
     * This is the constructor for Service configuration
     */
    @JsonCreator
    private NasaConfig(@JsonProperty("elasticsearch") ElasticSearchConfig elasticSearchConfig) {
        this.elasticSearchConfig = elasticSearchConfig;
    }

    public ElasticSearchConfig getElasticSearchConfig(){
        return elasticSearchConfig;
    }

}
