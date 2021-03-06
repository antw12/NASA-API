package com.andy.nasa.configuration;

import com.andy.nasa.configuration.configs.ElasticSearchConfig;
import com.andy.nasa.configuration.configs.RabbitConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

/**
 * This sets up the configuration for all the different technologies used in the dropwizard application
 * Created by awaldman on 4/18/17.
 */
public class NasaConfig extends Configuration{

    private final ElasticSearchConfig elasticSearchConfig;
    private final RabbitConfig rabbitConfig;

    /**
     * This is the constructor for Service configuration
     */
    @JsonCreator
    private NasaConfig(@JsonProperty("elasticsearch") ElasticSearchConfig elasticSearchConfig,
                       @JsonProperty("rabbit") RabbitConfig rabbitConfig) {
        this.elasticSearchConfig = elasticSearchConfig;
        this.rabbitConfig = rabbitConfig;
    }

    /**
     * method returns the elastic search configuration
     * @return elasticSearchConfig
     */
    public ElasticSearchConfig getElasticSearchConfig(){
        return elasticSearchConfig;
    }


    /**
     * Returns the rabbit config
     * @return rabbitConfig
     */
    public RabbitConfig getRabbitConfig() { return rabbitConfig; }

}
