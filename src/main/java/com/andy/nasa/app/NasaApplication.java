package com.andy.nasa.app;

import com.andy.nasa.configuration.NasaConfig;
import com.andy.nasa.elasticsearch.ESHealthCheck;
import com.andy.nasa.resource.NasaResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

/**
 * This class sets and ads resources aka apis to drop wizard in addition to creating the drop wizard application
 * Created by awaldman on 4/18/17.
 */
public class NasaApplication extends Application<NasaConfig>{

    public static void main(String[] args) throws Exception {
        //starts drop wizard application
        new NasaApplication().run(args);
    }

    /**
     * This method is from dropwizard application class and is used to set up
     * all the resources necessary for the application to do its job
     * @param nasaConfig
     * @param environment
     * @throws Exception
     */
    @Override
    public void run(NasaConfig nasaConfig, Environment environment) throws Exception {
        RestClient restClient = RestClient
                .builder(
                    new HttpHost(
                            nasaConfig.getElasticSearchConfig().getHost(),
                            nasaConfig.getElasticSearchConfig().getPort(),
                            "http"
                    )
                )
                .build();

        ESHealthCheck esHealthCheck = new ESHealthCheck(restClient);
        environment.healthChecks().register("es health check", esHealthCheck);
        final NasaResource nasaResource = new NasaResource(restClient);
        environment.jersey().register(nasaResource);
    }

    /**
     * This method override is to give the application a name
     * it will appear on a run
     * @return
     */
    @Override
    public String getName() {
        return "NASA - Application";
    }
}