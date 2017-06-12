package com.andy.nasa.configuration.configs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class creates a rabbit config so that,
 * it can be used to connect to rabbit
 * Created by awaldman on 5/8/17.
 */
public class RabbitConfig {
    private int port;
    private String host;
    private String user;
    private String pass;

    @JsonCreator
    private RabbitConfig(@JsonProperty("port") int port,
                         @JsonProperty("host") String host,
                         @JsonProperty("user") String user,
                         @JsonProperty("pass") String pass){
        this.port = port;
        this.host = host;
        this.user = user;
        this.pass = pass;
    }

    /**
     * This method is to set the port of the Rabbit Object
     * @param port
     */
    public void setPort(int port){
        this.port = port;
    }

    /**
     * This method returns the port of the Rabbit object
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * This method is to set the host of the Rabbit Object
     * @param host
     */
    public void setHost(String host){
        this.host = host;
    }

    /**
     * This method returns the host of the Rabbit object
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * This method is to set the User of the Rabbit Object
     * @param user
     */
    public void setUser(String user){
        this.user = user;
    }

    /**
     * This method returns the username of the Rabbit object
     * @return username
     */
    public String getUser() {
        return user;
    }

    /**
     * This method is to set the pass of the Rabbit Object
     * @param pass
     */
    public void setPass(String pass){
        this.pass = pass;
    }

    /**
     * This method sets the username of the Rabbit object
     * @return password
     */
    public String getPass() {
        return pass;
    }
}
