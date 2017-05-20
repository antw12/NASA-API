package com.andy.nasa.service;

import com.andy.nasa.event.DatabaseHandler;
import com.rabbitmq.client.*;
import io.dropwizard.lifecycle.Managed;

import java.io.IOException;


/**
 * This class subscribes to rabbit and consumes the messages
 * that has Nasa entries and then write to the ES DB
 * Created by awaldman on 5/10/17.
 */
public class ServiceRabbitIngestion implements Managed {

    //Used to write to the DB on consuming a message
    private final DatabaseHandler databaseHandler;

    //connection to rabbit
    private Connection connection;

    //channel of rabbit used to consume
    private Channel channel;

    /**
     * This constructs the class that ingests messages
     * from rabbit
     * @param
     */
    public ServiceRabbitIngestion(DatabaseHandler databaseHandler, Connection connection) throws IOException {
        this.databaseHandler = databaseHandler;
        this.connection = connection;
        channel  = connection.createChannel();
    }

    /**
     * This is a consumer which is prepared for consuming a message
     */
    private Consumer consumer = new DefaultConsumer(channel) {
        // overriding the consuming of a message from rabbit
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) throws IOException {
            try {
                // write the entry to the db
                databaseHandler.writeToDB(new String(body, "UTF-8"));
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    };

    /**
     * This is the method invoked whilst the application
     * is still alive
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        String queueName = "nasa-queue";
        channel.basicConsume(queueName, true, consumer);
    }

    /**
     * This closed the connection to rabbit
     * on closing the application
     * @throws Exception
     */
    @Override
    public void stop() throws Exception {
        connection.close();
    }
}