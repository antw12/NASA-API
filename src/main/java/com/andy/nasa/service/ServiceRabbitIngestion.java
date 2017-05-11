package com.andy.nasa.service;

import com.andy.nasa.Event.DatabaseHandler;
import com.rabbitmq.client.*;
import io.dropwizard.lifecycle.Managed;

import java.io.IOException;


/**
 * This class subscribes to rabbit and consumes the messages
 * that has Nasa entries and then write to the ES DB
 * Created by awaldman on 5/10/17.
 */
public class ServiceRabbitIngestion implements Managed {

    private final DatabaseHandler databaseHandler;
    private Connection connection;
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

    Consumer consumer = new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
                                   AMQP.BasicProperties properties, byte[] body) throws IOException {
            try {
//                String message = new String(body, "UTF-8");
                databaseHandler.writeToDB(new String(body, "UTF-8"));
//                System.out.println(message);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    };

    @Override
    public void start() throws Exception {
        String queueName = "nasa-queue";
        channel.basicConsume(queueName, true, consumer);
    }

    @Override
    public void stop() throws Exception {
//        System.out.println("finished");
        connection.close();
    }
}