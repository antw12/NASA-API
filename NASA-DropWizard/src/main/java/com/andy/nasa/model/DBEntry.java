package com.andy.nasa.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

/**
 * This is to create a object holding the entry data
 * Created by awaldman on 4/18/17.
 */
@Value.Immutable
@Value.Style(depluralize = true)
@JsonSerialize(as = ImmutableDBEntry.class)
@JsonDeserialize(as = ImmutableDBEntry.class)
public interface DBEntry {
    /**
     * creates an immutable object for entryID
     * @return entryID
     */
    @Nullable
    @Value.Parameter
    String entryID();

    /**
     * creates an immutable object for responseCode
     * @return responseCode
     */
    @Value.Parameter
    Integer responseCode();

    /**
     * creates an immutable object for restfulAPI
     * @return restfulAPI
     */
    @Value.Parameter
    String restfulAPI();

    /**
     * creates an immutable object for payloadSize
     * @return payloadSize
     */
    @Value.Parameter
    Integer payloadSize();

    /**
     * creates an immutable object for resourceAccessed
     * @return resourceAccessed
     */
    @Value.Parameter
    String resourceAccessed();

    /**
     * creates an immutable object for restfulAPI
     * @return restfulAPI
     */
    @Nullable
    @Value.Parameter
    String username();

    /**
     * creates an immutable object for datetime
     * @return datetime
     */
    @Nullable
    @Value.Parameter
    DateTime datetime();

    /**
     * creates an immutable object for fileExtension
     * @return restfulAPI
     */
    @Nullable
    @Value.Parameter
    String fileExtension();

    /**
     * creates an immutable object for client
     * @return client
     */
    @Value.Parameter
    String client();

}
