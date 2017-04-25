package com.andy.nasa.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.joda.time.DateTime;

/**
 * This is to create a object holding the entry data
 * Created by awaldman on 4/18/17.
 */
@Value.Immutable
@Value.Style(depluralize = true)
@JsonSerialize(as = ImmutableDBEntry.class)
@JsonDeserialize(as = ImmutableDBEntry.class)
public interface DBEntry {

	@Value.Parameter
	String entryID();

	@Value.Parameter
	Integer responseCode();

	@Value.Parameter
	String restfulAPI();

	@Value.Parameter
	Integer payloadSize();

	@Value.Parameter
	String resourceAccessed();

	@Value.Parameter
	String username();

	@Value.Parameter
	DateTime datetime();

}
