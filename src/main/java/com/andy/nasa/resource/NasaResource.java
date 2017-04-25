package com.andy.nasa.resource;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * This resource class is the API's that can be called with the dropwizard application
 * Created by awaldman on 4/18/17.
 */
@Path("/entry")
@Produces(MediaType.APPLICATION_JSON)
public class NasaResource {

	@POST
	public void sendDate(String entryPayload) {
		// on entry to DB entries will never be the same with this data set due to time
		// however hashing is used to prevent this and so used for practise
		System.out.println(entryPayload);
	}
}
