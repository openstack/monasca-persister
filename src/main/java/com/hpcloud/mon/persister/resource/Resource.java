package com.hpcloud.mon.persister.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/resource")
@Produces(MediaType.APPLICATION_JSON)
public class Resource {

    public Resource() {

    }

    @GET
    public PlaceHolder getResource() {
        return new PlaceHolder("placeholder");
    }
}
