package org.apache.hadoop.yarn.server.resourcemanager.webapp.ui3;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;

@Singleton
@Path("/ui3")
public class RMUI3Services {

  private final ResourceManager rm;

  @Inject
  public RMUI3Services(ResourceManager rm) {
    this.rm = rm;
  }

  @GET
  @Produces("text/css")
  @Path("css")
  public Response css() {
    try (
        InputStream inputStream = RMUI3Services.class.getClassLoader().getResourceAsStream("bulma.min.css");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    ) {
      String css = reader.lines().collect(Collectors.joining("\n"));
      return Response.ok(css)
          .header("Cache-Control", "max-age=86400, public") // 1 day
          .header("Pragma", "public")
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to load CSS", e);
    }
  }

  @GET
  @Produces(MediaType.TEXT_HTML)
  public String get() {
    return String.format("""
        <!DOCTYPE html>
         <html>
         <head>
           <title>UI3-POC</title>
           <link rel="stylesheet" href="ui3/css">
         </head>
         <body>
           <h1 class="title">Hello, Hadoop %s!</h1>
         </body>
         </html>""", new ClusterInfo(rm).getHadoopVersionBuiltOn());
  }
}
