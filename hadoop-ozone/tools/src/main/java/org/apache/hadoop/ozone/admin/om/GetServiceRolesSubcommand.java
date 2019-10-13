package org.apache.hadoop.ozone.admin.om;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of om get-service-roles command.
 */
@CommandLine.Command(
    name = "get-service-roles",
    description = "List all OMs and their respective Ratis server roles",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class GetServiceRolesSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--om-service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;

  @Override
  public Void call() throws Exception {
    ClientProtocol client = parent.createClient(omServiceId);
    getOmServerRoles(client.getOMServerRoles());
    return null;
  }

  private void getOmServerRoles(List<OMRoleInfo> roleInfos) {
    for (OMRoleInfo roleInfo : roleInfos) {
      System.out.println(
          roleInfo.getNodeId() + " : " + roleInfo.getServerRole());
    }
  }
}
