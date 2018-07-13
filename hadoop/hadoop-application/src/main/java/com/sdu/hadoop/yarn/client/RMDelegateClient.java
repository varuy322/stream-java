package com.sdu.hadoop.yarn.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Note:
 * 1: {@link org.apache.hadoop.yarn.client.api.impl.YarnClientImpl#rmClient}
 *
 *    实现Client与ResourceManager交互协议ApplicationClientProtocol
 *
 * 2:
 *
 * @author hanhan.zhang
 * */
public class RMDelegateClient extends YarnClient {

    private YarnClient client;

    public RMDelegateClient(YarnConfiguration conf) {
        super(RMDelegateClient.class.getName());
        client = YarnClient.createYarnClient();

        // 启动Service lifecycle

        // AbstractService.init()
        //   |
        //   +-------> AbstractService.serviceInit()
        init(conf);

        // AbstractService.start()
        //   |
        //   +-------> AbstractService.serviceStart()
        start();
    }

    /************************************ service lifecycle start *****************************************/

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        client.init(conf);
        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        client.start();
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        client.stop();
        super.serviceStop();
    }

    /************************************ service lifecycle end ******************************************/

    @Override
    public YarnClientApplication createApplication() throws YarnException, IOException {
        return null;
    }

    @Override
    public ApplicationId submitApplication(ApplicationSubmissionContext appContext) throws YarnException, IOException {
        return null;
    }

    @Override
    public void killApplication(ApplicationId applicationId) throws YarnException, IOException {

    }

    @Override
    public ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException {
        return null;
    }

    @Override
    public Token<AMRMTokenIdentifier> getAMRMToken(ApplicationId appId) throws YarnException, IOException {
        return null;
    }

    @Override
    public List<ApplicationReport> getApplications() throws YarnException, IOException {
        return null;
    }

    @Override
    public List<ApplicationReport> getApplications(Set<String> applicationTypes) throws YarnException, IOException {
        return null;
    }

    @Override
    public List<ApplicationReport> getApplications(EnumSet<YarnApplicationState> applicationStates) throws YarnException, IOException {
        return null;
    }

    @Override
    public List<ApplicationReport> getApplications(Set<String> applicationTypes, EnumSet<YarnApplicationState> applicationStates) throws YarnException, IOException {
        return null;
    }

    @Override
    public YarnClusterMetrics getYarnClusterMetrics() throws YarnException, IOException {
        return null;
    }

    @Override
    public List<NodeReport> getNodeReports(NodeState... states) throws YarnException, IOException {
        return null;
    }

    @Override
    public org.apache.hadoop.yarn.api.records.Token getRMDelegationToken(Text renewer) throws YarnException, IOException {
        return null;
    }

    @Override
    public QueueInfo getQueueInfo(String queueName) throws YarnException, IOException {
        return null;
    }

    @Override
    public List<QueueInfo> getAllQueues() throws YarnException, IOException {
        return null;
    }

    @Override
    public List<QueueInfo> getRootQueueInfos() throws YarnException, IOException {
        return null;
    }

    @Override
    public List<QueueInfo> getChildQueueInfos(String parent) throws YarnException, IOException {
        return null;
    }

    @Override
    public List<QueueUserACLInfo> getQueueAclsInfo() throws YarnException, IOException {
        return null;
    }

    @Override
    public ApplicationAttemptReport getApplicationAttemptReport(ApplicationAttemptId applicationAttemptId) throws YarnException, IOException {
        return null;
    }

    @Override
    public List<ApplicationAttemptReport> getApplicationAttempts(ApplicationId applicationId) throws YarnException, IOException {
        return null;
    }

    @Override
    public ContainerReport getContainerReport(ContainerId containerId) throws YarnException, IOException {
        return null;
    }

    @Override
    public List<ContainerReport> getContainers(ApplicationAttemptId applicationAttemptId) throws YarnException, IOException {
        return null;
    }

    @Override
    public void moveApplicationAcrossQueues(ApplicationId appId, String queue) throws YarnException, IOException {

    }

    @Override
    public ReservationSubmissionResponse submitReservation(ReservationSubmissionRequest request) throws YarnException, IOException {
        return null;
    }

    @Override
    public ReservationUpdateResponse updateReservation(ReservationUpdateRequest request) throws YarnException, IOException {
        return null;
    }

    @Override
    public ReservationDeleteResponse deleteReservation(ReservationDeleteRequest request) throws YarnException, IOException {
        return null;
    }

    @Override
    public Map<NodeId, Set<String>> getNodeToLabels() throws YarnException, IOException {
        return null;
    }

    @Override
    public Map<String, Set<NodeId>> getLabelsToNodes() throws YarnException, IOException {
        return null;
    }

    @Override
    public Map<String, Set<NodeId>> getLabelsToNodes(Set<String> labels) throws YarnException, IOException {
        return null;
    }

    @Override
    public Set<String> getClusterNodeLabels() throws YarnException, IOException {
        return null;
    }
}
