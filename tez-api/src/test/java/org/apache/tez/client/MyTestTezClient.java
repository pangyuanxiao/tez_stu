package org.apache.tez.client;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MyTestTezClient {
    class TezClientTest extends TezClient {
        TezYarnClient mockTezYarnClient;
        DAGClientAMProtocolBlockingPB sessionAmProxy;
        YarnClient mockYarnClient;
        ApplicationId mockAppId;
        boolean callRealGetSessionAMProxy;
        Long prewarmTimeoutMs;

        public TezClientTest(String name, TezConfiguration tezConf,
                                @Nullable Map<String, LocalResource> localResources,
                                @Nullable Credentials credentials) {
            super(name, tezConf, localResources, credentials);
        }

        @Override
        protected FrameworkClient createFrameworkClient() {
            return mockTezYarnClient;
        }

        @Override
        protected DAGClientAMProtocolBlockingPB getAMProxy(ApplicationId appId)
                throws TezException, IOException {
            if (!callRealGetSessionAMProxy) {
                return sessionAmProxy;
            }
            return super.getAMProxy(appId);
        }

        public void setPrewarmTimeoutMs(Long prewarmTimeoutMs) {
            this.prewarmTimeoutMs = prewarmTimeoutMs;
        }

        @Override
        protected long getPrewarmWaitTimeMs() {
            return prewarmTimeoutMs == null ? super.getPrewarmWaitTimeMs() : prewarmTimeoutMs;
        }
    }

    MyTestTezClient.TezClientTest configureAndCreateTezClient() throws YarnException, IOException, ServiceException {
        return configureAndCreateTezClient(null);
    }

    MyTestTezClient.TezClientTest configureAndCreateTezClient(TezConfiguration conf) throws YarnException, ServiceException,
            IOException {
        return configureAndCreateTezClient(new HashMap<String, LocalResource>(), true, conf);
    }
    MyTestTezClient.TezClientTest configureAndCreateTezClient(Map<String, LocalResource> lrs, boolean isSession,
                                                               TezConfiguration conf) throws YarnException, IOException, ServiceException {
        if (conf == null) {
            conf = new TezConfiguration();
        }
        conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
        //isSession = true
        conf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, isSession);

        MyTestTezClient.TezClientTest client = new MyTestTezClient.TezClientTest("test", conf, lrs, null);

        ApplicationId appId1 = ApplicationId.newInstance(0, 1);
        YarnClient yarnClient = mock(YarnClient.class, RETURNS_DEEP_STUBS);
        when(yarnClient.createApplication().getNewApplicationResponse().getApplicationId()).thenReturn(appId1);
        when(yarnClient.getApplicationReport(appId1).getYarnApplicationState()).thenReturn(YarnApplicationState.NEW);
        when(yarnClient.submitApplication(any(ApplicationSubmissionContext.class))).thenReturn(appId1);

        DAGClientAMProtocolBlockingPB sessionAmProxy = mock(DAGClientAMProtocolBlockingPB.class, RETURNS_DEEP_STUBS);
        when(sessionAmProxy.getAMStatus(any(RpcController.class), any(DAGClientAMProtocolRPC.GetAMStatusRequestProto.class)))
                .thenReturn(DAGClientAMProtocolRPC.GetAMStatusResponseProto.newBuilder().setStatus(DAGClientAMProtocolRPC.TezAppMasterStatusProto.RUNNING).build());

        client.sessionAmProxy = sessionAmProxy;
        client.mockTezYarnClient = new TezYarnClient(yarnClient);
        client.mockYarnClient = yarnClient;
        client.mockAppId = appId1;

        return client;
    }

    @Test(timeout = 10000)
    public void testSubmit() throws ServiceException, IOException, YarnException, TezException {
    final  MyTestTezClient.TezClientTest client = configureAndCreateTezClient();
    client.start();
    }

}
