package com.sdu.hadoop.yarn.client;

import com.sdu.hadoop.yarn.client.protocol.AppSubmitClientProtocol;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * @author hanhan.zhang
 * */
public class ApplicationSubmitter implements AppSubmitClientProtocol {

    @Override
    public long getProtocolVersion(String protocol,
                                   long clientVersion) throws IOException {
        return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
                                                  long clientVersion,
                                                  int clientMethodsHash) throws IOException {
        return null;
    }
}
