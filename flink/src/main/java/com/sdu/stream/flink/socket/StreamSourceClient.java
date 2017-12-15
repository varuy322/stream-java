package com.sdu.stream.flink.socket;

import java.io.InputStream;
import java.net.Socket;

/**
 * @author hanhan.zhang
 * */
public class StreamSourceClient {

    public static void main(String[] args) throws Exception {
        Socket sc = new Socket("localhost", 6712);

        sc.setReuseAddress(true);
        sc.setReceiveBufferSize(1024);
        sc.setTcpNoDelay(true);

        InputStream input = sc.getInputStream();
        while (true) {
            if (input.available() > 0) {
                byte[] bytes = new byte[input.available()];
                input.read(bytes);
                System.out.println(new String(bytes));
            }
        }
     }

}
