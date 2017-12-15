package com.sdu.stream.flink.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hanhan.zhang
 * */
public class StreamSourceServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSourceServer.class);

    private static final AtomicLong IDGenerator = new AtomicLong(0);

    public void start(int port) throws IOException {
        ServerSocket ss = new ServerSocket(port, 50);
        ss.setReuseAddress(true);
        ss.setReceiveBufferSize(1024);
        // 处理连接请求
        while (ss.isBound()) {
            Socket sc = ss.accept();
            TransportThread transport = new TransportThread(sc, "TransportThread-" + IDGenerator.getAndIncrement());
            transport.start();
        }
    }

    private class TransportThread extends Thread {

        private Socket sc;

        private volatile boolean isRunning = true;

        public TransportThread(Socket sc, String threadName) {
            super(threadName);
            this.sc = sc;
        }

        @Override
        public void run() {
            // 获取输出流, 写入数据
            while (isRunning) {
                if (sc == null || sc.isClosed()) {
                    LOGGER.info("Remote client already disconnected, address: {}", sc);
                    cancel();
                    break;
                }
                try {
                    OutputStream output = sc.getOutputStream();

                    Scanner scanner = new Scanner(System.in);
                    LOGGER.info("Thread: {} waiting input data ...", getName());
                    while (isRunning) {
                        String data = scanner.nextLine() + "\n";
                        output.write(data.getBytes());
                        output.flush();
                    }

                } catch (IOException e) {
                    throw new RuntimeException("Thread: " + getName() + " occur exception", e);
                }
            }
        }

        private void cancel() {
            if (isRunning) {
                isRunning = false;
                return;
            }
            throw new IllegalStateException("Thread: " + getName() + " already stop running");
        }
    }

    public static void main(String[] args) throws IOException {
        StreamSourceServer server = new StreamSourceServer();
        server.start(6712);
    }
}

