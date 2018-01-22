package com.sdu.spark

import java.net.{InetAddress, InetSocketAddress}
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
  * @author hanhan.zhang
  * */
object Utils {

  def getLocalIp : String = {
    InetAddress.getLocalHost.getHostAddress
  }

  def getAddress(sc: SocketChannel) : String = {
    val address = sc.getRemoteAddress.asInstanceOf[InetSocketAddress]
    val host = address.getHostString
    val port = address.getPort
    s"$host:$port"
  }

  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }
}
