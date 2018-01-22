package com.sdu.spark.streaming.socket

import java.net.{InetSocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import java.util.concurrent.atomic.AtomicBoolean

import com.sdu.spark.{Message, Utils}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.reflect.ClassTag
import scala.util.control.Breaks._

/**
  * @author hanhan.zhang
  * */
private[streaming] class SocketInputDStream[T: ClassTag](ssc: StreamingContext, storageLevel: StorageLevel) extends ReceiverInputDStream[T](ssc) {

  override def getReceiver(): Receiver[T] = {
    new SocketStreamFetcher(storageLevel, ssc.sparkContext.getConf)
  }

}

private[streaming] class SocketStreamFetcher[T: ClassTag](storageLevel: StorageLevel,
                                                          conf: SparkConf) extends Receiver[T](storageLevel) with Logging {

  val started = new AtomicBoolean(false)
  val SOCKET_HEAD_LENGTH = 4

  override def onStart(): Unit = {
    val sc = SocketChannel.open()
    val selector = Selector.open()
    sc.configureBlocking(false)

    // Socket选项
    sc.setOption[Integer](StandardSocketOptions.SO_RCVBUF, 10240)
    sc.setOption[Integer](StandardSocketOptions.SO_SNDBUF, 10240)
    sc.setOption[java.lang.Boolean](StandardSocketOptions.SO_KEEPALIVE, true)

    // 注册IO事件
    sc.register(selector, SelectionKey.OP_READ | SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE)

    // 连接
    val host = conf.get("spark.streaming.fetcher.host", Utils.getLocalIp)
    val port = conf.getInt("spark.streaming.fetcher.port", 6718)
    sc.connect(new InetSocketAddress(host, port))
    started.compareAndSet(false, true)

    logInfo(s"SocketStreamFetcher start to pull data from $host:$port")

    val buf = ByteBuffer.allocate(conf.getInt("spark.streaming.fetcher.buffer.size", 10240))
    val serializer = new JavaSerializer(conf)
    while (started.get) {
      selector.select()
      val it = selector.selectedKeys().iterator()
      while (it.hasNext) {
        val key = it.next()
        it.remove()

        if (key.isConnectable) {
          doConnect(key)
        } else if (key.isReadable) {
          doRead(key, buf, serializer)
        } else if (key.isWritable) {
          doWrite(key)
        }
      }

    }
  }

  private def doConnect(key: SelectionKey): Unit = {
    val sc = key.channel().asInstanceOf[SocketChannel]
    if (sc.isConnectionPending) {
      sc.finishConnect()
      // 变更监听事件
      key.interestOps(SelectionKey.OP_READ)
    }
  }

  private def doRead(key: SelectionKey, buf: ByteBuffer, serializer: JavaSerializer): Unit = {
    val sc = key.channel().asInstanceOf[SocketChannel]
    val size = sc.read(buf)
    if (size < 0) {
      val address = Utils.getAddress(sc)
      logInfo(s"SocketStreamFetcher start to close because of server is shutdown, address: $address")
      sc.close()
      System.exit(-1)
    }
    buf.flip()

    var read = false
    // 处理读半包
    while (buf.hasRemaining) {
      // 记录当前Buffer的position, 当发生读半包时将Buffer还原
      buf.mark

      if (buf.remaining < SOCKET_HEAD_LENGTH) {
        break
      }

      val bodyLength = buf.getInt
      if (buf.remaining() < bodyLength) {
        // 发生'读半包', buf还原初始状态, 继续读取Socket通道数据
        buf.reset()
        break
      }

      val messageBytes = new Array[Byte](bodyLength)
      buf.get(messageBytes)

      val dataItem = serializer.newInstance().deserialize[T](ByteBuffer.wrap(messageBytes))
      store(dataItem)

//      logInfo(s"===================$dataItem======================")

      read = true
    }

    if (read) {
      buf.compact()
    }
    key.interestOps(SelectionKey.OP_READ)
  }

  private def doWrite(key: SelectionKey): Unit = {

  }

  override def onStop(): Unit = {
    started.compareAndSet(true, false)
  }


}

//object SocketStreamFetcher {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//    conf.set("spark.streaming.fetcher.host", Utils.getLocalIp)
//    conf.set("spark.streaming.fetcher.port", "6719")
//
//    val fetcher = new SocketStreamFetcher[Message](StorageLevel.MEMORY_ONLY, conf)
//    fetcher.onStart()
//  }
//}

