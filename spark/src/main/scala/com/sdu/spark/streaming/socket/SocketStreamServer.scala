package com.sdu.spark.streaming.socket

import java.net.{InetSocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.util.concurrent.TimeUnit

import com.sdu.spark.{Message, Utils}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer

import scala.util.Random

/**
  * @author hanhan.zhang
  * */
object SocketStreamServer {

  val words = Array[String]("刚", "刚", "闭", "幕", "的", "党", "的", "十", "九", "届", "二", "中", "全", "会",
                            "刚", "刚", "闭", "幕", "的", "党", "的", "十", "九", "届", "二", "中", "全", "会",
                            "刚", "刚", "闭", "幕", "的", "党", "的", "十", "九", "届", "二", "中", "全", "会")

  val random = new Random()


  def main(args: Array[String]): Unit = {
    val selector = Selector.open()
    val ssc = ServerSocketChannel.open()

    // 必须设置为非阻塞
    ssc.configureBlocking(false)

    // 设置ServerSocket属性
    ssc.setOption[java.lang.Boolean](StandardSocketOptions.SO_REUSEADDR, true)
    ssc.setOption[Integer](StandardSocketOptions.SO_RCVBUF, 10240)

    // 注册通道并监听OP_ACCEPT事件
    ssc.register(selector, SelectionKey.OP_ACCEPT)
    ssc.bind(new InetSocketAddress(Utils.getLocalIp, 6719))

    val serializer = new JavaSerializer(new SparkConf())

    while (true) {
      selector.select()

      val it = selector.selectedKeys().iterator()
      while (it.hasNext) {
        val key = it.next()
        it.remove()

        if (key.isAcceptable) {
          def doAccept(): Unit = {
            val ssc = key.channel().asInstanceOf[ServerSocketChannel]
            val sc = ssc.accept()
            sc.configureBlocking(false)
            sc.socket().setKeepAlive(true)
            sc.socket().setTcpNoDelay(true)
            // 注册通道
            sc.register(selector, SelectionKey.OP_WRITE)
          }
          doAccept()
        } else if (key.isReadable) {

        } else if (key.isWritable) {
          val doWrite = {
            val sc = key.channel().asInstanceOf[SocketChannel]
            val msg = Message(words(random.nextInt(words.length)), System.currentTimeMillis())
            val buf = serializer.newInstance().serialize[Message](msg)

            val sendBuf = ByteBuffer.allocate(4 + buf.remaining())
            sendBuf.putInt(buf.remaining())
            sendBuf.put(buf)
            sendBuf.flip()
            sc.write(sendBuf)

            // 注册IO事件
            key.interestOps(SelectionKey.OP_WRITE)

            TimeUnit.MILLISECONDS.sleep(500)
          }

          doWrite
        }
      }
    }
  }

}
