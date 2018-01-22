package com.sdu.spark.streaming.socket

import com.esotericsoftware.kryo.Kryo
import com.sdu.spark.Message
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
  * @author hanhan.zhang
  * */
object SocketUtils {

  def createStream[T: ClassTag](ssc: StreamingContext, storageLevel: StorageLevel) : SocketInputDStream[T] = {
    new SocketInputDStream[T](ssc, storageLevel)
  }

}

private class SocketMessageRegister extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Message])
  }

}
