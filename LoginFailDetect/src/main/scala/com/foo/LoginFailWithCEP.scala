package com.foo


import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(1, "192.168.0.3", "fail", 1558430845),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)

    //    定义一个匹配模式
    val loginFailParttern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    //    在keyby之后的流中匹配出定义的pattern stream
    val patternStream = CEP.pattern(loginStream, loginFailParttern)

    //    再从pattern stream当中获取匹配的事件流
    //    .select方法 传入一个 pattern select function ，当检测到定义好的模式序列时就会调用
    val loginFailDataStream = patternStream.select(
//      String指的是上面的Begin、next。。。Iterable指的是里面可迭代的事件
      (pattern:scala.collection.Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()
        (second.userId, first.ip, second.eventType)
      })

    loginFailDataStream.print()
    env.execute("Login Fail Detect Job")
  }
}
