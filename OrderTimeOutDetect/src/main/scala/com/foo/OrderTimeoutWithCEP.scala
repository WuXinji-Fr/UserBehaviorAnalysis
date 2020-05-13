package com.foo

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, pattern}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
* author wxj
* date 2020-05-06
* 支付订单实时监控模块CEP实现
* */

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, eventType: String)

object OrderTimeoutWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(1, "pay", 1558430842),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000)


    // 定义一个带匹配时间窗口的模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 定义一个输出标签，用于标明侧输出流，因为我们想要拿到的最重要的数据不是匹配成功的数据，更想拿到的是超时的数据
    val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")

    //    在keyby之后的流中匹配出定义的pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    //    再从pattern stream当中获取匹配的事件流
    //    .select方法 传入一个 pattern select function ，当检测到定义好的模式序列时就会调用
    import scala.collection.Map
    val completedResultDataStream = patternStream.select(orderTimeoutOutput)(
//      对于超时的部分序列，调用pattern select function
//      柯里化
    (pattern:Map[String,Iterable[OrderEvent]],timestamp:Long)=>{
        val timeOutOrderId = pattern.getOrElse("begin",null).iterator.next().orderId
      OrderResult(timeOutOrderId,"timeout")
    })(

//    正常匹配的部分，调用pattern select function
    (pattern:Map[String,Iterable[OrderEvent]])=>{
      val payedOrderId = pattern.getOrElse("follow",null).iterator.next().orderId
      OrderResult(payedOrderId,"sucess")
    })

    completedResultDataStream.print()//打印出来的是匹配的事件序列

//    打印输出timeout结果
    val timeOutResultDataStream = completedResultDataStream.getSideOutput(orderTimeoutOutput)

    timeOutResultDataStream.print()

    env.execute("time Out CEP")
  }
}
