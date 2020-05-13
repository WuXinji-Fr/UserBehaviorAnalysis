package com.foo

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/*
* author wxj
* date 2020-05-06
* 状态编程的实现模式
* 只需要按照用户 ID 分流，然后遇到登录失败的事件时将其保存在 ListState 中，然后设置一个定时器，
* 2 秒后触发。定时器触发时检查状态中的登录失败事件个数，如果大于等于 2，那么就输出报警信息。
* */


//定义一个输入的登录事件流
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LoginFail {
  def main(args: Array[String]) {

    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //显示地定义Time的类型,默认是ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置并行度
    env.setParallelism(1)

    val loginStream = env
      .fromCollection(List(
        LoginEvent(3, "192.168.0.1", "fail", 1558430820),
        LoginEvent(1, "192.168.0.2", "fail", 1558430843),
        LoginEvent(1, "192.168.0.3", "fail", 1558430844),
        LoginEvent(1, "192.168.0.3", "fail", 1558430845),
        LoginEvent(2, "192.168.10.10", "success", 1558430845)
      ))
      .assignAscendingTimestamps(_.eventTime*1000)
      .filter(_.eventType=="fail")
      .keyBy(_.userId) //按照userId做分流
      .process(new MatchFunction)

    loginStream.print()

    env.execute("Login Fail Detect Job")
  }

}

//自定义KeyedProcessFunction
class MatchFunction extends KeyedProcessFunction[Long,LoginEvent,LoginEvent] {
  //直接定义我们的状态变量,懒加载
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginState",classOf[LoginEvent]))
  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
    loginState.add(i)
    //注册定时器,设定为2s后触发
    context.timerService().registerEventTimeTimer(i.eventTime*1000 + 2 * 1000)

  }

  //实现onTimer的触发操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
    //从状态中获取所有的url的访问量
    val allLogins: ListBuffer[LoginEvent] = ListBuffer()
    import scala.collection.JavaConversions._
    for ( login<- loginState.get() ) {
      allLogins += login
    }
    //清楚状态
    loginState.clear()

    if(allLogins.length > 1){
      out.collect(allLogins.head)

    }
  }
}
