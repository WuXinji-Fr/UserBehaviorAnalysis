package com.foo

/*
* author wxj
* date 2020-05-06
* 实时流量统计模块
* 每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL
* */

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入数据格式
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//输出数据格式
case class UrlViewCount(url: String, windowEnd: Long, count: Long)


object TrafficAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.readTextFile("D:\\APP\\Scala\\workSpace\\UserBehaviorAnalysis\\NetWorkTrafficAnalysis\\src\\main\\resources\\apache.log")
      .map(line => {
        val lines = line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(lines(3)).getTime
        ApacheLogEvent(lines(0), lines(2), timestamp, lines(5), lines(6))
      })
      //    创建时间戳和水位线
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
      override def extractTimestamp(t: ApacheLogEvent): Long = {
        t.eventTime
      }
    })
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    stream.print()
    env.execute("Hot Url Job")
  }

  //  自定义实现聚合函数
  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  //  自定义实现windowFunction，输出的是ItemViewCount格式
  class WindowResultFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url: String = key
      val count = input.iterator.next()
      out.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

  //  自定义processFunction，统计访问量最大的URL，排序输出
  class TopNHotUrls(topsize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
    //  直接定义我们的状态变量，懒加载
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState", classOf[UrlViewCount]))

    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      //      将每条数据保存到状态中去
      urlState.add(i)
      //      注册定时器，windowEnd + 1
      context.timerService().registerEventTimeTimer(i.windowEnd + 10 * 1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //    从状态中获取所有的url的访问量
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()

      import scala.collection.JavaConversions._
      for (urlView <- urlState.get) {
        allUrlViews += urlView
      }
      //    清空state
      urlState.clear()

      //    按照访问量排序输出
      val sortedUrlView = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topsize)

      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      for (i <- sortedUrlView.indices) {
        val currentItem: UrlViewCount = sortedUrlView(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result.append("No").append(i + 1).append(":")
          .append("  URL=").append(currentItem.url)
          .append("  流量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}
