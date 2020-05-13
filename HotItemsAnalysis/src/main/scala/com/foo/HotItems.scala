package com.foo

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/*
*   Author : wxj
*   Time : 2020-05-05
*   实时热门商品统计模块
*   1、抽取业务时间戳，用业务时间做窗口
*   2、过滤出点击行为
*   3、滑动窗口
*   4、按每个窗口进行聚合，输出每个窗口中点击量，前TopN
* */

//输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)

//输出数据样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {
  def main(args: Array[String]): Unit = {

    /*val properties = new Properties()
    properties.setProperty("bootstrap.servers", "wxj:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")*/

    //    创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    显示的定义Time的类型，默认是ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    设置并行度
    env.setParallelism(1)

    val stream = env
            .readTextFile("D:\\APP\\Scala\\workSpace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
//      .addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map(line => {
        val lines = line.split(",")
        UserBehavior(lines(0).toLong, lines(1).toLong, lines(2).toInt, lines(3), lines(4).toLong)
      })
      //    指定时间戳和水位线
      .assignAscendingTimestamps(_.timeStamp * 1000) //真实业务一般都是乱序的，所以一般不用assignAscendingTimestamps，而使用BoundedOutOfOrdernessTimestampExtractor,这里我们把业务时间当成水位线
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))


    stream.print()
    env.execute("Hot Items Job")
  }

  //  自定义实现聚合函数
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  //  自定义实现windowFunction，输出的是ItemViewCount格式
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }


  //  自定义实现ProcessFunction
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

    //    状态定义ListState
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      //      定义状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)

      //      注册定时器，触发时间为windowEnd + 1 ，触发说明window已经搜集完所有数据
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }

    //    定义定时器的触发操作，从state里面取出所有的数据，排序TopN输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //      获取所有商品的点击信息
      var allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems += item
      }

      //      清楚状态中的数据，释放空间
      itemState.clear()

      // 按照点击量从大到小排序

      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成 String, 便于打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortedItems.indices) {
        val currentItem: ItemViewCount = sortedItems(i)
        // e.g.  No1 ：  商品 ID=12224  浏览量 =2413
        result.append("No").append(i + 1).append(":")
          .append("  商品 ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输 出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }

}
