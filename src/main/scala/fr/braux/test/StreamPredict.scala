package fr.braux.test

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Random
// warning: this line is mandatory to avoid error message "could not find implicit value for evidence parameter"
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.watermark.Watermark

import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.java.{DMatrix => JDMatrix}
import ml.dmlc.xgboost4j.scala.{Booster, XGBoost}
import ml.dmlc.xgboost4j.LabeledPoint

object StreamPredict extends App  {

    if (args.length == 0) {
      System.err.println("USAGE: StreamPredict --brokers <kafka brokers> --in <input topic> --out <output topic> --model <XGBoost model> --variables <# variables>")
      System.exit(1)
    }

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Event time processing (using implicit Kafka timestamp)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", params.getRequired("brokers"))
    properties.setProperty("group.id", Random.alphanumeric.filter(_.isLetter).take(20).mkString)
    properties.setProperty("auto.offset.reset", "earliest")
    // Partition discovery
    properties.setProperty("flink.partition-discovery.interval-millis","1000")

    val model = params.getRequired("model")
    val booster = XGBoost.loadModel("/data/" + model)
    val variables = params.getInt("variables",100)

    val consumer = new FlinkKafkaConsumer010[String](params.getRequired("in"), new SimpleStringSchema(), properties)
    // consumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[elt] { def extractAscendingTimestamp(element: String): Long = elt =})

    val producer = new FlinkKafkaProducer010[String](params.getRequired("brokers"), params.getRequired("out"), new SimpleStringSchema())
    val streamin = env.addSource(consumer)

    // per message processing (not optimal solution but working)
    class MapPredict(boost: Booster, numvar: Int) extends RichMapFunction[String,String] {
        override def map(svmrow: String): String = {
          // decode a svmlib row and build a CSR sparse matrix with a single row
          val items = svmrow.split(' ').tail
          val (indices,values) = items.map{item => (item.split(':')(0).toInt, item.split(':')(1).toFloat)}.unzip
          val dmat = new DMatrix(List[Long](0, indices.length).toArray, indices, values, JDMatrix.SparseType.CSR, numvar+1)
          val predict = boost.predict(dmat)
          // the prediction contains a single row and a single column (value between 0 and 1) which is rounded and prepended to the message
          (predict(0).map(p => if (p > 0.5) "1" else "0").head +: items).mkString(" ")
        }
    }
    // val streamout = streamin.map(new MapPredict(booster, variables))

    // windows processing (NOT WORKING)
    class WindowPredictFunction (boost: Booster) extends ProcessAllWindowFunction[String, String, TimeWindow] {
      override def process(context: Context, input: Iterable[String], out: Collector[String])  = {
        val mapper = (svmrow: String) => {
          val items = svmrow.split(' ').tail
          val (indices,values) = items.map{item => (item.split(':')(0).toInt, item.split(':')(1).toFloat)}.unzip
          LabeledPoint(0.0f, indices, values)
        }
        val dmat = new DMatrix(for (x <- input.iterator) yield mapper(x), null)
        val predict = boost.predict(dmat)
        out.collect(predict.map(_.head).map(p => if (p > 0.5) "1" else "0").head)
      }
    }

  // windows processing (test NOT WORKING pb with timestamps)
  class TestWindowFunction extends ProcessAllWindowFunction[String, String, TimeWindow] {
    override def process(context: Context, input: Iterable[String], out: Collector[String])  = {
      var cnt = 0
      for (x <- input) if (x.contains("118:")) cnt = cnt + 1
      out.collect(cnt.toString)
    }
  }

    val streamout = streamin.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1))).process(new TestWindowFunction())
    streamout.addSink(producer)
    env.execute("StreamPredict(" + model + ")")
}
