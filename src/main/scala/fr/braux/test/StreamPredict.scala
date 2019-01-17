package fr.braux.test

import java.util.Properties

import ml.dmlc.xgboost4j.scala.{Booster, XGBoost}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import scala.util.Random

object StreamPredict extends App  {

    if (args.length == 0) {
      System.err.println("USAGE: StreamPredict --brokers <broker list> --in <input topic> --out <output topic> --model <model file> <nb_features>")
      System.exit(1)
    }


    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/#via-withparametersconfiguration
    val conf = new Configuration()

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", params.getRequired("brokers"))
    properties.setProperty("group.id", Random.alphanumeric.filter(_.isLetter).take(20).mkString)
    properties.setProperty("auto.offset.reset", "earliest")
    val model = XGBoost.loadModel(params.getRequired("model"))
    val features = params.getInt("features",127)

    val consumer = new FlinkKafkaConsumer010[String](params.getRequired("in"), new SimpleStringSchema(), properties)
    val producer = new FlinkKafkaProducer010[String](params.getRequired("brokers"), params.getRequired("out"), new SimpleStringSchema())

    val streamin = env.addSource(consumer)

    class MapPredict(boost: Booster, shape: Int) extends RichMapFunction[String,String] {
        override def map(svmrow: String): String = {
            boost.predict(Utils.svm2DMatrix(svmrow, shape)).mkString
        }
    }

    // this line with hard-coded params is working
    //  val streamout = streamin.map(x => XGBoost.loadModel("/data/agaricus.model").predict(Utils.svm2DMatrix(x, 127).mkString)
    val streamout = streamin.map(new MapPredict(model, features))
    streamout.addSink(producer)
    env.execute("StreamPredict")
}
