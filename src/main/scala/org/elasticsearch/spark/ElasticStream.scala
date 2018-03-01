package org.elastics.spark

/**
  * Created by vikas on 10/10/2017.
  */

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql._

object ElasticStream {

  case class users(uId: String, birth_dt: String, gender_cd: String)

  case class products(url: String, category: String)

  def main(args: Array[String]) {

    val userFilePath = args(0).toString()
    val productFilePath = args(1).toString()
    val outputFilePath = args(2).toString()
    val kafkaBroker = args(3).toString()
    val warehouse_dir = args(4).toString()
    val es_node = args(5).toString()
    val es_cluster_name = args(6).toString()
    val es_index = args(7).toString()
    val es_index_mapping = args(8).toString()
    val kafka_topic = args(9).toString()


    if (args.length < 10) {
      System.err.println("Usage: bin/spark-submit " +
        "\n--class org.dfz.clickstream.spark.ElasticStream" +
        "\n--master yarn " +
        "\n--deploy-mode cluster"+
        "\n--num-executors 3 " +
        "\n--driver-memory 8g " +
        "\n--executor-memory 12g " +
        "\n--executor-cores 10 " +
        "\n/JAR_path/clickStream1.0.jar <JAR_application-arguments>: UserFilePath ProductFilePath " +
        "outputFilePath kafkaBroker warehouse_dir " +
        "es_node es_cluster_name es_index es_index_mapping" +
        "\n\nHelp Example: " +
        "\n./bin/spark-submit " +
        "--class org.dfz.clickstream.spark.ElasticStream " +
        "--master yarn " +
        "--num-executors 3 " +
        "--driver-memory 8g " +
        "--executor-memory 12g " +
        "--executor-cores 10 " +
        "/JAR_path/clickStream*.jar 10.1.51.*  /root/UserFilePath /root/ProductFilePath /root/outputFilePath")
      System.exit(1)
    }


    val conf = new SparkConf()
    // .setMaster("local[*]")

    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.debug","maxToStringFields")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.kryoserializer.buffer.max","768m")
    conf.set("spark.sql.codegen","true")
    conf.set("spark.executor.memory", "3g")

    val spark = SparkSession.builder()
     // .config("spark.sql.warehouse.dir", "E:\\POC\\SparkEs\\spark-warehouse")
      .config("spark.sql.warehouse.dir", warehouse_dir)
      .config(conf)
      .getOrCreate()

    //System.setProperty("hadoop.home.dir", "E:\\Hadoop")
    import spark.sqlContext.implicits._

    //log4j
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    import org.apache.log4j.Logger
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.WARN)

    // Elastic connection parameters
    val elasticConf: Map[String, String] = Map("es.nodes" -> es_node,
      "es.clustername" -> es_cluster_name)

    //loading users Data
    val usersFile = spark.sparkContext.textFile(userFilePath).cache()
    val userHeader = usersFile.first()
    val userRecords = usersFile.filter(x => x != userHeader)
    val usersDF = userRecords.map(x => x.split("\t", -1))
      .map(u => users(u(0), u(1), u(2))).toDF("uId", "birth_dt", "gender_cd")
    val usersDF1 = usersDF.filter("uId != 'null'").filter("uId != 'NULL'")
    usersDF1.createOrReplaceTempView("userData")
    val usersDF2 = spark.sql("SELECT uId,birth_dt,gender_cd,CAST(datediff(from_unixtime( unix_timestamp() )," +
      "from_unixtime( unix_timestamp(birth_dt, 'dd-MMM-yy'))) / 365  AS INT) age from userData")
    usersDF2.createOrReplaceTempView("usersData")
    usersDF2.persist()

    //loading product Data
    val productFile = spark.sparkContext.textFile(productFilePath).cache()
    val productHeader = productFile.first()
    val productRecords = productFile.filter(x => x != productHeader)
    val productDF = productRecords.map(x => x.split("\t"))
      .map(p => products(p(0), p(1))).toDF("url", "category")

    productDF.createOrReplaceTempView("productCategory")
    productDF.persist()

    //Streaming
    val ssc = new StreamingContext(spark.sparkContext, Seconds(4))

    val kafkat = Set(kafka_topic)
    val kafkaParams = Map[String, Object](
      //"bootstrap.servers" -> "172.17.0.2:6667"
      "bootstrap.servers" ->  kafkaBroker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //Kafka Spark Streaming Consumer
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](kafkat, kafkaParams)
    )

    kafkaStream.foreachRDD {
      msg =>
        if (!msg.isEmpty) {
          val omnitureStreamDF = msg.map { v => v.value().split("\t",-1)}
            .map(o => {
              org.dfz.elasticsearch.spark.OmnitureSchema(o(0).trim.toString, o(1).trim.toString, o(2).trim.toString, o(3).trim.toString, o(4).trim.toString, o(5).trim.toString, o(6).trim.toString, o(7).trim.toString, o(8).trim.toString, o(9).trim.toString, o(10).trim.toString, o(11).trim.toString, o(12).trim.toString, o(13).trim.toString,
                o(14).trim.toString, o(15).trim.toString, o(16).trim.toString, o(17).trim.toString, o(18).trim.toString, o(19).trim.toString, o(20).trim.toString, o(21).trim.toString, o(22).trim.toString, o(23).trim.toString, o(24).trim.toString, o(25).trim.toString, o(26).trim.toString, o(27).trim.toString, o(28).trim.toString, o(29).trim.toString, o(30).trim.toString, o(31).trim.toString, o(32).trim.toString,
                o(33).trim.toString, o(34).trim.toString, o(35).trim.toString, o(36).trim.toString, o(37).trim.toString, o(38).trim.toString, o(39).trim.toString, o(40).trim.toString, o(41).trim.toString, o(42).trim.toString, o(43).trim.toString, o(44).trim.toString, o(45).trim.toString, o(46).trim.toString, o(47).trim.toString, o(48).trim.toString, o(49).trim.toString, o(50).trim.toString, o(51).trim.toString,
                o(52).trim.toString, o(53).trim.toString, o(54).trim.toString, o(55).trim.toString, o(56).trim.toString, o(57).trim.toString, o(58).trim.toString, o(59).trim.toString, o(60).trim.toString, o(61).trim.toString, o(62).trim.toString, o(63).trim.toString, o(64).trim.toString, o(65).trim.toString, o(66).trim.toString, o(67).trim.toString, o(68).trim.toString,
                o(69).trim.toString, o(70).trim.toString, o(71).trim.toString, o(72).trim.toString, o(73).trim.toString, o(74).trim.toString, o(75).trim.toString, o(76).trim.toString, o(77).trim.toString, o(78).trim.toString, o(79).trim.toString, o(80).trim.toString, o(81).trim.toString, o(82).trim.toString, o(83).trim.toString, o(84).trim.toString, o(85).trim.toString,
                o(86).trim.toString, o(87).trim.toString, o(88).trim.toString, o(89).trim.toString, o(90).trim.toString, o(91).trim.toString, o(92).trim.toString, o(93).trim.toString, o(94).trim.toString, o(95).trim.toString, o(96).trim.toString, o(97).trim.toString, o(98).trim.toString, o(99).trim.toString, o(100).trim.toString, o(101).trim.toString, o(102).trim.toString,
                o(103).trim.toString, o(104).trim.toString, o(105).trim.toString, o(106).trim.toString, o(107).trim.toString, o(108).trim.toString, o(109).trim.toString, o(110).trim.toString, o(111).trim.toString, o(112).trim.toString, o(113).trim.toString, o(114).trim.toString, o(115).trim.toString, o(116).trim.toString, o(117).trim.toString,
                o(118).trim.toString, o(119).trim.toString, o(120).trim.toString, o(121).trim.toString, o(122).trim.toString, o(123).trim.toString, o(124).trim.toString, o(125).trim.toString, o(126).trim.toString, o(127).trim.toString, o(128).trim.toString, o(129).trim.toString, o(130).trim.toString, o(131).trim.toString, o(132).trim.toString,
                o(133).trim.toString, o(134).trim.toString, o(135).trim.toString, o(136).trim.toString, o(137).trim.toString, o(138).trim.toString, o(139).trim.toString, o(140).trim.toString, o(141).trim.toString, o(142).trim.toString, o(143).trim.toString, o(144).trim.toString, o(145).trim.toString, o(146).trim.toString, o(147).trim.toString,
                o(148).trim.toString, o(149).trim.toString, o(150).trim.toString, o(151).trim.toString, o(152).trim.toString, o(153).trim.toString, o(154).trim.toString, o(155).trim.toString, o(156).trim.toString, o(157).trim.toString, o(158).trim.toString, o(159).trim.toString, o(160).trim.toString, o(161).trim.toString, o(162).trim.toString,
                o(163).trim.toString, o(164).trim.toString, o(165).trim.toString, o(166).trim.toString, o(167).trim.toString, o(168).trim.toString, o(169).trim.toString, o(170).trim.toString, o(171).trim.toString, o(172).trim.toString, o(173).trim.toString, o(174).trim.toString, o(175).trim.toString, o(176).trim.toString, o(177))
            }).cache()
            .toDF("sessionId", "clickTime", "col_3", "col_4", "col_5", "col_6", "col_7", "ipAddress", "col_9", "col_10",
              "col_11", "col_12", "productUrl", "swId", "col_15", "col_16", "col_17", "col_18", "col_19", "col_20",
              "col_21", "col_22", "col_23", "col_24", "col_25", "col_26", "col_27", "language", "col_29", "col_30",
              "col_31", "col_32", "col_33", "col_34", "col_35", "col_36", "col_37", "col_38", "domain", "regTime",
              "col_41", "col_42", "col_43", "sysSpec", "col_45", "col_46", "col_47", "col_48", "col_49", "city",
              "country", "areaCode", "state", "col_54", "col_55", "col_56", "col_57", "col_58", "col_59", "col_60",
              "col_61", "col_62", "col_63", "col_64", "col_65", "col_66", "col_67", "col_68", "col_69", "col_70",
              "col_71", "col_72", "col_73", "col_74", "col_75", "col_76", "col_77", "col_78", "col_79", "col_80",
              "col_81", "col_82", "col_83", "col_84", "col_85", "col_86", "col_87", "col_88", "col_89", "col_90",
              "col_91", "col_92", "col_93", "col_94", "col_95", "col_96", "col_97", "col_98", "col_99", "col_100",
              "col_101", "col_102", "col_103", "col_104", "col_105", "col_106", "col_107", "col_108", "col_109",
              "col_110", "col_111", "col_112", "col_113", "col_114", "col_115", "col_116", "col_117", "col_118",
              "col_119", "col_120", "col_121", "col_122", "col_123", "col_124", "col_125", "col_126", "col_127",
              "col_128", "col_129", "col_130", "col_131", "col_132", "col_133", "col_134", "col_135", "col_136",
              "col_137", "col_138", "col_139", "col_140", "col_141", "col_142", "col_143", "col_144", "col_145",
              "col_146", "col_147", "col_148", "col_149", "col_150", "col_151", "col_152", "col_153", "col_154",
              "col_155", "col_156", "col_157", "col_158", "col_159", "col_160", "col_161", "col_162", "col_163",
              "col_164", "col_165", "col_166", "col_167", "col_168", "col_169", "col_170", "col_171", "col_172",
              "col_173", "col_174", "col_175", "col_176", "col_177", "col_178")

          omnitureStreamDF.createOrReplaceTempView("omnitureStreamLog")

          omnitureStreamDF.persist()

          val omniProdStreamDF=spark.sql("SELECT  * FROM omnitureStreamLog o join productCategory p on o.productUrl = p.url")

          omniProdStreamDF.createOrReplaceTempView("omnitureProdLog")

          val omniStreamDF=spark.sql("SELECT  * FROM omnitureProdLog o join usersData u on o.swId=u.uId")

          println("Storing Stream data to files")

          omniStreamDF.filter("sessionId != ''")
            .write.format("json").mode("append")
            .save(outputFilePath)

          println("Storing stream data to elasticsearch")

          omnitureStreamDF.filter("sessionId != ''").saveToEs(s"${es_index}/${es_index_mapping}", elasticConf)

          println("Stored stream data to elasticsearch")

        }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}



//omniStreamDF.write.format("json").mode("append").save(outputFilePath)

//          import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
//          import org.apache.hadoop.conf.Configuration
//          import java.io.File
//
//          def merge(srcPath: String, dstPath: String): Unit =  {
//            val hadoopConfig = new Configuration()
//            val hdfs = FileSystem.get(hadoopConfig)
//            FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
//          }
//
//          val file = args(4).toString()  //E:\POC\Code\Data\output
//          FileUtil.fullyDelete(new File(file))
//          val destinationFile = args(5).toString()  //E:\POC\Code\Data\Dest_cust
//          FileUtil.fullyDelete(new File(destinationFile))



//          val file = "E:\\POC\\Code\\Data\\output"
//          FileUtil.fullyDelete(new File(file))
//
//          val destinationFile= "E:\\POC\\Code\\Data\\CustAct\\custStreamLogs.json"
//          FileUtil.fullyDelete(new File(destinationFile))

//  omnitureStreamDF.saveToEs("run2_index/run2_index_type",elasticConf)

//          omnitureStreamDF.write.format("org.elasticsearch.spark.sql")
//          .option("es.resource","custAct_index/custAct_index_type")
//          .mode("overwrite")
//          .save()