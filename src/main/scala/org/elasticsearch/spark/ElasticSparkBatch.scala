package org.elastics.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.elasticsearch.spark.sql._

object ElasticSparkBatch {
  case class users(swid: String, birth_dt: String, gender_cd: String)

  case class products(url: String, category: String)

  def main(args: Array[String]) {

    if (args.length < 7) {
      System.err.println("Usage: bin/spark-submit " +
        "\n--class org.dfz.clickstream.spark.ElasticSparkBatch" +
        "\n--master yarn " +
        "\n--deploy-mode cluster"+
        "\n--num-executors 3 " +
        "\n--driver-memory 8g " +
        "\n--executor-memory 12g " +
        "\n--executor-cores 10 " +
        "\n/JAR_path/clickStream1.0.jar <JAR_application-arguments>: Elastic_Search_Node ES_Cluster_Name ES_Index ES_Index_mapping" +
        "UserFilePath ProductFilePath OmniDirPath" +
        "\n\nHelp Example: " +
        "\n./bin/spark-submit " +
        "--class org.dfz.clickstream.spark.ElasticSparkBatch " +
        "--master yarn " +
        "--num-executors 3 " +
        "--driver-memory 8g " +
        "--executor-memory 12g " +
        "--executor-cores 10 " +
        "/JAR_path/clickStream*.jar 10.1.51.* elasticsearch sample_Index sample_Index_mapping /root/UserFilePath /root/ProductFilePath /root/OmniDirPath")
      System.exit(1)
    }

    val es_node = args(0).toString()
    val es_cluster_name = args(1).toString()
    val es_index = args(2).toString()
    val es_index_mapping = args(3).toString()
    val userFilePath = args(4).toString()
    val productFilePath = args(5).toString()
    val omniDirPath = args(6).toString()

    // initialise spark context
    val conf = new SparkConf()
     // .setMaster("local")
     .setAppName("ElasticSparkBatch").set("es.index.auto.create", "true")

    val spark = SparkSession.builder().config(conf).getOrCreate()

  // System.setProperty("hadoop.home.dir", "E:\\Hadoop")

    import spark.sqlContext.implicits._

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    import org.apache.log4j.Logger
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.WARN)

    // Elastic connection parameters
    val elasticConf: Map[String, String] = Map("es.nodes" -> es_node,
      "es.clustername" -> es_cluster_name)

    // DataFrame
    val usersFile = spark.sparkContext.textFile(userFilePath)
    val userHeader = usersFile.first()
    val userRecords = usersFile.filter(x => x != userHeader)
    val usersDF = userRecords.map(x=>x.split("\t",-1)).map(u=>users(u(0),u(1),u(2))).toDF("swid", "birth_dt", "gender_cd")

    val usersDF1 = usersDF.filter("swid != 'null'").filter("swid != 'NULL'")
    usersDF1.createOrReplaceTempView("userData")

    val usersDF2=spark.sql("SELECT swid,birth_dt,gender_cd,CAST(datediff(from_unixtime( unix_timestamp() )," +
      "from_unixtime( unix_timestamp(birth_dt, 'dd-MMM-yy'))) / 365  AS INT) age from userData")

    usersDF2.createOrReplaceTempView("usersData")

    val productFile = spark.sparkContext.textFile(productFilePath)
    val productHeader = productFile.first()
    val productRecords = productFile.filter(x => x != productHeader)
    val productDF =   productRecords.map(x=>x.split("\t")).map(p=>products(p(0),p(1))).toDF("url","category")

    productDF.createOrReplaceTempView("productCategory")

    val omnitureFile =  spark.sparkContext.textFile(omniDirPath)
    val omnitureBatchDF1 = omnitureFile.map(x => x.split("\t",-1)).map(o => {
      org.dfz.elasticsearch.spark.OmnitureSchema(o(0), o(1), o(2), o(3), o(4), o(5), o(6), o(7), o(8), o(9), o(10), o(11), o(12),
        o(13), o(14), o(15), o(16), o(17), o(18), o(19), o(20), o(21), o(22), o(23), o(24), o(25), o(26), o(27), o(28), o(29),
        o(30), o(31), o(32), o(33), o(34), o(35), o(36), o(37), o(38), o(39), o(40), o(41), o(42), o(43), o(44), o(45), o(46),
        o(47), o(48), o(49), o(50), o(51), o(52), o(53), o(54), o(55), o(56), o(57), o(58), o(59), o(60), o(61), o(62), o(63),
        o(64), o(65), o(66), o(67), o(68), o(69), o(70), o(71), o(72), o(73), o(74), o(75), o(76), o(77), o(78), o(79), o(80),
        o(81), o(82), o(83), o(84), o(85), o(86), o(87), o(88), o(89), o(90), o(91), o(92), o(93), o(94), o(95), o(96), o(97),
        o(98), o(99), o(100), o(101), o(102), o(103), o(104), o(105), o(106), o(107), o(108), o(109), o(110), o(111), o(112),
        o(113), o(114), o(115), o(116), o(117), o(118), o(119), o(120), o(121), o(122), o(123), o(124), o(125), o(126), o(127),
        o(128), o(129), o(130), o(131), o(132), o(133), o(134), o(135), o(136), o(137), o(138), o(139), o(140), o(141), o(142),
        o(143), o(144), o(145), o(146), o(147), o(148), o(149), o(150), o(151), o(152), o(153), o(154), o(155), o(156), o(157),
        o(158), o(159), o(160), o(161), o(162), o(163), o(164), o(165), o(166), o(167), o(168), o(169), o(170), o(171), o(172),
        o(173), o(174), o(175), o(176), o(177))
    })
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
        "col_173", "col_174", "col_175", "col_176", "col_177", "col_178").na.fill("e",Seq("blank"))


    def remove_string: String => String = _.replaceAll("[{}]", "")
    def remove_string_udf = udf(remove_string)
    val omnitureBatchDF = omnitureBatchDF1.withColumn("swId", remove_string_udf($"swId"))
    omnitureBatchDF.createOrReplaceTempView("omnitureBatchLog")

    val omnitureProduct=spark.sql("SELECT  * FROM omnitureBatchLog o join productCategory p WHERE o.productUrl = p.url").toDF()
    omnitureProduct.createOrReplaceTempView("omnitureProductView")

    val omnitureUsers = spark.sql("SELECT * FROM omnitureProductView o join usersData u WHERE o.swId=u.swid")
    println("Storing batch data to elasticsearch")
    omnitureUsers.saveToEs(s"${es_index}/${es_index_mapping}", elasticConf)


    // terminate spark context
    //    spark.stop()
  }
}
