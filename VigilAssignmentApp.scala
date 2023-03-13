package com.vigil.app

import com.amazonaws.auth.profile.{ProfileCredentialsProvider, ProfilesConfigFile}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession


object VigilAssignmentApp {
  val invalidInputErrorMessage = """Invalid Input Params Detected, Valid Input Params: inputPath="s3://path/to/the/input" outputPath="s3://path/to/the/output" awsProfileName="default" """
  val invalidS3PathErrorMessage = "Invalid s3 path found, valid path: s3://vigil-input-data/data/"
  val inputPathKey = "inputPath"
  val outputPathKey = "outputPath"
  val awsProfileNamePath = "awsProfileName"
  val s3PathPattern = "s3:/(.+)".r

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Vigil_Assignment_Application").getOrCreate()
    log.info("==Welcome to the Assignment==")
    //parse and validate the input arguments
    val inputArgsMap: Map[String, String] = parseInputArgs(args)

    val inputBucketName = getBucketAndKey(inputArgsMap(inputPathKey)).map(_._1)
    val outputBucketAndKey = getBucketAndKey(inputArgsMap(outputPathKey))

    //throw an exception if the provided s3 paths are in invalid format
    if(inputBucketName.isEmpty || outputBucketAndKey.isEmpty) {
      throw new IllegalArgumentException(invalidS3PathErrorMessage)
    }

    //setup aws client config
    val profilesConfigFile = new ProfilesConfigFile()
    val accessKey = profilesConfigFile.getCredentials(inputArgsMap(awsProfileNamePath)).getAWSAccessKeyId
    val secretAccessKey = profilesConfigFile.getCredentials(inputArgsMap(awsProfileNamePath)).getAWSSecretKey
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val inputS3Path = inputArgsMap(inputPathKey).replace("s3", "s3a")

    //read the input data into a rdd, read each valid row and convert it to tab separated values
    val resultRDD = spark.sparkContext.textFile(inputS3Path)
      .flatMap(getValidRow)
      .map(_-> 1)
      .reduceByKey(_+_)
      .filter(_._2 % 2 != 0)
      .map(item => item._1.split(",").head + "\t"+ item._2)

    // convert the final rdd to a string
    val resultStr = resultRDD.collect().toList.mkString("\n")

    //setup s3 client and write the results to output s3 path
    val s3Client: AmazonS3 = AmazonS3ClientBuilder
        .standard()
      .withCredentials(new ProfileCredentialsProvider(profilesConfigFile, inputArgsMap(awsProfileNamePath)))
      .withRegion(Regions.US_EAST_2)
      .build()

    val response = s3Client.putObject(outputBucketAndKey.get._1, outputBucketAndKey.get._2 + "vigil_output2.tsv", resultStr)

    log.info(s"results stored to s3 output path successfully: ${response.getVersionId}")
    spark.stop()
  }

  /**
   * validates the input row
   * @param row input row data
   * @return row as a tab separated string if valid else None
   */
  def getValidRow(row: String): Option[String] = row match {
    case data if data.matches("\\d,\\d")=> Some(data)
    case data if data.matches("\\d\t\\d") => Some(data.replace("\t", ","))
    case data if data.matches(",\\d") =>  Some("0"+data)
    case data if data.matches("\t\\d") =>  Some("0"+data.replace("\t", ","))
    case _ => None
  }

  /**
   * validate and parse the input arguments
   * @param args array of input arguments
   * @return key value pairs if input arguments
   */
  def parseInputArgs(args: Array[String]): Map[String, String] = {
    //verify if the input has only 3 params with format: key=value else throw exception
    if(args.length != 3 && args.forall(_.split("=").length != 2)) throw new IllegalArgumentException(invalidInputErrorMessage)
     val inputArgsMap = args.map(arg => {
      val keyValArr = arg.split("=")
       keyValArr.head -> keyValArr.last
    }).toMap

    // verify if the 3 input params key names are as expected. This is needed to describe the purpose of the param
    if(inputArgsMap.keySet != Set(inputPathKey, outputPathKey, awsProfileNamePath)) throw new IllegalArgumentException(invalidInputErrorMessage)
    inputArgsMap
  }

  /**
   * parses the s3 path into bucket name and key
   * @param s3Path s3 path string
   * @return optional tuple of bucket name and key
   */
  def getBucketAndKey(s3Path: String): Option[(String, String)] = {
    s3Path match {
      case s3PathPattern(url) => {
        val Array(_, bucketName, key) = url.split("/", 3)
        Some(bucketName -> key)
      }
      case _ => None
    }
  }
}
