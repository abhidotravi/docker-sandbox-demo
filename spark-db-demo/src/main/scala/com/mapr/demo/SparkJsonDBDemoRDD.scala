package com.mapr.demo

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.mapr.db.spark._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aravi on 11/28/17.
  *
  * As a part of this demo we load the business table and obtain city with most
  * restaurant rated more than 3 stars.
  *
  *
  */
object SparkJsonDBDemoRDD {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName("SparkJsonDBDemoRDD").setMaster("local[*]")
    val sc = new SparkContext(spark)

    val businessRDD = sc.loadFromMapRDB[Business]("/demoVol/business_table").where(field("stars") > 3)
    println(businessRDD
      .map(business => (business.city, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false).first())
  }
}


@JsonIgnoreProperties(ignoreUnknown = true)
case class Business (@JsonProperty("_id") id: String,
                   @JsonProperty("name") name: String,
                   @JsonProperty("review_count") review_count: Int,
                   @JsonProperty("stars") stars: Float,
                   @JsonProperty("address") address: String,
                   @JsonProperty("city") city: String,
                   @JsonProperty("state") state: String)