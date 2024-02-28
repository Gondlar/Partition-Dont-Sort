package de.unikl.cs.dbis.waves.testjobs.query

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions.{col,count,size,array,length}

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.util.Logger

object BetzeTwitter extends QueryRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("DeletedTweetsPerUser")
    runWithVoid(spark, jobConfig, Seq(
      (df: DataFrame) => df.where((col("retweeted_status.user").isNotNull)).groupBy(col("user.protected")).count().show(),
      (df: DataFrame) => df.where(((col("retweeted_status.quoted_status.id_str").isNotNull) || (col("user.notifications").isNotNull))).groupBy(col("place.bounding_box.type")).agg(count(col("user.profile_image_url"))).show(),
      (df: DataFrame) => df.where(((col("retweeted_status.quoted_status.user.listed_count") === 152229) || (col("retweeted_status.retweeted") === false))).groupBy(col("retweet_count")).count().show(),
      (df: DataFrame) => df.where((((col("retweeted_status.quoted_status.id_str").isNotNull) || (col("user.notifications").isNotNull)) && ((col("quoted_status.user.name").isNotNull) || (col("user.url").startsWith("ht"))))).groupBy(col("quoted_status.quoted_status_id")).agg(count(col("quoted_status.place"))).show(),
      (df: DataFrame) => df.where(((((col("retweeted_status.quoted_status.id_str").isNotNull) || (col("user.notifications").isNotNull)) && ((col("quoted_status.user.name").isNotNull) || (col("user.url").startsWith("ht")))) && ((col("user.profile_banner_url").isNotNull) && (((col("user.created_at").startsWith("S")) || (col("user.created_at").startsWith("T"))) || (col("user.created_at").startsWith("Fr")))))).groupBy(col("created_at")).count().show(),
      (df: DataFrame) => df.where(((((col("retweeted_status.quoted_status.id_str").isNotNull) || (col("user.notifications").isNotNull)) && ((col("quoted_status.user.name").isNotNull) || (col("user.url").startsWith("ht")))) && size(col("entities.media")) <= 1)).groupBy(col("quoted_status.possibly_sensitive")).agg(count(col("quoted_status.user.created_at"))).show(),
      (df: DataFrame) => df.where((((((col("retweeted_status.quoted_status.id_str").isNotNull) || (col("user.notifications").isNotNull)) && ((col("quoted_status.user.name").isNotNull) || (col("user.url").startsWith("ht")))) && size(col("entities.media")) <= 1) && ((((col("retweeted_status.quoted_status.user.url").startsWith("h")) || (col("quoted_status.place").isNotNull)) || (col("retweeted_status.quoted_status.user.profile_image_url").isNotNull)) || (col("retweeted_status.user.geo_enabled") === true)))).groupBy(col("retweeted_status.quoted_status.retweet_count")).agg(count(col("retweeted_status.entities.media"))).show(),
      (df: DataFrame) => df.where((((((col("retweeted_status.quoted_status.id_str").isNotNull) || (col("user.notifications").isNotNull)) && ((col("quoted_status.user.name").isNotNull) || (col("user.url").startsWith("ht")))) && size(col("entities.media")) <= 1) && ((((col("user.created_at").startsWith("Su")) || (col("user.created_at").startsWith("Thu"))) || (col("user.created_at").startsWith("Sat"))) || (col("user.created_at").startsWith("Th"))))).groupBy(col("is_quote_status")).agg(count(col("quoted_status.id"))).show(),
      (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status.id_str").isNotNull) || (col("user.notifications").isNotNull)) && ((col("quoted_status.user.name").isNotNull) || (col("user.url").startsWith("ht")))) && size(col("entities.media")) <= 1) && ((((col("user.created_at").startsWith("Su")) || (col("user.created_at").startsWith("Thu"))) || (col("user.created_at").startsWith("Sat"))) || (col("user.created_at").startsWith("Th")))) && ((((col("retweeted_status.quoted_status.place.bounding_box.type").isNotNull) || (col("retweeted_status.quoted_status.quoted_status_id").isNotNull)) || (col("quoted_status.user.default_profile_image").isNotNull)) || (((col("retweeted_status.user.created_at").startsWith("S")) || (col("retweeted_status.user.created_at").startsWith("T"))) || (col("retweeted_status.user.created_at").startsWith("Fr")))))).groupBy(col("quoted_status.quoted_status_id_str")).sum("quoted_status.in_reply_to_user_id").show(),
      (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status.id_str").isNotNull) || (col("user.notifications").isNotNull)) && ((col("quoted_status.user.name").isNotNull) || (col("user.url").startsWith("ht")))) && size(col("entities.media")) <= 1) && ((((col("user.created_at").startsWith("Su")) || (col("user.created_at").startsWith("Thu"))) || (col("user.created_at").startsWith("Sat"))) || (col("user.created_at").startsWith("Th")))) && (((((((col("retweeted_status.quoted_status.user.lang").startsWith("th")) || (col("retweeted_status.quoted_status.user.lang").startsWith("ja"))) || (col("retweeted_status.quoted_status.user.lang").startsWith("pt"))) && (col("retweeted_status.quoted_status.user.lang").startsWith("j"))) || (col("retweeted_status.quoted_status.user.lang").startsWith("en"))) || (col("quoted_status.quoted_status_id_str").startsWith("8"))) || (col("id") <= 860186628684536192.000000)))).groupBy(col("quoted_status.retweeted")).count().show(),

      // (df: DataFrame) => df.where(((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull))).groupBy(col("truncated")).agg(count(col("entities"))).show(),
      // (df: DataFrame) => df.where((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht"))))).groupBy(col("lang")).count().show(),
      // (df: DataFrame) => df.where(((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht")))) && ((((col("quoted_status.user.lang").isNotNull) || (col("retweeted_status.quoted_status.user.followers_count") === 78712813)) || (col("retweeted_status.extended_tweet.entities.hashtags").isNotNull)) || (col("retweeted_status.place.country_code").isNotNull)))).groupBy(col("user.verified")).count().show(),
      // // putt?
      // (df: DataFrame) => df.where((((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht")))) && ((((col("quoted_status.user.lang").isNotNull) || (col("retweeted_status.quoted_status.user.followers_count") === 78712813)) || (col("retweeted_status.extended_tweet.entities.hashtags").isNotNull)) || (col("retweeted_status.place.country_code").isNotNull))) && ((col("retweeted_status.quoted_status.scopes").isNotNull) || (col("quoted_status.created_at").isNotNull)))).groupBy(col("favorited")).agg(count(col("user.profile_text_color"))).show(),
      // (df: DataFrame) => df.where((((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht")))) && ((((col("quoted_status.user.lang").isNotNull) || (col("retweeted_status.quoted_status.user.followers_count") === 78712813)) || (col("retweeted_status.extended_tweet.entities.hashtags").isNotNull)) || (col("retweeted_status.place.country_code").isNotNull))) && (size(col("retweeted_status.quoted_status.extended_entities.media")) <= 1 || (col("quoted_status.user.created_at").isNotNull)))).groupBy(col("user.profile_sidebar_border_color")).agg(count(col("quoted_status.favorite_count"))).show(),
      // (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht")))) && ((((col("quoted_status.user.lang").isNotNull) || (col("retweeted_status.quoted_status.user.followers_count") === 78712813)) || (col("retweeted_status.extended_tweet.entities.hashtags").isNotNull)) || (col("retweeted_status.place.country_code").isNotNull))) && (size(col("retweeted_status.quoted_status.extended_entities.media")) <= 1 || (col("quoted_status.user.created_at").isNotNull))) && (((col("quoted_status.quoted_status_id_str").startsWith("858")) || (col("quoted_status.quoted_status_id_str").startsWith("8590"))) || (col("quoted_status.user.geo_enabled") === true)))).groupBy(col("quoted_status.in_reply_to_status_id")).count().show(),
      // // (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht")))) && ((((col("quoted_status.user.lang").isNotNull) || (col("retweeted_status.quoted_status.user.followers_count") === 78712813)) || (col("retweeted_status.extended_tweet.entities.hashtags").isNotNull)) || (col("retweeted_status.place.country_code").isNotNull))) && (size(col("retweeted_status.quoted_status.extended_entities.media")) <= 1 || (col("quoted_status.user.created_at").isNotNull))) && (col("retweeted_status.truncated") === false))).groupBy(col("retweet_count")).agg(count(col("retweeted_status.place.attributes"))).show(),
      // (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht")))) && ((((col("quoted_status.user.lang").isNotNull) || (col("retweeted_status.quoted_status.user.followers_count") === 78712813)) || (col("retweeted_status.extended_tweet.entities.hashtags").isNotNull)) || (col("retweeted_status.place.country_code").isNotNull))) && (size(col("retweeted_status.quoted_status.extended_entities.media")) <= 1 || (col("quoted_status.user.created_at").isNotNull))) && ((col("retweeted_status.quoted_status.user.created_at").startsWith("T")) || (col("retweeted_status.quoted_status.user.created_at").startsWith("Sa"))))).groupBy(col("possibly_sensitive")).agg(count(col("retweeted_status.user.profile_use_background_image"))).show(),
      // (df: DataFrame) => df.where((((((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht")))) && ((((col("quoted_status.user.lang").isNotNull) || (col("retweeted_status.quoted_status.user.followers_count") === 78712813)) || (col("retweeted_status.extended_tweet.entities.hashtags").isNotNull)) || (col("retweeted_status.place.country_code").isNotNull))) && (size(col("retweeted_status.quoted_status.extended_entities.media")) <= 1 || (col("quoted_status.user.created_at").isNotNull))) && ((col("retweeted_status.quoted_status.user.created_at").startsWith("T")) || (col("retweeted_status.quoted_status.user.created_at").startsWith("Sa")))) && (col("retweeted_status.quoted_status.user.verified") === true))).select(count(col("retweeted_status.quoted_status.in_reply_to_screen_name"))).show(),
      // (df: DataFrame) => df.where((((((((col("retweeted_status.quoted_status.in_reply_to_user_id").isNotNull) || (col("retweeted_status.coordinates").isNotNull)) && ((col("retweeted_status.favorite_count") === 1096967) || (col("user.profile_banner_url").startsWith("ht")))) && ((((col("quoted_status.user.lang").isNotNull) || (col("retweeted_status.quoted_status.user.followers_count") === 78712813)) || (col("retweeted_status.extended_tweet.entities.hashtags").isNotNull)) || (col("retweeted_status.place.country_code").isNotNull))) && (size(col("retweeted_status.quoted_status.extended_entities.media")) <= 1 || (col("quoted_status.user.created_at").isNotNull))) && ((col("retweeted_status.quoted_status.user.created_at").startsWith("T")) || (col("retweeted_status.quoted_status.user.created_at").startsWith("Sa")))) && ((col("user.verified") === true) || (col("quoted_status.truncated") === false)))).groupBy(col("quoted_status_id_str")).sum("favorite_count").show(),

      // (df: DataFrame) => df.where(((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht")))).groupBy(col("in_reply_to_user_id")).count().show(),
      // (df: DataFrame) => df.where((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht")))).groupBy(col("retweeted_status.user.profile_link_color")).agg(count(col("retweeted_status.user.follow_request_sent"))).show(),
      // (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht"))) && ((((((((col("quoted_status.user.description").startsWith("म")) || (col("quoted_status.user.description").startsWith("🐧"))) || (col("quoted_status.user.description").startsWith("う"))) || (col("quoted_status.user.description").startsWith("ก"))) || (col("quoted_status.user.description").startsWith("상"))) || (((((col("quoted_status.user.location").startsWith(" ")) || (col("quoted_status.user.location").startsWith("#"))) || (col("quoted_status.user.location").startsWith("A"))) || (col("quoted_status.user.location").startsWith("S"))) || (col("quoted_status.user.location").startsWith("C")))) || (col("retweeted_status.quoted_status.user.contributors_enabled") === false)) || (col("user.geo_enabled") === true)))).groupBy(col("quoted_status.text")).agg(count(col("retweeted_status.id"))).show(),
      // (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht"))) && (col("possibly_sensitive").isNotNull))).groupBy(col("quoted_status.user.lang")).count().show(),
      // (df: DataFrame) => df.where((((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht"))) && (col("possibly_sensitive").isNotNull)) && (col("quoted_status.entities").isNotNull))).groupBy(col("retweeted_status.lang")).count().show(),
      // (df: DataFrame) => df.where((((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht"))) && (col("possibly_sensitive").isNotNull)) && (((col("retweeted_status.quoted_status.geo.type").isNotNull) || ((col("retweeted_status.quoted_status.lang").startsWith("e")) || (col("retweeted_status.quoted_status.lang").startsWith("t")))) || (col("quoted_status.user.default_profile").isNotNull)))).groupBy(col("source")).agg(count(col("user"))).show(),
      // (df: DataFrame) => df.where(((((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht"))) && (col("possibly_sensitive").isNotNull)) && (((col("retweeted_status.quoted_status.geo.type").isNotNull) || ((col("retweeted_status.quoted_status.lang").startsWith("e")) || (col("retweeted_status.quoted_status.lang").startsWith("t")))) || (col("quoted_status.user.default_profile").isNotNull))) && (col("quoted_status.display_text_range").isNotNull))).groupBy(col("user.profile_background_tile")).agg(count(col("truncated"))).show(),
      // (df: DataFrame) => df.where((((((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht"))) && (col("possibly_sensitive").isNotNull)) && (((col("retweeted_status.quoted_status.geo.type").isNotNull) || ((col("retweeted_status.quoted_status.lang").startsWith("e")) || (col("retweeted_status.quoted_status.lang").startsWith("t")))) || (col("quoted_status.user.default_profile").isNotNull))) && (col("quoted_status.display_text_range").isNotNull)) && (((((((col("quoted_status.user.location").startsWith("M")) || (col("quoted_status.user.location").startsWith("P"))) || (col("quoted_status.user.location").startsWith("B"))) || (col("quoted_status.user.location").startsWith("O"))) || (col("quoted_status.user.location").startsWith("T"))) || (col("retweeted_status.extended_tweet.entities.symbols").isNotNull)) || (col("retweeted_status.quoted_status.user.profile_background_image_url_https").startsWith("ht"))))).select(count(col("quoted_status_id"))).show(),
      // (df: DataFrame) => df.where((((((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht"))) && (col("possibly_sensitive").isNotNull)) && (((col("retweeted_status.quoted_status.geo.type").isNotNull) || ((col("retweeted_status.quoted_status.lang").startsWith("e")) || (col("retweeted_status.quoted_status.lang").startsWith("t")))) || (col("quoted_status.user.default_profile").isNotNull))) && (col("quoted_status.display_text_range").isNotNull)) && ((col("quoted_status.geo.type").isNotNull) || ((((col("quoted_status.created_at").startsWith("S")) || (col("quoted_status.created_at").startsWith("T"))) || (col("quoted_status.created_at").startsWith("W"))) || (col("quoted_status.created_at").startsWith("Fr")))))).groupBy(col("user.statuses_count")).agg(count(col("in_reply_to_user_id"))).show(),
      // // (df: DataFrame) => df.where((((((((((col("retweeted_status.quoted_status_id") >= 666996895763915904.000000) || (col("quoted_status.favorite_count").isNotNull)) || (col("retweeted_status.quoted_status.is_quote_status") === false)) || (col("user.profile_background_image_url").startsWith("ht"))) && (col("retweeted_status.user.profile_image_url_https").startsWith("ht"))) && (col("possibly_sensitive").isNotNull)) && (((col("retweeted_status.quoted_status.geo.type").isNotNull) || ((col("retweeted_status.quoted_status.lang").startsWith("e")) || (col("retweeted_status.quoted_status.lang").startsWith("t")))) || (col("quoted_status.user.default_profile").isNotNull))) && (col("quoted_status.display_text_range").isNotNull)) && size(array(col("retweeted_status.quoted_status.extended_entities.*"))) <= 1)).groupBy(col("quoted_status.id_str")).agg(count(col("quoted_status.user"))).show(),

      // (df: DataFrame) => df.where((col("retweeted_status.retweeted").isNotNull)).groupBy(col("user.profile_image_url_https")).count().show(),
      // (df: DataFrame) => df.where(((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull))).groupBy(col("delete.timestamp_ms")).count().show(),
      // (df: DataFrame) => df.where((((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull)) && (((col("quoted_status.extended_entities.media").isNotNull) || (col("retweeted_status.quoted_status.user.profile_image_url").startsWith("h"))) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("h"))))).groupBy(col("id")).agg(count(col("in_reply_to_screen_name"))).show(),
      // (df: DataFrame) => df.where(((((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull)) && (((col("quoted_status.extended_entities.media").isNotNull) || (col("retweeted_status.quoted_status.user.profile_image_url").startsWith("h"))) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("h")))) && (col("retweeted_status.user.default_profile") === true))).groupBy(col("quoted_status.favorited")).count().show(),
      // (df: DataFrame) => df.where((((((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull)) && (((col("quoted_status.extended_entities.media").isNotNull) || (col("retweeted_status.quoted_status.user.profile_image_url").startsWith("h"))) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("h")))) && (col("retweeted_status.user.default_profile") === true)) && (((col("retweeted_status.quoted_status.extended_tweet.entities.user_mentions").isNotNull) || (col("retweeted_status.extended_tweet.entities.media").isNotNull)) || (col("quoted_status.user.description").isNotNull)))).groupBy(col("retweeted_status.quoted_status.user.profile_image_url_https")).sum("retweeted_status.quoted_status.id").show(),
      // (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull)) && (((col("quoted_status.extended_entities.media").isNotNull) || (col("retweeted_status.quoted_status.user.profile_image_url").startsWith("h"))) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("h")))) && (col("retweeted_status.user.default_profile") === true)) && (((col("retweeted_status.quoted_status.extended_tweet.entities.user_mentions").isNotNull) || (col("retweeted_status.extended_tweet.entities.media").isNotNull)) || (col("quoted_status.user.description").isNotNull))) && (col("quoted_status.entities.user_mentions").isNotNull))).groupBy(col("retweeted_status.lang")).agg(count(col("favorited"))).show(),
      // (df: DataFrame) => df.where(((((((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull)) && (((col("quoted_status.extended_entities.media").isNotNull) || (col("retweeted_status.quoted_status.user.profile_image_url").startsWith("h"))) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("h")))) && (col("retweeted_status.user.default_profile") === true)) && (((col("retweeted_status.quoted_status.extended_tweet.entities.user_mentions").isNotNull) || (col("retweeted_status.extended_tweet.entities.media").isNotNull)) || (col("quoted_status.user.description").isNotNull))) && (col("retweeted_status.quoted_status.truncated").isNotNull))).groupBy(col("quoted_status.is_quote_status")).count().show(),
      // (df: DataFrame) => df.where((((((((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull)) && (((col("quoted_status.extended_entities.media").isNotNull) || (col("retweeted_status.quoted_status.user.profile_image_url").startsWith("h"))) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("h")))) && (col("retweeted_status.user.default_profile") === true)) && (((col("retweeted_status.quoted_status.extended_tweet.entities.user_mentions").isNotNull) || (col("retweeted_status.extended_tweet.entities.media").isNotNull)) || (col("quoted_status.user.description").isNotNull))) && (col("retweeted_status.quoted_status.truncated").isNotNull)) && ((((col("retweeted_status.quoted_status.in_reply_to_status_id") === 746262948003259230L) || (col("retweeted_status.quoted_status.place.full_name").isNotNull)) || ((col("user.profile_background_color").startsWith("0")) || (col("user.profile_background_color").startsWith("F")))) || (col("quoted_status.extended_tweet.display_text_range").isNotNull)))).groupBy(col("quoted_status.filter_level")).agg(count(col("retweeted_status.user.id"))).show(),
      // (df: DataFrame) => df.where(((((((((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull)) && (((col("quoted_status.extended_entities.media").isNotNull) || (col("retweeted_status.quoted_status.user.profile_image_url").startsWith("h"))) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("h")))) && (col("retweeted_status.user.default_profile") === true)) && (((col("retweeted_status.quoted_status.extended_tweet.entities.user_mentions").isNotNull) || (col("retweeted_status.extended_tweet.entities.media").isNotNull)) || (col("quoted_status.user.description").isNotNull))) && (col("retweeted_status.quoted_status.truncated").isNotNull)) && ((((col("retweeted_status.quoted_status.in_reply_to_status_id") === 746262948003259230L) || (col("retweeted_status.quoted_status.place.full_name").isNotNull)) || ((col("user.profile_background_color").startsWith("0")) || (col("user.profile_background_color").startsWith("F")))) || (col("quoted_status.extended_tweet.display_text_range").isNotNull))) && (col("user.profile_background_image_url").startsWith("h")))).groupBy(col("quoted_status.user.created_at")).agg(count(col("retweeted_status.truncated"))).show(),
      // //(df: DataFrame) => df.where(((((((((col("retweeted_status.quoted_status.user.geo_enabled") === true) || (col("user.profile_image_url").isNotNull)) && (((col("quoted_status.extended_entities.media").isNotNull) || (col("retweeted_status.quoted_status.user.profile_image_url").startsWith("h"))) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("h")))) && (col("retweeted_status.user.default_profile") === true)) && (((col("retweeted_status.quoted_status.extended_tweet.entities.user_mentions").isNotNull) || (col("retweeted_status.extended_tweet.entities.media").isNotNull)) || (col("quoted_status.user.description").isNotNull))) && (col("retweeted_status.quoted_status.truncated").isNotNull)) && ((((col("retweeted_status.quoted_status.in_reply_to_status_id") === 746262948003259230L) || (col("retweeted_status.quoted_status.place.full_name").isNotNull)) || ((col("user.profile_background_color").startsWith("0")) || (col("user.profile_background_color").startsWith("F")))) || (col("quoted_status.extended_tweet.display_text_range").isNotNull))) && ((((col("quoted_status.place.attributes").isNotNull) || (col("retweeted_status.user.verified") === true)) || (col("retweeted_status.id_str").startsWith("859"))) || (col("retweeted_status.place.url").isNotNull)))).groupBy(col("retweeted")).agg(count(col("quoted_status.in_reply_to_status_id_str"))).show(),

      (df: DataFrame) => df.where((col("retweeted_status.user.verified") === false)).groupBy(col("retweet_count")).count().show(),
      (df: DataFrame) => df.where(((col("retweeted_status.user.verified") === false) && (((size(col("quoted_status.contributors")) <= 1 || (col("retweeted_status.quoted_status.user.id") >= 747470416785397760.000000)) || (((((col("retweeted_status.quoted_status.place.country_code").startsWith("A")) || (col("retweeted_status.quoted_status.place.country_code").startsWith("B"))) || (col("retweeted_status.quoted_status.place.country_code").startsWith("P"))) || (col("retweeted_status.quoted_status.place.country_code").startsWith("M"))) || (col("retweeted_status.quoted_status.place.country_code").startsWith("I")))) || ((((col("retweeted_status.created_at").startsWith("T")) || (col("retweeted_status.created_at").startsWith("S"))) || (col("retweeted_status.created_at").startsWith("Fr"))) || (col("retweeted_status.created_at").startsWith("Wed")))))).groupBy(col("possibly_sensitive")).agg(count(col("retweeted_status.quoted_status.extended_entities"))).show(),
      (df: DataFrame) => df.where(((col("retweeted_status.user.verified") === false) && ((col("retweeted_status.extended_tweet.entities.media").isNotNull) || (col("retweeted_status.user.profile_background_tile") === false)))).groupBy(col("id")).count().show(),
      (df: DataFrame) => df.where(((col("retweeted_status.user.verified") === false) && (col("user.profile_use_background_image") === true))).groupBy(col("quoted_status.place.country")).count().show(),
      (df: DataFrame) => df.where((((col("retweeted_status.user.verified") === false) && (col("user.profile_use_background_image") === true)) && (col("entities.media").isNotNull))).groupBy(col("id_str")).count().show(),
      (df: DataFrame) => df.where((((col("retweeted_status.user.verified") === false) && (col("user.profile_use_background_image") === true)) && ((col("quoted_status.place").isNotNull) || (col("retweeted_status.entities.media").isNotNull)))).groupBy(col("user.id_str")).agg(count(col("quoted_status.quoted_status_id_str"))).show(),
      (df: DataFrame) => df.where((((col("retweeted_status.user.verified") === false) && (col("user.profile_use_background_image") === true)) && (((col("quoted_status.in_reply_to_screen_name").isNotNull) || ((col("retweeted_status.in_reply_to_user_id_str").startsWith("2")) || (col("retweeted_status.in_reply_to_user_id_str").startsWith("5")))) || (col("user.listed_count") <= 28084.399905)))).groupBy(col("user.profile_banner_url")).agg(count(col("user.notifications"))).show(),
      (df: DataFrame) => df.where(((((col("retweeted_status.user.verified") === false) && (col("user.profile_use_background_image") === true)) && (((col("quoted_status.in_reply_to_screen_name").isNotNull) || ((col("retweeted_status.in_reply_to_user_id_str").startsWith("2")) || (col("retweeted_status.in_reply_to_user_id_str").startsWith("5")))) || (col("user.listed_count") <= 28084.399905))) && ((col("retweeted_status.id_str").startsWith("6")) || (col("retweeted_status.user.default_profile") === true)))).groupBy(col("user.profile_sidebar_border_color")).count().show(),
      (df: DataFrame) => df.where(((((col("retweeted_status.user.verified") === false) && (col("user.profile_use_background_image") === true)) && (((col("quoted_status.in_reply_to_screen_name").isNotNull) || ((col("retweeted_status.in_reply_to_user_id_str").startsWith("2")) || (col("retweeted_status.in_reply_to_user_id_str").startsWith("5")))) || (col("user.listed_count") <= 28084.399905))) && ((col("retweeted_status.quoted_status.extended_tweet").isNotNull) || (col("user.geo_enabled") === false)))).select(count(col("retweeted_status.user.id"))).show(),
      (df: DataFrame) => df.where((((((col("retweeted_status.user.verified") === false) && (col("user.profile_use_background_image") === true)) && (((col("quoted_status.in_reply_to_screen_name").isNotNull) || ((col("retweeted_status.in_reply_to_user_id_str").startsWith("2")) || (col("retweeted_status.in_reply_to_user_id_str").startsWith("5")))) || (col("user.listed_count") <= 28084.399905))) && ((col("retweeted_status.quoted_status.extended_tweet").isNotNull) || (col("user.geo_enabled") === false))) && (((col("retweeted_status.quoted_status.in_reply_to_user_id_str").isNotNull) || (col("quoted_status.is_quote_status") === true)) || (col("retweeted_status.user.profile_background_image_url_https").startsWith("ht"))))).select(count(col("favorited"))).show()
    ))
  }
}
