package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.util.UUID

object PrepareTPCDS {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    implicit val spark = jobConfig.makeSparkSession(s"Prepare TPC-DS ${jobConfig.inputPath} to ${jobConfig.wavesPath}")

    // Dimension Tables
    val customer_address = readTable("customer_address", customerAddressSchema, jobConfig.inputPath)
    val customer_demographics = readTable("customer_demographics", customerDemographicsSchema, jobConfig.inputPath)
    val date_dim = readTable("date_dim", dateDimSchema, jobConfig.inputPath)
    val income_band = readTable("income_band", incomeBandSchema, jobConfig.inputPath)
    val item = readTable("item", itemSchema, jobConfig.inputPath)
    val reason = readTable("reason", reasonSchema, jobConfig.inputPath)
    val ship_mode = readTable("ship_mode", shipModeSchema, jobConfig.inputPath)
    val time_dim = readTable("time_dim", timeDimSchema, jobConfig.inputPath)
    val warehouse = readTable("warehouse", warehouseSchema, jobConfig.inputPath)

    val call_center = readTable("call_center", callCenterSchema, jobConfig.inputPath)
      .transform(embed("closed_date_sk", "date_sk", date_dim))
      .transform(embed("open_date_sk", "date_sk", date_dim))
    val catalog_page = readTable("catalog_page", catalogPageSchema, jobConfig.inputPath)
      .transform(embed("start_date_sk", "date_sk", date_dim))
      .transform(embed("end_date_sk", "date_sk", date_dim))
    val household_demographics = readTable("household_demographics", householdDemographicsSchema, jobConfig.inputPath)
      .transform(embed("income_band_sk", "income_band_sk", income_band))
    val promotion = readTable("promotion", promotionSchema, jobConfig.inputPath)
      .transform(embed("start_date_sk", "date_sk", date_dim))
      .transform(embed("end_date_sk", "date_sk", date_dim))
      .transform(embed("item_sk", "item_sk", item))
    val store = readTable("store", storeSchema, jobConfig.inputPath)
      .transform(embed("closed_date_sk", "date_sk", date_dim))
    val web_site = readTable("web_site", webSiteSchema, jobConfig.inputPath)
      .transform(embed("open_date_sk", "date_sk", date_dim))
      .transform(embed("close_date_sk", "date_sk", date_dim))

    val customer = readTable("customer", customerSchema, jobConfig.inputPath)
      .transform(embed("current_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("current_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("current_addr_sk", "address_sk", customer_address))
      .transform(embed("first_shipto_date_sk", "date_sk", date_dim))
      .transform(embed("first_sales_date_sk", "date_sk", date_dim))
      .transform(embed("last_review_date_sk", "date_sk", date_dim))

    val web_page = readTable("web_page", webPageSchema, jobConfig.inputPath)
      .transform(embed("creation_date_sk", "date_sk", date_dim))
      .transform(embed("access_date_sk", "date_sk", date_dim))
      .transform(embed("customer_sk", "customer_sk", customer))

    // Fact Tables
    val catalog_sales = readTable("catalog_sales", catalogSalesSchema, jobConfig.inputPath, true)
      .transform(embed("sold_date_sk", "date_sk", date_dim))
      .transform(embed("sold_time_sk", "time_sk", time_dim))
      .transform(embed("ship_date_sk", "date_sk", date_dim))
      .transform(embed("bill_customer_sk", "customer_sk", customer))
      .transform(embed("bill_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("bill_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("bill_addr_sk", "address_sk", customer_address))
      .transform(embed("ship_customer_sk", "customer_sk", customer))
      .transform(embed("ship_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("ship_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("ship_addr_sk", "address_sk", customer_address))
      .transform(embed("call_center_sk", "call_center_sk", call_center))
      .transform(embed("catalog_page_sk", "catalog_page_sk", catalog_page))
      .transform(embed("ship_mode_sk", "ship_mode_sk", ship_mode))
      .transform(embed("warehouse_sk", "warehouse_sk", warehouse))
      .transform(embed("item_sk", "item_sk", item))
      .transform(embed("promo_sk", "promo_sk", promotion))

    val catalog_returns = readTable("catalog_returns", catalogReturnsSchema, jobConfig.inputPath, true)
      .transform(embed("returned_date_sk", "date_sk", date_dim))
      .transform(embed("returned_time_sk", "time_sk", time_dim))
      .transform(embed("item_sk", "item_sk", item))
      .transform(embed("refunded_customer_sk", "customer_sk", customer))
      .transform(embed("refunded_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("refunded_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("refunded_addr_sk", "address_sk", customer_address))
      .transform(embed("returning_customer_sk", "customer_sk", customer))
      .transform(embed("returning_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("returning_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("returning_addr_sk", "address_sk", customer_address))
      .transform(embed("call_center_sk", "call_center_sk", call_center))
      .transform(embed("catalog_page_sk", "catalog_page_sk", catalog_page))
      .transform(embed("ship_mode_sk", "ship_mode_sk", ship_mode))
      .transform(embed("warehouse_sk", "warehouse_sk", warehouse))
      .transform(embed("reason_sk", "reason_sk", reason))

    val inventory = readTable("inventory", inventorySchema, jobConfig.inputPath, true)
      .transform(embed("date_sk", "date_sk", date_dim))
      .transform(embed("item_sk", "item_sk", item))
      .transform(embed("warehouse_sk", "warehouse_sk", warehouse))

    val store_sales = readTable("store_sales", storeSalesSchema, jobConfig.inputPath, true)
      .transform(embed("sold_date_sk", "date_sk", date_dim))
      .transform(embed("sold_time_sk", "time_sk", time_dim))
      .transform(embed("item_sk", "item_sk", item))
      .transform(embed("customer_sk", "customer_sk", customer))
      .transform(embed("cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("addr_sk", "address_sk", customer_address))
      .transform(embed("store_sk", "store_sk", store))
      .transform(embed("promo_sk", "promo_sk", promotion))

    val store_returns = readTable("store_returns", storeReturnsSchema, jobConfig.inputPath, true)
      .transform(embed("returned_date_sk", "date_sk", date_dim))
      .transform(embed("return_time_sk", "time_sk", time_dim))
      .transform(embed("item_sk", "item_sk", item))
      .transform(embed("customer_sk", "customer_sk", customer))
      .transform(embed("cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("addr_sk", "address_sk", customer_address))
      .transform(embed("store_sk", "store_sk", store))
      .transform(embed("reason_sk", "reason_sk", reason))

    val web_sales = readTable("web_sales", webSalesSchema, jobConfig.inputPath, true)
      .transform(embed("sold_date_sk", "date_sk", date_dim))
      .transform(embed("sold_time_sk", "time_sk", time_dim))
      .transform(embed("ship_date_sk", "date_sk", date_dim))
      .transform(embed("item_sk", "item_sk", item))
      .transform(embed("bill_customer_sk", "customer_sk", customer))
      .transform(embed("bill_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("bill_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("bill_addr_sk", "address_sk", customer_address))
      .transform(embed("ship_customer_sk", "customer_sk", customer))
      .transform(embed("ship_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("ship_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("ship_addr_sk", "address_sk", customer_address))
      .transform(embed("web_page_sk", "web_page_sk", web_page))
      .transform(embed("web_site_sk", "site_sk", web_site))
      .transform(embed("ship_mode_sk", "ship_mode_sk", ship_mode))
      .transform(embed("warehouse_sk", "warehouse_sk", warehouse))
      .transform(embed("promo_sk", "promo_sk", promotion))

    val web_returns = readTable("web_returns", webReturnsSchema, jobConfig.inputPath, true)
      .transform(embed("returned_date_sk", "date_sk", date_dim))
      .transform(embed("returned_time_sk", "time_sk", time_dim))
      .transform(embed("item_sk", "item_sk", item))
      .transform(embed("refunded_customer_sk", "customer_sk", customer))
      .transform(embed("refunded_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("refunded_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("refunded_addr_sk", "address_sk", customer_address))
      .transform(embed("returning_customer_sk", "customer_sk", customer))
      .transform(embed("returning_cdemo_sk", "demo_sk", customer_demographics))
      .transform(embed("returning_hdemo_sk", "demo_sk", household_demographics))
      .transform(embed("returning_addr_sk", "address_sk", customer_address))
      .transform(embed("web_page_sk", "web_page_sk", web_page))
      .transform(embed("reason_sk", "reason_sk", reason))

    merge(catalog_sales, catalog_returns, inventory, store_sales, store_returns, web_sales, web_returns)
      .write
      .json(jobConfig.wavesPath)
  }

  def readTable(name: String, schema: StructType, path: String, withTypeTag: Boolean = false)(implicit spark: SparkSession): DataFrame = {
    var data = spark.read.option("delimiter", "|").schema(schema).csv(s"${path}/$name.dat")
    if (withTypeTag)
      data = data.withColumn("TypeTag", typedLit(name))
    data
  }

  def embed(referencingColumn: String, referencedColumn: String, referencedDataFrame: DataFrame)(subject: DataFrame) = {
    val referencedAlias = UUID.randomUUID().toString()
    val aliasedReferencedDataFrame = referencedDataFrame.as(referencedAlias)

    val subjectAlias = UUID.randomUUID().toString()
    val aliasedSubject = subject.as(subjectAlias)

    val nested = struct(aliasedReferencedDataFrame.schema.fields.map(field => col(s"$referencedAlias.${field.name}")):_*)
    val conditional = when(col(s"$subjectAlias.$referencingColumn").isNotNull, nested).otherwise(typedLit(null)).as(referencingColumn)

    val joinColumnIndex = aliasedSubject.schema.fieldIndex(referencingColumn)
    val before = aliasedSubject.schema.fields.slice(0, joinColumnIndex).map(field => col(s"$subjectAlias.${field.name}"))
    val after = aliasedSubject.schema.fields.slice(joinColumnIndex+1, aliasedSubject.schema.size).map(field => col(s"$subjectAlias.${field.name}"))
    val embeddedColumns = before ++ (conditional +: after)

    aliasedSubject
      .join(aliasedReferencedDataFrame, col(s"$subjectAlias.$referencingColumn") === col(s"$referencedAlias.$referencedColumn"), "leftouter")
      .select(embeddedColumns:_*)
  }

  def merge(df: DataFrame*)
    = df.reduce((lhs, rhs) => lhs.unionByName(rhs, true)).orderBy(rand())

  val customerAddressSchema = StructType(Array(
    StructField("address_sk",              IntegerType),
    StructField("address_id",              StringType),
    StructField("street_number",           StringType),
    StructField("street_name",             StringType),
    StructField("street_type",             StringType),
    StructField("suite_number",            StringType),
    StructField("city",                    StringType),
    StructField("county",                  StringType),
    StructField("state",                   StringType),
    StructField("zip",                     StringType),
    StructField("country",                 StringType),
    StructField("gmt_offset",              DecimalType(5,2)),
    StructField("location_type",           StringType)
  ))

  val customerDemographicsSchema = StructType(Array(
    StructField("demo_sk",                 IntegerType),
    StructField("gender",                  StringType),
    StructField("marital_status",          StringType),
    StructField("education_status",        StringType),
    StructField("purchase_estimate",       IntegerType),
    StructField("credit_rating",           StringType),
    StructField("dep_count",               IntegerType),
    StructField("dep_employed_count",      IntegerType),
    StructField("dep_college_count",       IntegerType)
  ))

  val dateDimSchema = StructType(Array(
    StructField("date_sk",                 IntegerType),
    StructField("date_id",                 StringType),
    StructField("date",                    DateType),
    StructField("month_seq",               IntegerType),
    StructField("week_seq",                IntegerType),
    StructField("quarter_seq",             IntegerType),
    StructField("year",                    IntegerType),
    StructField("dow",                     IntegerType),
    StructField("moy",                     IntegerType),
    StructField("dom",                     IntegerType),
    StructField("qoy",                     IntegerType),
    StructField("fy_year",                 IntegerType),
    StructField("fy_quarter_seq",          IntegerType),
    StructField("fy_week_seq",             IntegerType),
    StructField("day_name",                StringType),
    StructField("quarter_name",            StringType),
    StructField("holiday",                 StringType),
    StructField("weekend",                 StringType),
    StructField("following_holiday",       StringType),
    StructField("first_dom",               IntegerType),
    StructField("last_dom",                IntegerType),
    StructField("same_day_ly",             IntegerType),
    StructField("same_day_lq",             IntegerType),
    StructField("current_day",             StringType),
    StructField("current_week",            StringType),
    StructField("current_month",           StringType),
    StructField("current_quarter",         StringType),
    StructField("current_year",            StringType)
  ))

  val incomeBandSchema = StructType(Array(
    StructField("income_band_sk",          IntegerType),
    StructField("lower_bound",             IntegerType),
    StructField("upper_bound",             IntegerType)
  ))

  val itemSchema = StructType(Array(
    StructField("item_sk",                 IntegerType),
    StructField("item_id",                 StringType),
    StructField("rec_start_date",          DateType),
    StructField("rec_end_date",            DateType),
    StructField("item_desc",               StringType),
    StructField("current_price",           DecimalType(7,2)),
    StructField("wholesale_cost",          DecimalType(7,2)),
    StructField("brand_id",                IntegerType),
    StructField("brand",                   StringType),
    StructField("class_id",                IntegerType),
    StructField("class",                   StringType),
    StructField("category_id",             IntegerType),
    StructField("category",                StringType),
    StructField("manufact_id",             IntegerType),
    StructField("manufact",                StringType),
    StructField("size",                    StringType),
    StructField("formulation",             StringType),
    StructField("color",                   StringType),
    StructField("units",                   StringType),
    StructField("container",               StringType),
    StructField("manager_id",              IntegerType),
    StructField("product_name",            StringType)
  ))

  val reasonSchema = StructType(Array(
    StructField("reason_sk",               IntegerType),
    StructField("reason_id",               StringType),
    StructField("reason_desc",             StringType)
  ))

  val shipModeSchema = StructType(Array(
    StructField("ship_mode_sk",            IntegerType),
    StructField("ship_mode_id",            StringType),
    StructField("type",                    StringType),
    StructField("code",                    StringType),
    StructField("carrier",                 StringType),
    StructField("contract",                StringType)
  ))

  val timeDimSchema = StructType(Array(
    StructField("time_sk",                 IntegerType),
    StructField("time_id",                 StringType),
    StructField("time",                    IntegerType),
    StructField("hour",                    IntegerType),
    StructField("minute",                  IntegerType),
    StructField("second",                  IntegerType),
    StructField("am_pm",                   StringType),
    StructField("shift",                   StringType),
    StructField("sub_shift",               StringType),
    StructField("meal_time",               StringType)
  ))

  val warehouseSchema = StructType(Array(
    StructField("warehouse_sk",           IntegerType),
    StructField("warehouse_id",           StringType),
    StructField("warehouse_name",         StringType),
    StructField("warehouse_sq_ft",        IntegerType),
    StructField("street_number",          StringType),
    StructField("street_name",            StringType),
    StructField("street_type",            StringType),
    StructField("suite_number",           StringType),
    StructField("city",                   StringType),
    StructField("county",                 StringType),
    StructField("state",                  StringType),
    StructField("zip",                    StringType),
    StructField("country",                StringType),
    StructField("gmt_offset",             DecimalType(5,2))
  ))



  val callCenterSchema = StructType(Array(
    StructField("call_center_sk",         IntegerType),
    StructField("call_center_id",         StringType),
    StructField("rec_start_date",         DateType),
    StructField("rec_end_date",           DateType),
    StructField("closed_date_sk",         IntegerType),
    StructField("open_date_sk",           IntegerType),
    StructField("name",                   StringType),
    StructField("class",                  StringType),
    StructField("employees",              IntegerType),
    StructField("sq_ft",                  IntegerType),
    StructField("hours",                  StringType),
    StructField("manager",                StringType),
    StructField("mkt_id",                 IntegerType),
    StructField("mkt_class",              StringType),
    StructField("mkt_desc",               StringType),
    StructField("market_manager",         StringType),
    StructField("division",               IntegerType),
    StructField("division_name",          StringType),
    StructField("company",                IntegerType),
    StructField("company_name",           StringType),
    StructField("street_number",          StringType),
    StructField("street_name",            StringType),
    StructField("street_type",            StringType),
    StructField("suite_number",           StringType),
    StructField("city",                   StringType),
    StructField("county",                 StringType),
    StructField("state",                  StringType),
    StructField("zip",                    StringType),
    StructField("country",                StringType),
    StructField("gmt_offset",             DecimalType(5,2)),
    StructField("tax_percentage",         DecimalType(5,2))
  ))
  
  val catalogPageSchema = StructType(Array(
    StructField("catalog_page_sk",        IntegerType),
    StructField("catalog_page_id",        StringType),
    StructField("start_date_sk",          IntegerType),
    StructField("end_date_sk",            IntegerType),
    StructField("department",             StringType),
    StructField("catalog_number",         IntegerType),
    StructField("catalog_page_number",    IntegerType),
    StructField("description",            StringType),
    StructField("type",                   StringType)
  ))

  val householdDemographicsSchema = StructType(Array(
    StructField("demo_sk",                 IntegerType),
    StructField("income_band_sk",          IntegerType),
    StructField("buy_potential",           StringType),
    StructField("dep_count",               IntegerType),
    StructField("vehicle_count",           IntegerType)
  ))

  val promotionSchema = StructType(Array(
    StructField("promo_sk",                IntegerType),
    StructField("promo_id",                StringType),
    StructField("start_date_sk",           IntegerType),
    StructField("end_date_sk",             IntegerType),
    StructField("item_sk",                 IntegerType),
    StructField("cost",                    DecimalType(15,2)),
    StructField("response_target",         IntegerType),
    StructField("promo_name",              StringType),
    StructField("channel_dmail",           StringType),
    StructField("channel_email",           StringType),
    StructField("channel_catalog",         StringType),
    StructField("channel_tv",              StringType),
    StructField("channel_radio",           StringType),
    StructField("channel_press",           StringType),
    StructField("channel_event",           StringType),
    StructField("channel_demo",            StringType),
    StructField("channel_details",         StringType),
    StructField("purpose",                 StringType),
    StructField("discount_active",         StringType)
  ))

  val storeSchema = StructType(Array(
    StructField("store_sk",                IntegerType),
    StructField("store_id",                StringType),
    StructField("rec_start_date",          DateType),
    StructField("rec_end_date",            DateType),
    StructField("closed_date_sk",          IntegerType),
    StructField("store_name",              StringType),
    StructField("number_employees",        IntegerType),
    StructField("floor_space",             IntegerType),
    StructField("hours",                   StringType),
    StructField("manager",                 StringType),
    StructField("market_id",               IntegerType),
    StructField("geography_class",         StringType),
    StructField("market_desc",             StringType),
    StructField("market_manager",          StringType),
    StructField("division_id",             IntegerType),
    StructField("division_name",           StringType),
    StructField("company_id",              IntegerType),
    StructField("company_name",            StringType),
    StructField("street_number",           StringType),
    StructField("street_name",             StringType),
    StructField("street_type",             StringType),
    StructField("suite_number",            StringType),
    StructField("city",                    StringType),
    StructField("county",                  StringType),
    StructField("state",                   StringType),
    StructField("zip",                     StringType),
    StructField("country",                 StringType),
    StructField("gmt_offset",              DecimalType(5,2)),
    StructField("tax_precentage",          DecimalType(5,2))
  ))

  val webSiteSchema = StructType(Array(
    StructField("site_sk",                 IntegerType),
    StructField("site_id",                 StringType),
    StructField("rec_start_date",          DateType),
    StructField("rec_end_date",            DateType),
    StructField("name",                    StringType),
    StructField("open_date_sk",            IntegerType),
    StructField("close_date_sk",           IntegerType),
    StructField("class",                   StringType),
    StructField("manager",                 StringType),
    StructField("mkt_id",                  IntegerType),
    StructField("mkt_class",               StringType),
    StructField("mkt_desc",                StringType),
    StructField("market_manager",          StringType),
    StructField("company_id",              IntegerType),
    StructField("company_name",            StringType),
    StructField("street_number",           StringType),
    StructField("street_name",             StringType),
    StructField("street_type",             StringType),
    StructField("suite_number",            StringType),
    StructField("city",                    StringType),
    StructField("county",                  StringType),
    StructField("state",                   StringType),
    StructField("zip",                     StringType),
    StructField("country",                 StringType),
    StructField("gmt_offset",              DecimalType(5,2)),
    StructField("tax_percentage",          DecimalType(5,2))
  ))



  val customerSchema = StructType(Array(
    StructField("customer_sk",             IntegerType),
    StructField("customer_id",             StringType),
    StructField("current_cdemo_sk",        IntegerType),
    StructField("current_hdemo_sk",        IntegerType),
    StructField("current_addr_sk",         IntegerType),
    StructField("first_shipto_date_sk",    IntegerType),
    StructField("first_sales_date_sk",     IntegerType),
    StructField("salutation",              StringType),
    StructField("first_name",              StringType),
    StructField("last_name",               StringType),
    StructField("preferred_cust_flag",     StringType),
    StructField("birth_day",               IntegerType),
    StructField("birth_month",             IntegerType),
    StructField("birth_year",              IntegerType),
    StructField("birth_country",           StringType),
    StructField("login",                   StringType),
    StructField("email_address",           StringType),
    StructField("last_review_date_sk",     StringType)
  ))



  val webPageSchema = StructType(Array(
    StructField("web_page_sk",             IntegerType),
    StructField("web_page_id",             StringType),
    StructField("rec_start_date",          DateType),
    StructField("rec_end_date",            DateType),
    StructField("creation_date_sk",        IntegerType),
    StructField("access_date_sk",          IntegerType),
    StructField("autogen_flag",            StringType),
    StructField("customer_sk",             IntegerType),
    StructField("url",                     StringType),
    StructField("type",                    StringType),
    StructField("char_count",              IntegerType),
    StructField("link_count",              IntegerType),
    StructField("image_count",             IntegerType),
    StructField("max_ad_count",            IntegerType)
  ))



  val catalogSalesSchema = StructType(Array(
    StructField("sold_date_sk",            IntegerType),
    StructField("sold_time_sk",            IntegerType),
    StructField("ship_date_sk",            IntegerType),
    StructField("bill_customer_sk",        IntegerType),
    StructField("bill_cdemo_sk",           IntegerType),
    StructField("bill_hdemo_sk",           IntegerType),
    StructField("bill_addr_sk",            IntegerType),
    StructField("ship_customer_sk",        IntegerType),
    StructField("ship_cdemo_sk",           IntegerType),
    StructField("ship_hdemo_sk",           IntegerType),
    StructField("ship_addr_sk",            IntegerType),
    StructField("call_center_sk",          IntegerType),
    StructField("catalog_page_sk",         IntegerType),
    StructField("ship_mode_sk",            IntegerType),
    StructField("warehouse_sk",            IntegerType),
    StructField("item_sk",                 IntegerType),
    StructField("promo_sk",                IntegerType),
    StructField("order_number",            LongType),
    StructField("quantity",                IntegerType),
    StructField("wholesale_cost",          DecimalType(7,2)),
    StructField("list_price",              DecimalType(7,2)),
    StructField("sales_price",             DecimalType(7,2)),
    StructField("ext_discount_amt",        DecimalType(7,2)),
    StructField("ext_sales_price",         DecimalType(7,2)),
    StructField("ext_wholesale_cost",      DecimalType(7,2)),
    StructField("ext_list_price",          DecimalType(7,2)),
    StructField("ext_tax",                 DecimalType(7,2)),
    StructField("coupon_amt",              DecimalType(7,2)),
    StructField("ext_ship_cost",           DecimalType(7,2)),
    StructField("net_paid",                DecimalType(7,2)),
    StructField("net_paid_inc_tax",        DecimalType(7,2)),
    StructField("net_paid_inc_ship",       DecimalType(7,2)),
    StructField("net_paid_inc_ship_tax",   DecimalType(7,2)),
    StructField("net_profit",              DecimalType(7,2))
  ))

  val catalogReturnsSchema = StructType(Array(
    StructField("returned_date_sk",      IntegerType),
    StructField("returned_time_sk",      IntegerType),
    StructField("item_sk",               IntegerType),
    StructField("refunded_customer_sk",  IntegerType),
    StructField("refunded_cdemo_sk",     IntegerType),
    StructField("refunded_hdemo_sk",     IntegerType),
    StructField("refunded_addr_sk",      IntegerType),
    StructField("returning_customer_sk", IntegerType),
    StructField("returning_cdemo_sk",    IntegerType),
    StructField("returning_hdemo_sk",    IntegerType),
    StructField("returning_addr_sk",     IntegerType),
    StructField("call_center_sk",        IntegerType),
    StructField("catalog_page_sk",       IntegerType),
    StructField("ship_mode_sk",          IntegerType),
    StructField("warehouse_sk",          IntegerType),
    StructField("reason_sk",             IntegerType),
    StructField("order_number",          LongType),
    StructField("return_quantity",       IntegerType),
    StructField("return_amount",         DecimalType(7,2)),
    StructField("return_tax",            DecimalType(7,2)),
    StructField("return_amt_inc_tax",    DecimalType(7,2)),
    StructField("fee",                   DecimalType(7,2)),
    StructField("return_ship_cost",      DecimalType(7,2)),
    StructField("refunded_cash",         DecimalType(7,2)),
    StructField("reversed_charge",       DecimalType(7,2)),
    StructField("store_credit",          DecimalType(7,2)),
    StructField("net_loss",              DecimalType(7,2))
  ))
  
  val storeSalesSchema = StructType(Array(
    StructField("sold_date_sk",            IntegerType),
    StructField("sold_time_sk",            IntegerType),
    StructField("item_sk",                 IntegerType),
    StructField("customer_sk",             IntegerType),
    StructField("cdemo_sk",                IntegerType),
    StructField("hdemo_sk",                IntegerType),
    StructField("addr_sk",                 IntegerType),
    StructField("store_sk",                IntegerType),
    StructField("promo_sk",                IntegerType),
    StructField("ticket_number",           LongType),
    StructField("quantity",                IntegerType),
    StructField("wholesale_cost",          DecimalType(7,2)),
    StructField("list_price",              DecimalType(7,2)),
    StructField("sales_price",             DecimalType(7,2)),
    StructField("ext_discount_amt",        DecimalType(7,2)),
    StructField("ext_sales_price",         DecimalType(7,2)),
    StructField("ext_wholesale_cost",      DecimalType(7,2)),
    StructField("ext_list_price",          DecimalType(7,2)),
    StructField("ext_tax",                 DecimalType(7,2)),
    StructField("coupon_amt",              DecimalType(7,2)),
    StructField("net_paid",                DecimalType(7,2)),
    StructField("net_paid_inc_tax",        DecimalType(7,2)),
    StructField("net_profit",              DecimalType(7,2))
  ))

  val inventorySchema = StructType(Array(
    StructField("date_sk",                 IntegerType),
    StructField("item_sk",                 IntegerType),
    StructField("warehouse_sk",            IntegerType),
    StructField("quantity_on_hand",        IntegerType)
  ))

  val storeReturnsSchema = StructType(Array(
    StructField("returned_date_sk",        IntegerType),
    StructField("return_time_sk",          IntegerType),
    StructField("item_sk",                 IntegerType),
    StructField("customer_sk",             IntegerType),
    StructField("cdemo_sk",                IntegerType),
    StructField("hdemo_sk",                IntegerType),
    StructField("addr_sk",                 IntegerType),
    StructField("store_sk",                IntegerType),
    StructField("reason_sk",               IntegerType),
    StructField("ticket_number",           LongType),
    StructField("return_quantity",         IntegerType),
    StructField("return_amt",              DecimalType(7,2)),
    StructField("return_tax",              DecimalType(7,2)),
    StructField("return_amt_inc_tax",      DecimalType(7,2)),
    StructField("fee",                     DecimalType(7,2)),
    StructField("return_ship_cost",        DecimalType(7,2)),
    StructField("refunded_cash",           DecimalType(7,2)),
    StructField("reversed_charge",         DecimalType(7,2)),
    StructField("store_credit",            DecimalType(7,2)),
    StructField("net_loss",                DecimalType(7,2))
  ))

  val webSalesSchema = StructType(Array(
    StructField("sold_date_sk",            IntegerType),
    StructField("sold_time_sk",            IntegerType),
    StructField("ship_date_sk",            IntegerType),
    StructField("item_sk",                 IntegerType),
    StructField("bill_customer_sk",        IntegerType),
    StructField("bill_cdemo_sk",           IntegerType),
    StructField("bill_hdemo_sk",           IntegerType),
    StructField("bill_addr_sk",            IntegerType),
    StructField("ship_customer_sk",        IntegerType),
    StructField("ship_cdemo_sk",           IntegerType),
    StructField("ship_hdemo_sk",           IntegerType),
    StructField("ship_addr_sk",            IntegerType),
    StructField("web_page_sk",             IntegerType),
    StructField("web_site_sk",             IntegerType),
    StructField("ship_mode_sk",            IntegerType),
    StructField("warehouse_sk",            IntegerType),
    StructField("promo_sk",                IntegerType),
    StructField("order_number",            LongType),
    StructField("quantity",                IntegerType),
    StructField("wholesale_cost",          DecimalType(7,2)),
    StructField("list_price",              DecimalType(7,2)),
    StructField("sales_price",             DecimalType(7,2)),
    StructField("ext_discount_amt",        DecimalType(7,2)),
    StructField("ext_sales_price",         DecimalType(7,2)),
    StructField("ext_wholesale_cost",      DecimalType(7,2)),
    StructField("ext_list_price",          DecimalType(7,2)),
    StructField("ext_tax",                 DecimalType(7,2)),
    StructField("coupon_amt",              DecimalType(7,2)),
    StructField("ext_ship_cost",           DecimalType(7,2)),
    StructField("net_paid",                DecimalType(7,2)),
    StructField("net_paid_inc_tax",        DecimalType(7,2)),
    StructField("net_paid_inc_ship",       DecimalType(7,2)),
    StructField("net_paid_inc_ship_tax",   DecimalType(7,2)),
    StructField("net_profit",              DecimalType(7,2))
  ))

  val webReturnsSchema = StructType(Array(
    StructField("returned_date_sk",      IntegerType),
    StructField("returned_time_sk",      IntegerType),
    StructField("item_sk",               IntegerType),
    StructField("refunded_customer_sk",  IntegerType),
    StructField("refunded_cdemo_sk",     IntegerType),
    StructField("refunded_hdemo_sk",     IntegerType),
    StructField("refunded_addr_sk",      IntegerType),
    StructField("returning_customer_sk", IntegerType),
    StructField("returning_cdemo_sk",    IntegerType),
    StructField("returning_hdemo_sk",    IntegerType),
    StructField("returning_addr_sk",     IntegerType),
    StructField("web_page_sk",           IntegerType),
    StructField("reason_sk",             IntegerType),
    StructField("order_number",          LongType),
    StructField("return_quantity",       IntegerType),
    StructField("return_amt",            DecimalType(7,2)),
    StructField("return_tax",            DecimalType(7,2)),
    StructField("return_amt_inc_tax",    DecimalType(7,2)),
    StructField("fee",                   DecimalType(7,2)),
    StructField("return_ship_cost",      DecimalType(7,2)),
    StructField("refunded_cash",         DecimalType(7,2)),
    StructField("reversed_charge",       DecimalType(7,2)),
    StructField("account_credit",        DecimalType(7,2)),
    StructField("net_loss",              DecimalType(7,2))
  ))
}
