package de.unikl.cs.dbis.waves.testjobs.query

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.util.Logger

/**
  * Run adapted versions of TPC-DS Queries 1-5,7,11-13,15
  * 
  * These are the first 10 queries applicable to the modified schema
  */
object TPCDS extends QueryRunner {
  def main(args: Array[String]) : Unit = {
      val jobConfig = JobConfig.fromArgs(args)
      val spark = jobConfig.makeSparkSession("TPCDS")
      runWithVoid(spark, jobConfig, Seq(
        df => df.createOrReplaceTempView("tpcds"),
        // Q7
        df => df.sparkSession.sql("""
          select  item_sk.item_id item, 
                  avg(quantity) agg1,
                  avg(list_price) agg2,
                  avg(coupon_amt) agg3,
                  avg(sales_price) agg4 
          from tpcds
          where typetag = 'store_sales' and
                cdemo_sk.gender = 'F' and 
                cdemo_sk.marital_status = 'W' and
                cdemo_sk.education_status = 'Primary' and
                (promo_sk.channel_email = 'N' or promo_sk.channel_event = 'N') and
                sold_date_sk.year = 1998 
          group by item
          order by item
          limit 100;
          """).show(),
        // Q12
        df => df.sparkSession.sql("""
          select  item_sk.item_id
                ,item_sk.item_desc 
                ,item_sk.category 
                ,item_sk.class 
                ,item_sk.current_price
                ,sum(ext_sales_price) as itemrevenue 
                ,sum(ext_sales_price)*100/sum(sum(ext_sales_price)) over
                    (partition by item_sk.class) as revenueratio
          from tpcds
          where typetag = 'web_sales'
            and item_sk.category in ('Jewelry', 'Music', 'Electronics')
            and sold_date_sk.date between cast('1999-03-12' as date) 
                and date_add(cast('1999-03-12' as date), 30)
          group by 
            item_sk.item_id
                  ,item_sk.item_desc 
                  ,item_sk.category
                  ,item_sk.class
                  ,item_sk.current_price
          order by 
            category
                  ,class
                  ,item_id
                  ,item_desc
                  ,revenueratio
          limit 100;
          """).show(),
          // Q13
          df => df.sparkSession.sql("""
          select avg(quantity)
                ,avg(ext_sales_price)
                ,avg(ext_wholesale_cost)
                ,sum(ext_wholesale_cost)
          from tpcds
          where typetag = "store_sales"
          and sold_date_sk.year = 2001
          and((cdemo_sk.marital_status = 'W'
            and cdemo_sk.education_status = 'Primary'
            and sales_price between 100.00 and 150.00
            and hdemo_sk.dep_count = 3   
              )or
              (cdemo_sk.marital_status = 'S'
            and cdemo_sk.education_status = 'Secondary'
            and sales_price between 50.00 and 100.00   
            and hdemo_sk.dep_count = 1
              ) or 
              (cdemo_sk.marital_status = 'D'
            and cdemo_sk.education_status = 'College'
            and sales_price between 150.00 and 200.00 
            and hdemo_sk.dep_count = 1  
              ))
          and((addr_sk.country = 'United States'
            and addr_sk.state in ('FL', 'NM', 'SC')
            and net_profit between 100 and 200  
              ) or
              (addr_sk.country = 'United States'
            and addr_sk.state in ('MS', 'TX', 'ID')
            and net_profit between 150 and 300  
              ) or
              (addr_sk.country = 'United States'
            and addr_sk.state in ('VA', 'CA', 'MI')
            and net_profit between 50 and 250  
              ))
          ;
          """).show(),
          // Q3
          df => df.sparkSession.sql("""
          select  sold_date_sk.year sold_year
                ,item_sk.brand_id brand_id 
                ,item_sk.brand brand
                ,sum(ext_sales_price) sum_agg
          from  tpcds
          where typetag = 'store_sales'
            and item_sk.manufact_id = 524
            and sold_date_sk.moy=12
          group by sold_year
                ,item_sk.brand
                ,item_sk.brand_id
          order by sold_year
                  ,sum_agg desc
                  ,brand_id
          limit 100;
          """).show(),
          // Q5
          df => df.sparkSession.sql("""
          with ssr as
          (select store_id,
                  sum(sales_price) as sales,
                  sum(profit) as profit,
                  sum(return_amt) as returns,
                  sum(net_loss) as profit_loss
          from
            ( select  store_sk.store_id as store_id,
                      sold_date_sk.date  as date_sk,
                      ext_sales_price as sales_price,
                      net_profit as profit,
                      cast(0 as decimal(7,2)) as return_amt,
                      cast(0 as decimal(7,2)) as net_loss
              from tpcds
              where typetag = 'store_sales'
              union all
              select store_sk.store_id as store_id,
                    returned_date_sk.date as date_sk,
                    cast(0 as decimal(7,2)) as sales_price,
                    cast(0 as decimal(7,2)) as profit,
                    return_amt as return_amt,
                    net_loss as net_loss
              from tpcds
              where typetag = 'store_returns'
            ) salesreturns
          where date_sk between cast('2000-08-19' as date) 
                        and date_add(cast('2000-08-19' as date), 14)
          group by store_id)
          ,
          csr as
          (select catalog_page_id,
                  sum(sales_price) as sales,
                  sum(profit) as profit,
                  sum(return_amt) as returns,
                  sum(net_loss) as profit_loss
          from
            ( select  catalog_page_sk.catalog_page_id as catalog_page_id,
                      sold_date_sk.date  as date_sk,
                      ext_sales_price as sales_price,
                      net_profit as profit,
                      cast(0 as decimal(7,2)) as return_amt,
                      cast(0 as decimal(7,2)) as net_loss
              from tpcds
              where typetag = 'catalog_sales'
              union all
              select catalog_page_sk.catalog_page_id as catalog_page_id,
                    returned_date_sk.date as date_sk,
                    cast(0 as decimal(7,2)) as sales_price,
                    cast(0 as decimal(7,2)) as profit,
                    return_amount as return_amt,
                    net_loss as net_loss
              from tpcds
              where typetag = 'catalog_returns'
            ) salesreturns
          where date_sk between cast('2000-08-19' as date)
                        and date_add(cast('2000-08-19' as date), 14)
          group by catalog_page_id)
          ,
          wsr as
          (select site_id,
                  sum(sales_price) as sales,
                  sum(profit) as profit,
                  sum(return_amt) as returns,
                  sum(net_loss) as profit_loss
          from
            ( select  web_site_sk.site_id as site_id,
                      sold_date_sk.date as date_sk,
                      ext_sales_price as sales_price,
                      net_profit as profit,
                      cast(0 as decimal(7,2)) as return_amt,
                      cast(0 as decimal(7,2)) as net_loss
              from tpcds
              where typetag = 'web_sales'
              union all
              select ws.web_site_sk.site_id as site_id,
                    wr.returned_date_sk.date as date_sk,
                    cast(0 as decimal(7,2)) as sales_price,
                    cast(0 as decimal(7,2)) as profit,
                    wr.return_amt as return_amt,
                    wr.net_loss as net_loss
              from tpcds wr left outer join tpcds ws on
                  ( wr.item_sk.item_sk = ws.item_sk.item_sk
                    and wr.order_number = ws.order_number)
              where wr.typetag = 'web_returns'
                and ws.typetag = 'web_sales'
            ) salesreturns
          where date_sk between cast('2000-08-19' as date)
                        and date_add(cast('2000-08-19' as date), 14)
          group by site_id)
            select  channel
                  , id
                  , sum(sales) as sales
                  , sum(returns) as returns
                  , sum(profit) as profit
          from 
          (select 'store channel' as channel
                  , 'store' || store_id as id
                  , sales
                  , returns
                  , (profit - profit_loss) as profit
          from   ssr
          union all
          select 'catalog channel' as channel
                  , 'catalog_page' || catalog_page_id as id
                  , sales
                  , returns
                  , (profit - profit_loss) as profit
          from  csr
          union all
          select 'web channel' as channel
                  , 'web_site' || site_id as id
                  , sales
                  , returns
                  , (profit - profit_loss) as profit
          from   wsr
          ) x
          group by rollup (channel, id)
          order by channel
                  ,id
          limit 100;
          """).show(),
          // Q1
          df => df.sparkSession.sql("""
          with customer_total_return as (
              select customer_sk,
                    store_sk,
                    sum(REFUNDED_CASH) as total_return
              from tpcds
              where returned_date_sk.year =2001
                and typetag = 'store_returns'
              group by customer_sk, store_sk
          ),
          avg_return as (
              select avg(total_return)*1.2 as avg_return,
                    ctr.store_sk.store_sk as store_sk
              from customer_total_return ctr
              group by ctr.store_sk.store_sk
          )
          select  ctr.customer_sk.customer_id
          from customer_total_return ctr,
              avg_return
          where ctr.total_return > avg_return.avg_return
            and ctr.store_sk.state = 'AL'
            and avg_return.store_sk = ctr.store_sk.store_sk
          order by ctr.customer_sk.customer_id
          limit 100;
          """).show(),
          // Q15
          df => df.sparkSession.sql("""
          select  bill_customer_sk.current_addr_sk.zip
                ,sum(sales_price)
          from tpcds
          where typetag = 'catalog_sales'
            and ( substr(bill_customer_sk.current_addr_sk.zip,1,5) in ('85669', '86197','88274','83405','86475',
                                            '85392', '85460', '80348', '81792')
                  or bill_customer_sk.current_addr_sk.state in ('CA','WA','GA')
                  or sales_price > 500)
            and sold_date_sk.qoy = 2 and sold_date_sk.year = 1999
          group by bill_customer_sk.current_addr_sk.zip
          order by bill_customer_sk.current_addr_sk.zip
          limit 100;
          """).show(),
          // Q4
          df => df.sparkSession.sql("""
          with year_total as (
          select customer_sk.customer_id customer_id
                ,customer_sk.first_name customer_first_name
                ,customer_sk.last_name customer_last_name
                ,customer_sk.preferred_cust_flag customer_preferred_cust_flag
                ,customer_sk.birth_country customer_birth_country
                ,customer_sk.email_address customer_email_address
                ,sold_date_sk.year dyear
                ,sum(((ext_list_price-ext_wholesale_cost-ext_discount_amt)+ext_sales_price)/2) year_total
                ,'s' sale_type
          from tpcds
          where typetag = 'store_sales'
          group by customer_id
                  ,customer_first_name
                  ,customer_last_name
                  ,customer_preferred_cust_flag
                  ,customer_birth_country
                  ,customer_email_address
                  ,dyear
          union all
          select bill_customer_sk.customer_id customer_id
                ,bill_customer_sk.first_name customer_first_name
                ,bill_customer_sk.last_name customer_last_name
                ,bill_customer_sk.preferred_cust_flag customer_preferred_cust_flag
                ,bill_customer_sk.birth_country customer_birth_country
                ,bill_customer_sk.email_address customer_email_address
                ,sold_date_sk.year dyear
                ,sum((((ext_list_price-ext_wholesale_cost-ext_discount_amt)+ext_sales_price)/2) ) year_total
                ,'c' sale_type
          from tpcds
          where typetag = 'catalog_sales'
          group by customer_id
                  ,customer_first_name
                  ,customer_last_name
                  ,customer_preferred_cust_flag
                  ,customer_birth_country
                  ,customer_email_address
                  ,dyear
          union all
          select bill_customer_sk.customer_id customer_id
                ,bill_customer_sk.first_name customer_first_name
                ,bill_customer_sk.last_name customer_last_name
                ,bill_customer_sk.preferred_cust_flag customer_preferred_cust_flag
                ,bill_customer_sk.birth_country customer_birth_country
                ,bill_customer_sk.email_address customer_email_address
                ,sold_date_sk.year dyear
                ,sum((((ext_list_price-ext_wholesale_cost-ext_discount_amt)+ext_sales_price)/2) ) year_total
                ,'w' sale_type
          from tpcds
          where typetag = 'web_sales'
          group by customer_id
                  ,customer_first_name
                  ,customer_last_name
                  ,customer_preferred_cust_flag
                  ,customer_birth_country
                  ,customer_email_address
                  ,dyear
                  )
            select  
                            t_s_secyear.customer_id
                          ,t_s_secyear.customer_first_name
                          ,t_s_secyear.customer_last_name
                          ,t_s_secyear.customer_email_address
          from year_total t_s_firstyear
              ,year_total t_s_secyear
              ,year_total t_c_firstyear
              ,year_total t_c_secyear
              ,year_total t_w_firstyear
              ,year_total t_w_secyear
          where t_s_secyear.customer_id = t_s_firstyear.customer_id
            and t_s_firstyear.customer_id = t_c_secyear.customer_id
            and t_s_firstyear.customer_id = t_c_firstyear.customer_id
            and t_s_firstyear.customer_id = t_w_firstyear.customer_id
            and t_s_firstyear.customer_id = t_w_secyear.customer_id
            and t_s_firstyear.sale_type = 's'
            and t_c_firstyear.sale_type = 'c'
            and t_w_firstyear.sale_type = 'w'
            and t_s_secyear.sale_type = 's'
            and t_c_secyear.sale_type = 'c'
            and t_w_secyear.sale_type = 'w'
            and t_s_firstyear.dyear =  2001
            and t_s_secyear.dyear = 2001+1
            and t_c_firstyear.dyear =  2001
            and t_c_secyear.dyear =  2001+1
            and t_w_firstyear.dyear = 2001
            and t_w_secyear.dyear = 2001+1
            and t_s_firstyear.year_total > 0
            and t_c_firstyear.year_total > 0
            and t_w_firstyear.year_total > 0
            and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
                    > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
            and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
                    > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
          order by t_s_secyear.customer_id
                  ,t_s_secyear.customer_first_name
                  ,t_s_secyear.customer_last_name
                  ,t_s_secyear.customer_email_address
          limit 100;
          """).show(),
          // Q2
          df => df.sparkSession.sql("""
          with wscs as(
              select sold_date_sk, ext_sales_price sales_price
              from tpcds
              where typetag = 'web_sales'
                  or typetag = 'catalog_sales'
          ),
          wswscs as (
                select sold_date_sk.week_seq week_seq,
                      sold_date_sk.year year,
                      sum(case when (sold_date_sk.day_name='Sunday') then sales_price else null end) sun_sales,
                      sum(case when (sold_date_sk.day_name='Monday') then sales_price else null end) mon_sales,
                      sum(case when (sold_date_sk.day_name='Tuesday') then sales_price else  null end) tue_sales,
                      sum(case when (sold_date_sk.day_name='Wednesday') then sales_price else null end) wed_sales,
                      sum(case when (sold_date_sk.day_name='Thursday') then sales_price else null end) thu_sales,
                      sum(case when (sold_date_sk.day_name='Friday') then sales_price else null end) fri_sales,
                      sum(case when (sold_date_sk.day_name='Saturday') then sales_price else null end) sat_sales
                from wscs
                group by sold_date_sk.week_seq, sold_date_sk.year
          )
          select week_seq1
                ,round(sun_sales1/sun_sales2,2)
                ,round(mon_sales1/mon_sales2,2)
                ,round(tue_sales1/tue_sales2,2)
                ,round(wed_sales1/wed_sales2,2)
                ,round(thu_sales1/thu_sales2,2)
                ,round(fri_sales1/fri_sales2,2)
                ,round(sat_sales1/sat_sales2,2)
          from
                (select wswscs.week_seq week_seq1
                      ,sun_sales sun_sales1
                      ,mon_sales mon_sales1
                      ,tue_sales tue_sales1
                      ,wed_sales wed_sales1
                      ,thu_sales thu_sales1
                      ,fri_sales fri_sales1
                      ,sat_sales sat_sales1
                from wswscs
                where year = 1999) y,
                (select wswscs.week_seq week_seq2
                      ,sun_sales sun_sales2
                      ,mon_sales mon_sales2
                      ,tue_sales tue_sales2
                      ,wed_sales wed_sales2
                      ,thu_sales thu_sales2
                      ,fri_sales fri_sales2
                      ,sat_sales sat_sales2
                from wswscs
                where year = 1999+1) z
          where week_seq1=week_seq2-53
          order by week_seq1;
          """).show(),
          // Q11
          df => df.sparkSession.sql("""
          with year_total as (
          select customer_sk.customer_id customer_id
                ,customer_sk.first_name customer_first_name
                ,customer_sk.last_name customer_last_name
                ,customer_sk.preferred_cust_flag customer_preferred_cust_flag
                ,customer_sk.birth_country customer_birth_country
                ,customer_sk.email_address customer_email_address
                ,sold_date_sk.year dyear
                ,sum(ext_list_price-ext_discount_amt) year_total
                ,'s' sale_type
          from tpcds
          where typetag = 'store_sales'
          group by customer_sk.customer_id
                  ,customer_sk.first_name
                  ,customer_sk.last_name
                  ,customer_sk.preferred_cust_flag 
                  ,customer_sk.birth_country
                  ,customer_sk.email_address
                  ,sold_date_sk.year 
          union all
          select bill_customer_sk.customer_id customer_id
                ,bill_customer_sk.first_name customer_first_name
                ,bill_customer_sk.last_name customer_last_name
                ,bill_customer_sk.preferred_cust_flag customer_preferred_cust_flag
                ,bill_customer_sk.birth_country customer_birth_country
                ,bill_customer_sk.email_address customer_email_address
                ,sold_date_sk.year dyear
                ,sum(ext_list_price-ext_discount_amt) year_total
                ,'w' sale_type
          from tpcds
          where typetag = 'web_sales' 
          group by bill_customer_sk.customer_id
                  ,bill_customer_sk.first_name
                  ,bill_customer_sk.last_name
                  ,bill_customer_sk.preferred_cust_flag 
                  ,bill_customer_sk.birth_country
                  ,bill_customer_sk.email_address
                  ,sold_date_sk.year
                  )
            select  
                            t_s_secyear.customer_id
                          ,t_s_secyear.customer_first_name
                          ,t_s_secyear.customer_last_name
                          ,t_s_secyear.customer_email_address
          from year_total t_s_firstyear
              ,year_total t_s_secyear
              ,year_total t_w_firstyear
              ,year_total t_w_secyear
          where t_s_secyear.customer_id = t_s_firstyear.customer_id
                  and t_s_firstyear.customer_id = t_w_secyear.customer_id
                  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
                  and t_s_firstyear.sale_type = 's'
                  and t_w_firstyear.sale_type = 'w'
                  and t_s_secyear.sale_type = 's'
                  and t_w_secyear.sale_type = 'w'
                  and t_s_firstyear.dyear = 2001
                  and t_s_secyear.dyear = 2001+1
                  and t_w_firstyear.dyear = 2001
                  and t_w_secyear.dyear = 2001+1
                  and t_s_firstyear.year_total > 0
                  and t_w_firstyear.year_total > 0
                  and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end
                      > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end
          order by t_s_secyear.customer_id
                  ,t_s_secyear.customer_first_name
                  ,t_s_secyear.customer_last_name
                  ,t_s_secyear.customer_email_address
          limit 100;
          """).show(),
      ))
  }
}
