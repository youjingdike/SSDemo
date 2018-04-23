package com.xq.sql.demo;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.xq.util.DateUtil;
import com.xq.util.RegUtil;

/**
 * JDBC数据源
 * 
 * @author Administrator
 *
 */
public class KpiMain {
	private static final String result_table = "kpi_result_test";
	
	public static void main(String[] args) {
		String user = "";
		Date currDate = new Date();
		long st = currDate.getTime();
		DateFormat df_datetime = DateUtil.dateTimeFormat;
		String currentDateStr = df_datetime.format(currDate);
		System.out.println("...任务开始,时间："+currentDateStr);
		
		final String dateStr;
		if (args.length>=2 && StringUtils.isNotBlank(args[1])) {
			dateStr = args[1];
		} else {
			dateStr = DateUtil.getDayBeforeOrAfter(currDate,-1);
		}
		
		Date parse = null;
		try {
			parse = df_datetime.parse(dateStr+" 19:59:59");
		} catch (ParseException e) {
			e.printStackTrace();
			System.out.println(dateStr+":is not rightful date");
			return;
		}
		System.out.println("处理的日期："+dateStr);		
		long tm = parse==null?0L:parse.getTime();
		
		Builder builder = SparkSession.builder()
		.appName("KPI_App")
		.config("spark.some.config.option", "some-value");
		
		if (args.length >= 3 && RegUtil.validataAll("\\d*", args[2])) {
			builder.config("spark.sql.shuffle.partitions", args[2]);
			System.out.println("分区数："+args[2]);	
		} else {
			builder.config("spark.sql.shuffle.partitions", "10");
		}
		
		if (args.length > 0 && "spark".equalsIgnoreCase(args[0])) {
			builder.master("spark://node5:7077");
		} else {
			builder.master("local[1]");
		}
		SparkSession ss = builder
			.getOrCreate();
		 
		// 分别将mysql中两张表的数据加载为Dataset<Row>
		DataFrameReader reader = ss.read().format("jdbc");
		reader.option("url", "jdbc:postgresql://10.99.0.9:5432/logdb");
		reader.option("driver", "org.postgresql.Driver");
		reader.option("user", "postgres");
		reader.option("password", "postgres");
		
		reader.option("dbtable", "mq_exception_info");
		Dataset<Row> mqDF = reader.load();
		
		reader.option("dbtable", "organ");
		Dataset<Row> organDF = reader.load();
//		organDF.show();
		
		reader.option("dbtable", "netty_exception_info");
		Dataset<Row> nettyDF = reader.load();
//		nettyDF.show();
		
		reader.option("dbtable", "kpi_param");
		Dataset<Row> kpiParamDF = reader.load();
//		kpiParamDF.show();
		
		reader.option("dbtable", "sys_parameter");
		Dataset<Row> sysDF = reader.load();
//		sysDF.show();
		
		reader.option("url", "jdbc:phoenix:10.99.0.9,10.99.0.8,10.99.0.6");
		reader.option("driver", "org.apache.phoenix.jdbc.PhoenixDriver");
		reader.option("user", "");
		reader.option("password", "");
		
		reader.option("dbtable", "log_analysis_module.order_log_info");
		Dataset<Row> orderLogDF = reader.load();
//		orderDF.show();
		
		reader.option("dbtable", "log_analysis_module.order_error_result");
		Dataset<Row> errorDF = reader.load();
//		errorDF.show();
		
		reader.option("dbtable", "log_analysis_module.cc_order_list");
		Dataset<Row> orderDF = reader.load().filter("SINGLE_TIME >= '"+dateStr+" 00:00:00' and SINGLE_TIME <= '"+dateStr+" 23:59:59'");
//		orderDF.show();
		
		Dataset<Row> timeoutDS = reader.load().filter(new FilterFunction<Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public boolean call(Row row) throws Exception {
				String single_time = row.<String>getAs("SINGLE_TIME");
				String take_time = row.<String>getAs("TAKE_TIME");
				String chanel = row.<String>getAs("CHANEL");
				boolean flag = false;
				if (StringUtils.isNotBlank(single_time) && StringUtils.isNotBlank(take_time) && StringUtils.isNotBlank(chanel) && dateStr.equals(single_time.split(" ")[0])) {
					long stime = df_datetime.parse(single_time).getTime();
					long ttime = df_datetime.parse(take_time).getTime();
					if ("BD06".equalsIgnoreCase(chanel)) {
						//百度超过10分钟
						if ((ttime - stime) > 10*60*1000) {
							flag = true;
						}
					} else if ("EL09".equalsIgnoreCase(chanel) || "MT08".equalsIgnoreCase(chanel)|| "MT11".equalsIgnoreCase(chanel)) {
						//美团、饿了么超过 5分钟
						if ((ttime - stime) > 5*60*1000) {
							flag = true;
						}
					}
				}
				
				return flag;
			}
		});
//		timeoutDS.show();
		
		mqDF.createOrReplaceTempView("mq_exce");
		organDF.createOrReplaceTempView("organ");
		nettyDF.createOrReplaceTempView("netty_exce");
		kpiParamDF.createOrReplaceTempView("kpi_param");
		
		orderLogDF.createOrReplaceTempView("order_log_info");
		errorDF.createOrReplaceTempView("order_error_info");
		orderDF.createOrReplaceTempView("order_list");
		timeoutDS.createOrReplaceTempView("order_timeout_list");
		sysDF.createOrReplaceTempView("sys_param");
		
		String sql = "";
		//分析MQ的数据
		sql = "select nvl(t.error_num,0) as error_num,o.org_uuid,nvl(t.kpi_percent,0) as kpi_percent,o.tenancy_id,o.store_id,nvl(p.kpi_score,100) as kpi_score,'3' as kpi_type,CAST('"+dateStr+"' as date) as kpi_date,CAST('"+currentDateStr+"' as timestamp) as create_time,'"+user+"' as user from"
				+ " organ o "
				+ " join sys_param s on o.tenancy_id = s.tenancy_id and o.store_id = s.store_id and s.para_value = '0' "
				+ " left join ("
				+ "	SELECT round(sum(nvl(cancel_time_long,"+tm+")-exception_time_long)/(3600*1000*10),4) as kpi_percent,'3' as kpi_type,t.org_uuid,count(1) as error_num FROM mq_exce t where t.exception_time >= '"+dateStr+" 10:00:00' and t.exception_time <= '"+dateStr+" 19:59:59' group by org_uuid"
						+ ") t "
						+ " on t.org_uuid = o.org_uuid "
						+ " left join kpi_param p on t.kpi_type = p.kpi_type and t.kpi_percent >= p.param_mix and t.kpi_percent < p.param_max ";
		Dataset<Row> mqResultDF = ss.sql(sql);
		
		mqResultDF.write().mode(SaveMode.Append)
		  .format("jdbc")
		  .option("url", "jdbc:postgresql://10.99.0.9:5432/logdb")
		  .option("driver", "org.postgresql.Driver")
		  .option("dbtable", result_table)
		  .option("user", "postgres")
		  .option("password", "postgres")
		  .save();

		//分析netty的数据
		sql = "select nvl(tt.error_num,0) as error_num,nvl(tt.kpi_percent,0) as kpi_percent,o.org_uuid,o.tenancy_id,o.store_id,nvl(p.kpi_score,100) as kpi_score,'3' as kpi_type,CAST('"+dateStr+"' as date) as kpi_date,CAST('"+currentDateStr+"' as timestamp) as create_time,'"+user+"' as user from"
				+ " organ o "
				+ " join sys_param s on o.tenancy_id = s.tenancy_id and o.store_id = s.store_id and s.para_value = '11' "
				+ " left join ("
				+ "SELECT round(sum(nvl(cancel_time_long,"+tm+")-exception_time_long)/(3600*1000*10),4) as kpi_percent,'3' as kpi_type,t.tenancy_id,t.store_id,count(1) as error_num FROM netty_exce t where t.exception_time >= '"+dateStr+" 10:00:00' and t.exception_time <= '"+dateStr+" 19:59:59' group by tenancy_id,store_id "
						+ ") tt on o.tenancy_id = tt.tenancy_id and o.store_id = tt.store_id "
						+ " left join kpi_param p on tt.kpi_type = p.kpi_type and tt.kpi_percent >= p.param_mix and tt.kpi_percent < p.param_max ";
		Dataset<Row> nettyResultDF = ss.sql(sql);
		nettyResultDF.write().mode(SaveMode.Append)
		  .format("jdbc")
		  .option("url", "jdbc:postgresql://10.99.0.9:5432/logdb")
		  .option("driver", "org.postgresql.Driver")
		  .option("dbtable", result_table)
		  .option("user", "postgres")
		  .option("password", "postgres")
		  .save();
		
		//分析菜品异常
		sql = "select tt.*,p.kpi_score,CAST('"+dateStr+"' as date) as kpi_date,CAST('"+currentDateStr+"' as timestamp) as create_time,'"+user+"' as user from "
				+ " (select round(nvl(t2.cot,0)/t1.cot,4) as kpi_percent,'1' as kpi_type,t1.tenancy_id,t1.store_id,nvl(t2.cot,0) as error_num from "
				+ "(select count(1) as cot,t.tenancy_id,t.store_id from order_log_info t where t.log_type = 'ppc' and t.tenancy_id is not null and t.order_datetime >= '"+dateStr+" 00:00:00' and t.order_datetime <= '"+dateStr+" 23:59:59' group by t.tenancy_id,t.store_id "
				+ ") t1 left join "
				+ " (select count(1) as cot,t.tenancy_id,t.store_id from order_error_info t where t.error_type = '1' and t.tenancy_id is not null and t.order_datetime = '"+dateStr+"'  group by t.tenancy_id,t.store_id "
				+ ") t2 on t1.tenancy_id = t2.tenancy_id and t1.store_id = t2.store_id "
				+ " ) tt"
				+ " left join kpi_param p on tt.kpi_type = p.kpi_type and tt.kpi_percent >= p.param_mix and tt.kpi_percent < p.param_max ";
		Dataset<Row> countDF = ss.sql(sql);
		countDF.write().mode(SaveMode.Append)
		  .format("jdbc")
		  .option("url", "jdbc:postgresql://10.99.0.9:5432/logdb")
		  .option("driver", "org.postgresql.Driver")
		  .option("dbtable", result_table)
		  .option("user", "postgres")
		  .option("password", "postgres")
		  .save();
		
		//分析订单超时异常
		sql = " select tt.*,p.kpi_score,CAST('"+dateStr+"' as date) as kpi_date,CAST('"+currentDateStr+"' as timestamp) as create_time,'"+user+"' as user from "
				+ "(select round(nvl(t2.cot,0)/t1.cot,4) as kpi_percent,'2' as kpi_type,t1.tenancy_id,t1.store_id,nvl(t2.cot,0) as error_num from ("
				+ " select t.tenancy_id,t.store_id,count(1) as cot from order_list t group by tenancy_id,store_id"
				+ ") t1 "
				+ " left join "
				+ " ("
				+ "		select t.tenancy_id,t.store_id,count(1) as cot from order_timeout_list t group by tenancy_id,store_id"
				+ ") t2 on t1.tenancy_id=t2.tenancy_id and t1.store_id=t2.store_id ) tt "
				+ " left join kpi_param p on tt.kpi_type = p.kpi_type and tt.kpi_percent >= p.param_mix and tt.kpi_percent < p.param_max ";
		Dataset<Row> timeoutDF = ss.sql(sql);
		timeoutDF.write().mode(SaveMode.Append)
		  .format("jdbc")
		  .option("url", "jdbc:postgresql://10.99.0.9:5432/logdb")
		  .option("driver", "org.postgresql.Driver")
		  .option("dbtable", result_table)
		  .option("user", "postgres")
		  .option("password", "postgres")
		  .save();
		
		//分析丢单kpi
		sql = "select tt.*,p.kpi_score,CAST('"+dateStr+"' as date) as kpi_date,CAST('"+currentDateStr+"' as timestamp) as create_time,'"+user+"' as user from "
				+ "(select round((tt1.orderAllCot-tt2.orderCot)/tt1.orderAllCot,4) as kpi_percent,'4' as kpi_type,tt1.tenancy_id,tt1.store_id,(tt1.orderAllCot-tt2.orderCot) as error_num from "
				+ "  (select sum(t1.orderCot) as orderAllCot,t1.tenancy_id,t1.store_id from "
				+ "   ("
				+ "	   select max(chanel_serial_number) as orderCot,t.tenancy_id,t.store_id,t.chanel from order_list t group by t.tenancy_id,t.store_id,t.chanel"
				+ "   ) t1 group by t1.tenancy_id,t1.store_id"
				+ "  ) tt1 join "
				+ "  ("
				+ "    select count(1) as orderCot,t.tenancy_id,t.store_id from order_list t group by t.tenancy_id,t.store_id"
				+ "  ) tt2 on tt1.tenancy_id = tt2.tenancy_id and tt1.store_id =tt2.store_id"
				+ ") tt "
				+ "left join kpi_param p on tt.kpi_type = p.kpi_type and tt.kpi_percent >= p.param_mix and tt.kpi_percent < p.param_max ";
		Dataset<Row> lostBillDF = ss.sql(sql);
		lostBillDF.write().mode(SaveMode.Append)
		  .format("jdbc")
		  .option("url", "jdbc:postgresql://10.99.0.9:5432/logdb")
		  .option("driver", "org.postgresql.Driver")
		  .option("dbtable", result_table)
		  .option("user", "postgres")
		  .option("password", "postgres")
		  .save();
		
		/**
		 * 将SparkSession 关闭，释放资源
		 */
		ss.stop();
		long ed = System.currentTimeMillis();
		System.out.println("任务完成。。。,耗时："+(ed-st)+"ms");
	}

}
