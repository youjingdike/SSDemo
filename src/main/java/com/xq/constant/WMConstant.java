package com.xq.constant;

public interface WMConstant {
	
	/*日志信息的区分标志*/
	String BAIDU_FLAG = "百度->新模式接收成功，消息体：";
	/*cmd:2种日志
	 * order.create
	 * 	1	待确认	
		5	已确认	
		7	正在取餐	骑士取餐路上
		8	正在配送	骑士配送中
		9	已完成	
		10	已取消
	 * */
	
	String MEITUAN_FLAG = "美团->新模式接收成功，消息体：";
	/*type:2种日志
	 * create
	 * cancel
	 * */
	
	String ELWME_FLAG = "饿了么2\\.0->新模式接收成功，消息体：";
	/*饿了么
	 * type：3种日志
		10 : 创建订单
		18：订单成功
		其他：订单取消日志
	 * */
	String XINMEIDA_FLAG = "新美大外卖->新模式接收成功，消息体：";
	
	String ALM_LOG_FLAG = "\\\"error\\\":\\\"\\[百度\\]收单失败:未正确获取餐盒费信息!\\\"";
	String ALM_LOG_REG_FLAG = "订单回复\\[BD06\\]:";
	String IN_DB_SUCC_FLAG = "插入数据库中的订单sql为:";
	String ORDER_TO_STORE_FLAG = "已成功异步发布到redis";
	String ORDER_CANCEL_FLAG = "query_result_obj:";
	String TAKE_ORDER_NOTICE_FLAG = "\\[ORDER -> update\\]总部收到请求数据:\\{\\\"data\\\":";
	String TAKE_ORDER_NOTICE_FLAG_SPLIT = "\\[ORDER -> update\\]总部收到请求数据:";
	String ORDER_ISSUE_FLAG = "总部下发saas数据\\(新推送平台方式\\)渠道";
	String ORDER_ISSUE_FLAG_1 = "Data is:";
	
	String MQ_FLAG = "\"desc\":\"OM监控MQ队列信息\"";
	String MQ_FLAG_1 = "\"desc\":\"OM监控qtoboh队列信息\"";
	
	/*日志库里主键的区分标志*/
	/**platform_push平台推送订单日志*/
	String PPC_FLAG = "ppc";
	/**platform_push平台推送订单取消日志*/
	String PPCA_FLAG = "ppca";
	String PPS_FLAG = "pps";
	String PP1_FLAG = "pp1";
	String PP5_FLAG = "pp5";
	String PP7_FLAG = "pp7";
	String PP8_FLAG = "pp8";
	String PP9_FLAG = "pp9";
	String PP10_FLAG = "pp10";
	/**alarm报警日志*/
	String ALM_FLAG = "alm";
	/**insert_db正常入库日志*/
	String IDB_FLAG = "idb";
	/**order_cancel_issue订单取消下发日志*/
	String OC_FLAG = "oc";
	/**order_to_store订单到店日志*/
	String OTS_FLAG = "ots";
	/**take_order_notice取单通知日志*/
	String TON_FLAG = "ton";
	/**order_issue新订单下发日志*/
	String OI_FLAG = "oi";

	String FOOD_NAME_FLAG = "*.*";
	
	String TOPIC_FLAG = "orderprocess";
	
	String EXCP_WM_FLAG = "Exception";
	/**接受第三方订单代码路径*/
	String EXCP_PACKAGE_FLAG_1 = "com\\.tzx\\.cc\\.baidu\\.rest\\.OrderDownstreamRest";
	String EXCP_PACKAGE_FLAG_1_S = "com.tzx.cc.baidu.rest.OrderDownstreamRest";
	/**接受第三方订单并保存saas数据库代码路径*/
	String EXCP_PACKAGE_FLAG_2 = "com\\.tzx\\.cc\\.baidu\\.bo\\.imp";
	String EXCP_PACKAGE_FLAG_2_S = "com.tzx.cc.baidu.bo.imp";
	/**订单下发到门店及saas订单状态变更*/
	String EXCP_PACKAGE_FLAG_3 = "com\\.tzx\\.cc\\.bo\\.imp\\.OrderUpdateManagementServiceImpl";
	String EXCP_PACKAGE_FLAG_3_S = "com.tzx.cc.bo.imp.OrderUpdateManagementServiceImpl";
	/**门店请求总部更改订单状态代码路径*/
	String EXCP_PACKAGE_FLAG_4 = "com\\.tzx\\.cc\\.service\\.rest\\.OrderRest";
	String EXCP_PACKAGE_FLAG_4_S = "com.tzx.cc.service.rest.OrderRest";
	/**门店在线状态请求路径*/
	String EXCP_PACKAGE_FLAG_5 = "com\\.tzx\\.hq\\.service\\.rest\\.HqRest";
	String EXCP_PACKAGE_FLAG_5_S = "com.tzx.hq.service.rest.HqRest";
	/**Saas接单服务类*/
	String EXCP_PACKAGE_FLAG_6 = "com\\.tzx\\.cc\\.takeaway\\.unifiedreceiveorder\\.service";
	String EXCP_PACKAGE_FLAG_6_S = "com.tzx.cc.takeaway.unifiedreceiveorder.service";
	/**重构后的接单服务*/
	String EXCP_PACKAGE_FLAG_7 = "com\\.tzx\\.cc\\.takeaway\\.service";
	String EXCP_PACKAGE_FLAG_7_S = "com.tzx.cc.takeaway.service";
	String EXCP_PACKAGE_FLAG_8 = "com\\.tzx\\.task\\.common\\.task\\.PushPlatFromModeTask";
	String EXCP_PACKAGE_FLAG_8_S = "com.tzx.task.common.task.PushPlatFromModeTask";
	
	/**连接超时*/
	String EXCP_TYPE_FLAG_1_1 = "存储.*渠道数据到redis时发生异常.*SocketTimeoutException";
	String EXCP_TYPE_FLAG_1_2 = "\\[第三方订单状态变更上传数据：\\].*连接超时";
	
	/**数据下发失败*/
	String EXCP_TYPE_FLAG_2_1 = "平台推送失败:渠道\\[.+\\],订单号";
	String EXCP_TYPE_FLAG_2_2 = "========= 总部下发saas 数据失败\\[.+\\],类型\\[.+\\],进入redis错误队列";
	String EXCP_TYPE_FLAG_2_3 = "数据下发失败";
	
	/**系统内部错误*/
	String EXCP_TYPE_FLAG_3 = "系统内部错误";
	
	/**连接异常（数据库）*/
	String EXCP_TYPE_FLAG_4 = "This connection has been closed";
	
	/**收取订单失败*/
	String EXCP_TYPE_FLAG_5 = "收取订单失败\\[.+\\]";
	
	/**redis连接超时*/
	String EXCP_TYPE_FLAG_6_1 = "外卖新接单模式监听发生异常.*JedisConnectionException";
	String EXCP_TYPE_FLAG_6_2 = "存储.+渠道数据到redis时发生异常.*JedisConnectionException";
	
	String CANCEL_EXCE_FLAG = "详细：Key \\(.*\\)=\\(.*\\) already exists";
}
