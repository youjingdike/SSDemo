package com.xq.constant;

public interface PayConstant {
	
	String PAY_FLAG = "接收的请求：";
	String WX_REQ_FLAG = "调用微信\\[GET_PREPAY_BARCODE\\]接口请求内容：";
	String XMD_REQ_FLAG = "新美大请求参数:";
	String WX_CALLBACK_FLAG = "=====================>> 【.+】 返回给微信信息:";
	String WX_CALLBACK_FLAG_1 = "【.+】";
	String WX_POLL_FLAG = "微信\\[QUERY_PAY_STATE\\]接口返回内容：";
	String EXC_PACKAGE_FLAG = "com\\.tzx\\.payment\\.";
	String EXC_TIMEOUT_FLAG = "connect timed out";
	String EXC_DATE_PARSE_FLAG = "Unable to parse the date:";
	String EXC_PAY_FLAG = "Exception";
	
	/**支付宝回调*/
	String ZFB_CALLBACK_FLAG = "支付宝回调返回数据为";
	/**支付宝轮询*/
	String ZFB_POLL_FLAG = "\\] ErrorScene\\^_\\^";
	String ZFB_POLL_FLAG_1 = "\\^_\\^Body:";
	
	String SECRET_ERROR_FLAG = "\\\"msg\\\":\\\"没有机构所对应的 秘钥、 商户号等信息";
	
	String TOPIC_FLAG = "taskprocess";
	String CREATE_FLAG = "GET_PREPAY_BARCODE";
	String CREATE_CUSTOMER_FLAG = "PAY_ORDER_BY_CUSTOMER";
	
	String NETTY_FLAG = "\"desc\":\"OM监控Netty 长连接信息\"";
}
