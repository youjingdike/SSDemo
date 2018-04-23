package com.xq.util;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;

public class XMLUtil {
	/**
     * 获取第三方平台推送日志信息中的XML部分
     *
     * @param str
     * @param regex
     */
	private static String getXMLStrFromLogInfo(String str,String regex) {
    	if (StringUtils.isNotBlank(str)) {
    		String [] strs = str.split(regex);
    		if (strs.length==2) {
    			return strs[1].trim();
    		}
    	}
    	return null;
    }
    
    /**
     * 使用dom4j解析xml,获取document对象
     * @param xmlStr
     * @return
     * @throws DocumentException
     */
	private static Document parseXML(String xmlStr) throws DocumentException {
		return StringUtils.isNotBlank(xmlStr)?DocumentHelper.parseText(xmlStr):null;
	}

    /**
     * 通过日志信息和分割符，获取XMLDom对象
     * @param logInfo
     * @param flag
     * @return
     * @throws DocumentException
     */
	public static Document getXMLDom(String logInfo,String flag) throws DocumentException {
		Document docm = null;
		String xmlStrFromLogInfo = getXMLStrFromLogInfo(logInfo,flag);
		if (StringUtils.isNotBlank(xmlStrFromLogInfo)) {
			docm = parseXML(xmlStrFromLogInfo);
		}
		return docm;
	}
	
	public static void main(String[] args) throws DocumentException {
		String text = "<person> <name><![CDATA[tzxstar_测试门店5]]></name> \n"
				+ "<name1><![CDATA[微信支付餐费，订单号：94201712050000000001，金额：0.01]]></name1></person>";
		Document parseXML = parseXML(text);
		Element rootElement = parseXML.getRootElement();
		Node selectSingleNode = rootElement.selectSingleNode("name1");
//		System.out.println(text);
		System.out.println(selectSingleNode.getText());
	}
}
