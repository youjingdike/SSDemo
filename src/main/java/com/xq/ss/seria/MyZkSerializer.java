package com.xq.ss.seria;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import com.google.common.base.Charsets;

public class MyZkSerializer implements ZkSerializer {

	@Override
	public byte[] serialize(Object obj) throws ZkMarshallingError {
		return String.valueOf(obj).getBytes(Charsets.UTF_8);
	}

	@Override
	public Object deserialize(byte[] bytes) throws ZkMarshallingError {
		return new String(bytes, Charsets.UTF_8);
	}

}
