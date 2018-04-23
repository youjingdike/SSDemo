package com.xq.ss.kryo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class MyRegistrator implements KryoRegistrator {  

		@Override
		public void registerClasses(Kryo t) {
			// TODO Auto-generated method stub
			t.register(ConsumerRecord.class);
		}  

}
