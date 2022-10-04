package br.com.kafka.ecommerce.kafka.utils;

import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T object) {
        return new GsonBuilder().create().toJson(object).getBytes();
    }
}
