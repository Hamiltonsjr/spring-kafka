package br.com.kafka.ecommerce.kafka.utils;

import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class OrderDeserializer<T> implements Deserializer<T> {

    public static final String TYPE_CONFIG = "br.com.ecommerce";

    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        final String typeName = String.valueOf(configs.get(TYPE_CONFIG));
        try {
            this.type = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(final String topic, byte[] bytes) {
        return new GsonBuilder().create().fromJson(new String(bytes), type);
    }
}
