package com.bakdata.demo.serdes;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

// TODO: This is taken from https://github.com/apache/kafka/pull/6592 and should be removed once the PR is merged
public class ListSerializer<Inner> implements Serializer<List<Inner>> {

    private Serializer<Inner> inner;
    private boolean isFixedLength;

    static private List<Class<? extends Serializer>> fixedLengthSerializers = Arrays.asList(
            ShortSerializer.class,
            IntegerSerializer.class,
            FloatSerializer.class,
            LongSerializer.class,
            DoubleSerializer.class,
            UUIDSerializer.class);

    public ListSerializer() {}

    public ListSerializer(Serializer<Inner> serializer) {
        this.inner = serializer;
        this.isFixedLength = fixedLengthSerializers.contains(serializer.getClass());
    }

    @SuppressWarnings(value = "unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO: comment this out again?
        if (inner == null) {
            final String innerSerdePropertyName = isKey ? "default.list.key.serde.inner" : "default.list.value.serde.inner";
            final Object innerSerde = configs.get(innerSerdePropertyName);
            try {
                if (innerSerde instanceof String) {
                    inner = Utils.newInstance((String) innerSerde, Serde.class).serializer();
                } else if (innerSerde instanceof Class) {
                    inner = ((Serde<Inner>) Utils.newInstance((Class) innerSerde)).serializer();
                } else {
                    throw new ClassNotFoundException();
                }
                inner.configure(configs, isKey);
            } catch (final ClassNotFoundException e) {
                throw new ConfigException(innerSerdePropertyName, innerSerde, "Serde class " + innerSerde + " could not be found.");
            }
        }
    }

    @Override
    public byte[] serialize(String topic, List<Inner> data) {
        if (data == null) {
            return null;
        }
        final int size = data.size();
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(size);
            for (Inner entry : data) {
                final byte[] bytes = inner.serialize(topic, entry);
                if (!isFixedLength) {
                    out.writeInt(bytes.length);
                }
                out.write(bytes);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize List", e);
        }
    }

    @Override
    public void close() {
        inner.close();
    }

}
