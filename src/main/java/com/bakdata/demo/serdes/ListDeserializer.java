package com.bakdata.demo.serdes;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

// TODO: This is taken from https://github.com/apache/kafka/pull/6592 and should be removed once the PR is merged
public class ListDeserializer<Inner> implements Deserializer<List<Inner>> {

    private Deserializer<Inner> inner;
    private Class<?> listClass;
    private Integer primitiveSize;

    static private Map<Class<? extends Deserializer>, Integer> fixedLengthDeserializers = mkMap(
            mkEntry(ShortDeserializer.class, 2),
            mkEntry(IntegerDeserializer.class, 4),
            mkEntry(FloatDeserializer.class, 4),
            mkEntry(LongDeserializer.class, 8),
            mkEntry(DoubleDeserializer.class, 8),
            mkEntry(UUIDDeserializer.class, 36)
    );

    public ListDeserializer() {}

    public <L extends List> ListDeserializer(Class<L> listClass, Deserializer<Inner> innerDeserializer) {
        this.listClass = listClass;
        this.inner = innerDeserializer;
        this.primitiveSize = fixedLengthDeserializers.get(innerDeserializer.getClass());
    }

    @SuppressWarnings(value = "unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (inner == null) {
            String listTypePropertyName = isKey ? "default.list.key.serde.type" : "default.list.value.serde.type";
            String innerSerdePropertyName = isKey ? "default.list.key.serde.inner" : "default.list.value.serde.inner";
            listClass = (Class<List<Inner>>) configs.get(listTypePropertyName);
            Class<Serde> innerSerde = (Class<Serde>) configs.get(innerSerdePropertyName);
            inner = Utils.newInstance(innerSerde).deserializer();
            inner.configure(configs, isKey);
        }
    }

    @SuppressWarnings(value = "unchecked")
    private List<Inner> getListInstance(int listSize) {
        try {
            Constructor<List<Inner>> listConstructor;
            try {
                listConstructor = (Constructor<List<Inner>>) listClass.getConstructor(Integer.TYPE);
                return listConstructor.newInstance(listSize);
            } catch (NoSuchMethodException e) {
                listConstructor = (Constructor<List<Inner>>) listClass.getConstructor();
                return listConstructor.newInstance();
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not construct a list instance of \"" + listClass.getCanonicalName() + "\"", e);
        }
    }

    @Override
    public List<Inner> deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            final int size = dis.readInt();
            List<Inner> deserializedList = getListInstance(size);
            for (int i = 0; i < size; i++) {
                byte[] payload = new byte[primitiveSize == null ? dis.readInt() : primitiveSize];
                if (dis.read(payload) == -1) {
                    throw new SerializationException("End of the stream was reached prematurely");
                }
                deserializedList.add(inner.deserialize(topic, payload));
            }
            return deserializedList;
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize into a List", e);
        }
    }

    @Override
    public void close() {
        inner.close();
    }

}