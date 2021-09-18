package com.github.zhangchunsheng.flink.schemas;

import com.github.zhangchunsheng.flink.model.EquipmentWorkTime;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Desc:
 * Created by Peter on 2021/09/01 11:00
 */
public class EquipmentWorkTimeSchema1 implements DeserializationSchema<Tuple2>, SerializationSchema<Tuple2<String, EquipmentWorkTime>> {

    private static final Gson gson = new Gson();

    @Override
    public Tuple2 deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), Tuple2.class);
    }

    @Override
    public boolean isEndOfStream(Tuple2 equipmentWorkTime) {
        return false;
    }

    @Override
    public byte[] serialize(Tuple2<String, EquipmentWorkTime> value) {
        return gson.toJson(value.f1).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<Tuple2> getProducedType() {
        return TypeInformation.of(Tuple2.class);
    }
}
