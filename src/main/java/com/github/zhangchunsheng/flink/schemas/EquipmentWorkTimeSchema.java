package com.github.zhangchunsheng.flink.schemas;

import com.github.zhangchunsheng.flink.model.EquipmentWorkTimeEvent;
import com.github.zhangchunsheng.flink.model.MetricEvent;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Desc:
 * Created by Peter on 2021/09/01 11:00
 */
public class EquipmentWorkTimeSchema implements DeserializationSchema<EquipmentWorkTimeEvent>, SerializationSchema<EquipmentWorkTimeEvent> {

    private static final Gson gson = new Gson();

    @Override
    public EquipmentWorkTimeEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), EquipmentWorkTimeEvent.class);
    }

    @Override
    public boolean isEndOfStream(EquipmentWorkTimeEvent equipmentWorkTimeEvent) {
        return false;
    }

    @Override
    public byte[] serialize(EquipmentWorkTimeEvent equipmentWorkTimeEvent) {
        return gson.toJson(equipmentWorkTimeEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<EquipmentWorkTimeEvent> getProducedType() {
        return TypeInformation.of(EquipmentWorkTimeEvent.class);
    }
}
