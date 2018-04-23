package com.sdu.stream.storm.schema.action;

import com.sdu.stream.storm.parse.DataRow;
import com.sdu.stream.storm.utils.RTDCalculateException;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public class Aviator implements Action {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void setState(KeyValueState keyValueState) {

    }

    @Override
    public void execute(DataRow dataRow) throws RTDCalculateException {

    }

    @Override
    public ActionType actionType() {
        return ActionType.EL;
    }

}
