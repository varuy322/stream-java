package com.sdu.stream.storm.schema.action;

public class Join implements Action {

    @Override
    public ActionType actionType() {
        return ActionType.Join;
    }

}