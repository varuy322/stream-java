package com.sdu.stream.storm.schema.action;

public class JoinAction implements Action {

    @Override
    public ActionType actionType() {
        return ActionType.JOIN;
    }

}
