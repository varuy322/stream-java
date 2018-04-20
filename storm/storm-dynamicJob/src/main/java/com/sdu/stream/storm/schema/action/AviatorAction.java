package com.sdu.stream.storm.schema.action;

public class AviatorAction implements Action {

    @Override
    public ActionType actionType() {
        return ActionType.EL;
    }

}
