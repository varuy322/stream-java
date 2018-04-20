package com.sdu.stream.storm.schema.action;

/**
 * 
 *
 * @author hanhan.zhang
 * */
public class AggreateAction implements Action {

    @Override
    public ActionType actionType() {
        return ActionType.AGGREGATE;
    }

}
