package com.memsource.hack2019.analytics20.domain;

@SuppressWarnings("unused")
public class TaskUnit {

    private String tUnitId;
    private Integer confirmedLevel;

    public String gettUnitId() {
        return tUnitId;
    }

    public Integer getConfirmedLevel() {
        return confirmedLevel;
    }

    @Override
    public String toString() {
        return "TaskUnit{" +
                "tUnitId='" + tUnitId + '\'' +
                ", confirmedLevel=" + confirmedLevel +
                '}';
    }
}
