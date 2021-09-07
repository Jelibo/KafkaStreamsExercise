package com.memsource.hack2019.analytics20.domain;

import java.util.List;

public class TaskData {

    private String taskId;
    private String tGroupId;
    private List<Integer> levels;
    private List<TaskUnit> tUnits;

    /**
     * Confirmed segment – a segment that was deemed translated by the linguist/translator.
     * A Segment is considered confirmed
     * when the first element of levels is equal to tUnits[0].confirmedLevel
     *
     * @return if segment is confirmed
     */
    public boolean isConfirmedSegment() {
        return levels.get(0).equals(tUnits.get(0).getConfirmedLevel());
    }

    public String getTaskId() {
        return taskId;
    }

    public String gettGroupId() {
        return tGroupId;
    }

    public List<Integer> getLevels() {
        return levels;
    }

    public List<TaskUnit> gettUnits() {
        return tUnits;
    }

    @Override
    public String toString() {
        return "TaskData{" +
                "taskId='" + taskId + '\'' +
                ", tGroupId='" + tGroupId + '\'' +
                ", levels=" + levels +
                ", tUnits=" + tUnits +
                '}';
    }
}
