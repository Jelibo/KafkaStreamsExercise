package com.memsource.hack2019.analytics20.domain;

/**
 * CDC Event
 */
public class Event {

    private static final String C_OPERATION = "c";

    private TaskData after;
    private TaskData patch;
    private String op;

    /**
     * Process only "c" events with levels not empty
     *
     * @return boolean if to process or not
     */
    public boolean shouldProcess() {
        return C_OPERATION.equalsIgnoreCase(op) && after.getLevels() != null &&
                !after.getLevels().isEmpty();
    }

    public TaskData getAfter() {
        return after;
    }

    public void setAfter(final TaskData after) {
        this.after = after;
    }

    public TaskData getPatch() {
        return patch;
    }

    public void setPatch(final TaskData patch) {
        this.patch = patch;
    }

    public String getOp() {
        return op;
    }

    public void setOp(final String op) {
        this.op = op;
    }

    @Override
    public String toString() {
        return "Event{" +
                "after='" + after + '\'' +
                ", patch='" + patch + '\'' +
                ", op='" + op + '\'' +
                '}';
    }
}
