package org.columbusEventsImportLambda.models;


import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
public class Event {

    private String locationName;
    private String eventName;
    private String date;
    private String time;
    private boolean isBadTraffic;
    private boolean isDesiredEvent;
}
