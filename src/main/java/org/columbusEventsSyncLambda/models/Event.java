package org.columbusEventsSyncLambda.models;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class Event {

    private String locationName;
    private String eventName;
    private String date;
    private String time;
    private boolean isBadTraffic;
    private boolean isDesiredEvent;


}
