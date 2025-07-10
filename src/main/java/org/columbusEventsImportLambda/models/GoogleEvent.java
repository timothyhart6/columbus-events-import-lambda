package org.columbusEventsImportLambda.models;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
public class GoogleEvent extends Event{
    private int rowNumber;

}
