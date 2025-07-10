package org.columbusEventsImportLambda.models;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
public class DynamoDBEvent extends Event {

    private String id;
}
