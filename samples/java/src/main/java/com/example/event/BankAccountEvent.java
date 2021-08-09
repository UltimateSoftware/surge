package com.example.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.UUID;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BankAccountCreated.class, name = "BankAccountCreated"),
        @JsonSubTypes.Type(value = BankAccountUpdated.class, name = "BankAccountUpdated"),
})
public interface BankAccountEvent {
    public UUID getAccountNumber();
}
