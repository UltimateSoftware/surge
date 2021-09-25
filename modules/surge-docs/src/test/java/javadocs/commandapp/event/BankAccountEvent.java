// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.event;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.UUID;

// #event_class
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BankAccountCreated.class, name = "BankAccountCreated"),
        @JsonSubTypes.Type(value = BankAccountUpdated.class, name = "BankAccountUpdated"),
})
public interface BankAccountEvent {

    UUID getAccountNumber();
}
//#event_class
