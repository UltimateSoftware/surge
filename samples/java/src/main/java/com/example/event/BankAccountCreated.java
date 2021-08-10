// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example.event;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.UUID;

@JsonSerialize
@JsonTypeName("BankAccountCreated")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public record BankAccountCreated(UUID accountNumber, String accountOwner, String securityCode,
                                 double balance) implements BankAccountEvent {

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }
}
