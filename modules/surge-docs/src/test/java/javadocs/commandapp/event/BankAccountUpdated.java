// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.event;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.UUID;

// #event_class
@JsonSerialize
@JsonTypeName("BankAccountUpdated")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class BankAccountUpdated implements BankAccountEvent {

    public double amount;
    public UUID accountNumber;

    public BankAccountUpdated(UUID accountNumber, double amount) {
        this.amount = amount;
        this.accountNumber = accountNumber;
    }

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }

    public double getAmount() {
        return amount;
    }
}

// #event_class
