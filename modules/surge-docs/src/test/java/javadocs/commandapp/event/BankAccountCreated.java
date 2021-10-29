// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.event;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.UUID;

// #event_class
@JsonSerialize
@JsonTypeName("BankAccountCreated")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class BankAccountCreated implements BankAccountEvent {

    private final UUID accountNumber;
    private final String accountOwner;
    private final String securityCode;
    private final double balance;

    public BankAccountCreated(UUID accountNumber, String accountOwner, String securityCode,
                              double balance) {
        this.accountNumber = accountNumber;
        this.accountOwner = accountOwner;
        this.securityCode = securityCode;
        this.balance = balance;
    }

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }

    public String getAccountOwner() {
        return accountOwner;
    }

    public String getSecurityCode() {
        return securityCode;
    }

    public double getBalance() {
        return balance;
    }
}
// #event_class
