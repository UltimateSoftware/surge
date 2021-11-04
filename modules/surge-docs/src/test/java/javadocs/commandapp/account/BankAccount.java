// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp.account;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.UUID;

// #aggregate_class
@JsonSerialize
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class BankAccount {
    private final UUID accountNumber;
    private final String accountOwner;
    private final String securityCode;
    private double balance;

    public BankAccount(UUID accountNumber, String accountOwner, String securityCode, double balance) {
        this.accountNumber = accountNumber;
        this.accountOwner = accountOwner;
        this.securityCode = securityCode;
        this.balance = balance;
    }

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
// #aggregate_class

