package com.example.command;

import java.util.UUID;

public record CreateAccount(UUID accountNumber, String accountOwner, String securityCode,
                            double initialBalance) implements BankAccountCommand {

    @Override
    public UUID getAccountNumber() {
        return this.accountNumber;
    }
}