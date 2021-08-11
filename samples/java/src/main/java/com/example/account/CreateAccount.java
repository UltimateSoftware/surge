package com.example.account;

import com.example.command.BankAccountCommand;

import java.util.UUID;

public record CreateAccount(UUID accountNumber, String accountOwner, String securityCode,
                            double initialBalance) implements BankAccountCommand {

    @Override
    public UUID getAccountNumber() {
        return accountNumber;
    }
}


