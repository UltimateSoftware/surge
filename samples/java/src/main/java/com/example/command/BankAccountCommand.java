package com.example.command;

import java.util.UUID;

public  abstract class BankAccountCommand {
    protected UUID accountNumber;

    public UUID getAccountNumber() {
        return accountNumber;
    }
}
