package com.example.event;


import java.util.UUID;

public abstract class BankAccountEvent {
    protected UUID accountNumber;

    public UUID getAccountNumber() {
        return accountNumber;
    }
}
