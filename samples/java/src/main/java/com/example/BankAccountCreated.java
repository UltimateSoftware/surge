package com.example;

import com.example.event.BankAccountEvent;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BankAccountCreated extends BankAccountEvent {
    private String accountOwner;
    private String securityCode;
    private double balance;

    public BankAccountCreated(int accountNumber, String accountOwner, String securityCode, double balance) {

        this.accountNumber = accountNumber;
        this.accountOwner = accountOwner;
        this.securityCode = securityCode;
        this.balance = balance;
    }
}
