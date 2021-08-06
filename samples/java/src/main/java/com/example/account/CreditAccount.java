package com.example.account;

import com.example.command.BankAccountCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class CreditAccount extends BankAccountCommand {
    private double amount;

    public CreditAccount(UUID accountNumber, double amount){
        this.accountNumber = accountNumber;
        this.amount = amount;
    }

}
