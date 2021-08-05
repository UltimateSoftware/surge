package com.example.account;

import com.example.command.BankAccountCommand;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@Data
@EqualsAndHashCode
public class CreateAccount extends BankAccountCommand {

    private String accountOwner;
    private String securityCode;
    private double initialBalance;

    public CreateAccount(UUID accountNumber, String accountOwner,String securityCode,double initialBalance){
        this.accountNumber = accountNumber;
        this.accountOwner = accountOwner;
        this.securityCode = securityCode;
        this.initialBalance = initialBalance;

    }
}
