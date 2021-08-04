package com.example.account;

import com.example.command.BankAccountCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class CreateAccount extends BankAccountCommand {

    private String accountOwner;
    private String securityCode;
    private double initialBalance;
}
