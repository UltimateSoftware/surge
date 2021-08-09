package com.example.account;

import com.example.command.BankAccountCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class DebitAccount extends BankAccountCommand {
    private double debitAmount;
}
