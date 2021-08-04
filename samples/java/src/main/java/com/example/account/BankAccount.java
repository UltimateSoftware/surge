package com.example.account;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class BankAccount {
    private int accountId;
    private String accountOwner;
    private String securityCode;
    private double balance;

}
