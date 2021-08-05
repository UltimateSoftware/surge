package com.example.account;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class BankAccount {
    private UUID accountId;
    private String accountOwner;
    private String securityCode;
    private double balance;

}
