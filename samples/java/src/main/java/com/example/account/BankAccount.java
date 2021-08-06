package com.example.account;

import java.util.UUID;
public record BankAccount(UUID accountId,String accountOwner,String securityCode, double balance) {}
