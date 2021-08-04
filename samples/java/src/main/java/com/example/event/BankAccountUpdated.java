package com.example.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class BankAccountUpdated extends BankAccountEvent {
    private int accountId;
    private double balance;
}
