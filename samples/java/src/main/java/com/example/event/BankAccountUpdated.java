package com.example.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@Data
@EqualsAndHashCode
@AllArgsConstructor
public class BankAccountUpdated extends BankAccountEvent {
    private UUID accountId;
    private double balance;
}
