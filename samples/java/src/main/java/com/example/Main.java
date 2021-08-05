package com.example;

import com.example.account.BankAccount;
import com.example.command.BankAccountCommand;
import com.example.event.BankAccountEvent;
import scala.runtime.Nothing$;
import surge.javadsl.command.SurgeCommand$;
import surge.javadsl.command.SurgeCommand;

import java.util.UUID;

public class Main {
    public static void main(String args[]){
                BankAccountSurgeModel bankAccountSurgeModel = new BankAccountSurgeModel();
        SurgeCommand<UUID, BankAccount, BankAccountCommand, Object, BankAccountEvent> surgeCommand =
        SurgeCommand$.MODULE$.create(bankAccountSurgeModel);

    }
}
