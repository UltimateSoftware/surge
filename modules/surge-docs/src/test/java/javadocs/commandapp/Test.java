// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package javadocs.commandapp;

import javadocs.commandapp.account.BankAccount;
import javadocs.commandapp.command.BankAccountCommand;
import javadocs.commandapp.command.CreateAccount;
import javadocs.commandapp.command.DebitAccount;
import surge.javadsl.common.CommandResult;
import surge.javadsl.testkit.MockedSurgeEngine;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class Test {

    // #sample_test
    void sampleTest() throws Exception {
        MockedSurgeEngine<UUID, BankAccount, BankAccountCommand, BankAccountCommand> mockedSurgeEngine;
        mockedSurgeEngine = new MockedSurgeEngine<>();

        UUID accountNumber = UUID.randomUUID();
        UUID otherAccountNumber = UUID.randomUUID();

        CreateAccount createAccount = new CreateAccount(accountNumber, "Jane", "Doe", 1000);
        DebitAccount debitAccount = new DebitAccount(otherAccountNumber, 50);
        BankAccount expectedBankAccount = new BankAccount(accountNumber, "Jane", "Doe", 1000);

        mockedSurgeEngine.whenSendCommand(accountNumber, createAccount).returnAggregate(expectedBankAccount);
        mockedSurgeEngine.whenSendCommand(otherAccountNumber, debitAccount).noSuchAggregate();

        CompletionStage<CommandResult<BankAccount>> actualResult;
        actualResult = mockedSurgeEngine.get()
                .aggregateFor(accountNumber).sendCommand(createAccount);

        // compare actual result (note: must resolve the future) with expected result
    }
    // #sample_test

}
