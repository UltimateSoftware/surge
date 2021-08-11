// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example;

import com.example.account.BankAccount;

import com.example.event.BankAccountEvent;
import com.example.format.SurgeAggregateReadFormattingBankAccount;
import com.example.format.SurgeAggregateWriteFormattingBankAccount;
import com.example.format.SurgeEventWriteFormattingBankEvent;
import surge.core.SurgeAggregateReadFormatting;
import surge.core.SurgeAggregateWriteFormatting;
import surge.core.SurgeEventWriteFormatting;
import surge.javadsl.command.AggregateCommandModel;
import surge.javadsl.command.SurgeCommandBusinessLogic;
import surge.kafka.KafkaTopic;
import java.util.UUID;

public class BankAccountSurgeModel extends SurgeCommandBusinessLogic<UUID, BankAccount, BankAccountCommand,
        BankAccountEvent> {
    @Override
    public AggregateCommandModel<BankAccount, BankAccountCommand, BankAccountEvent> commandModel() {
        return new BankAccountCommandModel();
    }

    @Override
    public String aggregateName() {
        return "bank-account";
    }

    @Override
    public KafkaTopic stateTopic() {
        return new KafkaTopic("bank-account-state");
    }

    @Override
    public KafkaTopic eventsTopic() {
        return new KafkaTopic("bank-account-events");
    }

    @Override
    public SurgeAggregateReadFormatting<BankAccount> aggregateReadFormatting() {
        return new SurgeAggregateReadFormattingBankAccount();
    }

    @Override
    public SurgeEventWriteFormatting<BankAccountEvent> eventWriteFormatting() {
        return new SurgeEventWriteFormattingBankEvent();
    }

    @Override
    public SurgeAggregateWriteFormatting<BankAccount> aggregateWriteFormatting() {
        return new SurgeAggregateWriteFormattingBankAccount();
    }
}
