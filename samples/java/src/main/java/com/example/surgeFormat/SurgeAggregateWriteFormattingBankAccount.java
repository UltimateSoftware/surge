package com.example.surgeFormat;

import com.example.account.BankAccount;
import scala.collection.immutable.HashMap$;
import surge.core.SerializedAggregate;
import surge.core.SurgeAggregateWriteFormatting;

public class SurgeAggregateWriteFormattingBankAccount implements SurgeAggregateWriteFormatting<BankAccount> {
    @Override
    public SerializedAggregate writeState(BankAccount bankAccount) {
        byte[] bankAccountByte = bankAccount.toString().getBytes();
        scala.collection.immutable.Map<String,String> map = HashMap$.MODULE$.empty();
        map.updated("aggregate_id",bankAccount.accountId());
        return SerializedAggregate.apply(bankAccountByte, map);
    }
}
