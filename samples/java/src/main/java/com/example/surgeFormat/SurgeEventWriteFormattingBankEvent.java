package com.example.surgeFormat;

import com.example.event.BankAccountEvent;
import scala.collection.immutable.HashMap$;
import surge.core.SerializedMessage;
import surge.core.SurgeEventWriteFormatting;

public class SurgeEventWriteFormattingBankEvent implements SurgeEventWriteFormatting<BankAccountEvent> {
    @Override
    public SerializedMessage writeEvent(BankAccountEvent evt) {
        String key = String.valueOf(evt.getAccountNumber());
        byte[] evtByte = evt.toString().getBytes();
        scala.collection.immutable.Map<String,String> map = HashMap$.MODULE$.empty();
        map.updated("aggregate_id",key);
        return  SerializedMessage.apply(key,evtByte,map);
    }
}
