package docs.example.format;

import docs.example.event.BankAccountEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.collection.immutable.HashMap$;
import surge.core.SerializedMessage;
import surge.core.SurgeEventWriteFormatting;

// #surge_model_class
public class SurgeEventWriteFormattingBankEvent implements SurgeEventWriteFormatting<BankAccountEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SerializedMessage writeEvent(BankAccountEvent evt) {
        try {
            String key = evt.getAccountNumber().toString();
            byte[] evtByte = objectMapper.writeValueAsBytes(evt);
            scala.collection.immutable.Map<String, String> map = HashMap$.MODULE$.empty();
            return SerializedMessage.apply(key, evtByte, map);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
// #surge_model_class
