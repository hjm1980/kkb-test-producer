package kr.jm.test.kkb.transaction.log;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
@ToString
public class AbstractTransactionLog implements TransactionLogInterface {
    protected TransactionType transactionType;
    protected int userNumber;
    protected long logTimestamp;
}
