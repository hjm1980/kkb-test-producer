package kr.jm.test.kkb.transaction.log;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
@ToString
public abstract class AbstractAmountTransactionLog extends
        AbstractTransactionLog {

    protected int accountNumber;
    protected long amount;

    public AbstractAmountTransactionLog(TransactionType transactionType,
            int userNumber, long logTimestamp, int accountNumber, long amount) {
        super(transactionType, userNumber, logTimestamp);
        this.accountNumber = accountNumber;
        this.amount = amount;
    }

}
