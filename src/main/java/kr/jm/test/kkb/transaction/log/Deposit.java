package kr.jm.test.kkb.transaction.log;

import lombok.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Deposit extends AbstractAmountTransactionLog {

    public Deposit(int userNumber, long logTimestamp, int accountNumber,
            long amount) {
        super(TransactionType.DEPOSIT, userNumber, logTimestamp, accountNumber,
                amount);
    }
}
