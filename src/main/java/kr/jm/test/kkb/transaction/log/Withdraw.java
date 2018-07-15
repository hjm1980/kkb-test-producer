package kr.jm.test.kkb.transaction.log;

import lombok.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Withdraw extends AbstractAmountTransactionLog {
    public Withdraw(int userNumber, long logTimestamp, int accountNumber,
            long amount) {
        super(TransactionType.WITHDRAW, userNumber, logTimestamp, accountNumber,
                amount);
    }

}
