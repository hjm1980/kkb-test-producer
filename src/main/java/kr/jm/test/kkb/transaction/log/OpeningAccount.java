package kr.jm.test.kkb.transaction.log;

import lombok.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class OpeningAccount extends AbstractTransactionLog {
    private int accountNumber;

    public OpeningAccount(int userNumber, long logTimestamp,
            int accountNumber) {
        super(TransactionType.OPENING_ACCOUNT, userNumber, logTimestamp);
        this.accountNumber = accountNumber;
    }
}
