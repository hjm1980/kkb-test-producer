package kr.jm.test.kkb.transaction.log;

import lombok.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Transfer extends AbstractAmountTransactionLog {
    private String toBank;
    private int toAccountNumber;
    private String toAccountUser;

    public Transfer(int userNumber, long logTimestamp,
            int accountNumber, long amount, String toBank, int toAccountNumber,
            String toAccountUser) {
        super(TransactionType.TRANSFER, userNumber, logTimestamp, accountNumber,
                amount);
        this.toBank = toBank;
        this.toAccountNumber = toAccountNumber;
        this.toAccountUser = toAccountUser;
    }
}
