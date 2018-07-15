package kr.jm.test.kkb.transaction.log;

import lombok.*;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class NewUser extends AbstractTransactionLog {
    private String userName;

    public NewUser(int userNumber, long logTimestamp, String userName) {
        super(TransactionType.NEW_USER, userNumber, logTimestamp);
        this.userName = userName;
    }
}