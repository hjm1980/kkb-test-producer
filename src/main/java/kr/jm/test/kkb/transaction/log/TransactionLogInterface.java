package kr.jm.test.kkb.transaction.log;

public interface TransactionLogInterface {
    TransactionType getTransactionType();

    int getUserNumber();

    long getLogTimestamp();
}
