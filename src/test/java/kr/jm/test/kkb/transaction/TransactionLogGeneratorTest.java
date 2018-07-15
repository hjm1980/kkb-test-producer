package kr.jm.test.kkb.transaction;

import kr.jm.test.kkb.transaction.log.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TransactionLogGeneratorTest {

    private TransactionLogGenerator transactionLogGenerator;

    @Before
    public void setUp() throws Exception {
        this.transactionLogGenerator = new TransactionLogGenerator(0, 0);
    }

    @Test
    public void testGenerateAll() throws Exception {
        NewUser newUser =
                transactionLogGenerator.generateNewUser("generateNewUser");
        int userNumber = newUser.getUserNumber();
        OpeningAccount openingAccount =
                transactionLogGenerator.generateOpeningAccount(userNumber);
        int accountNumber = openingAccount.getAccountNumber();
        Deposit deposit = transactionLogGenerator.generateDeposit(userNumber,
                accountNumber, 10000);
        Withdraw withdraw = transactionLogGenerator
                .generateWithdraw(userNumber, accountNumber, 5000);
        Transfer transfer = transactionLogGenerator.generateTransfer(userNumber,
                accountNumber, 5000, "Kakao Bank", 0,
                "toAccountUser");
        TransactionLogInterface[] transactionLogs =
                {newUser, openingAccount, deposit, withdraw, transfer};
        System.out.println(Arrays.toString(transactionLogs));

        String[] jsonStrings = Arrays.stream(transactionLogs)
                .map(transactionLogGenerator::toJsonStringLog)
                .peek(Assert::assertNotNull).toArray(String[]::new);
        TransactionLogInterface[] deTransactionLogs =
                Arrays.stream(jsonStrings).peek(System.out::println)
                        .map(jsonString -> transactionLogGenerator
                                .deserializeLogWithJsonString(jsonString))
                        .toArray
                                (TransactionLogInterface[]::new);

        System.out.println(Arrays.toString(deTransactionLogs));

        Assert.assertEquals(transactionLogs.length, deTransactionLogs.length);
        for (int i = 0; i < deTransactionLogs.length; i++)
            Assert.assertEquals(transactionLogs[i].toString(),
                    deTransactionLogs[i].toString());
    }

    @Test
    public void testGenerateNewUserAndAccount() throws Exception {
        TransactionLogInterface[] newUserAndAccountTransactionLogs =
                transactionLogGenerator.generateNewUserAndAccount(
                        IntStream.range(0, 100).mapToObj(i -> "user-" + i)
                                .toArray(String[]::new));
        Map<TransactionType, List<TransactionLogInterface>>
                transactionTypeListMap =
                Arrays.stream(newUserAndAccountTransactionLogs)
                        .collect(Collectors.groupingBy(
                                TransactionLogInterface::getTransactionType));
        transactionTypeListMap.values().stream().flatMap(List::stream)
                .sorted(Comparator
                        .comparing(TransactionLogInterface::getUserNumber))
                .forEach(System.out::println);
        Assert.assertEquals(2, transactionTypeListMap.size());
        Assert.assertEquals(100,
                transactionTypeListMap.get(TransactionType.NEW_USER).size());
        Assert.assertEquals(100,
                transactionTypeListMap.get(TransactionType.OPENING_ACCOUNT)
                        .size());
    }

    @Test
    public void testGenerateAbout100TransactionLogEach() {
        TransactionLogInterface[] about100TransactionLogs =
                transactionLogGenerator
                        .generateAbout100TransactionLogEach("제민", "jemin");
        System.out.println(about100TransactionLogs.length);
        Assert.assertTrue(183 < about100TransactionLogs.length &&
                about100TransactionLogs.length < 225);
        Arrays.stream(about100TransactionLogs)
                .map(transactionLogGenerator::toJsonStringLog)
                .forEach(System.out::println);
    }

    @Test
    public void testGenerateAbout100TransactionLogEachWithDelay() {
        List<TransactionLogInterface> transactionLogList =
                Collections.synchronizedList(new ArrayList<>());
        transactionLogGenerator.generateAbout100TransactionLogEach(t -> {
            System.out.println(transactionLogList.size() + " " +
                    System.currentTimeMillis() + " " + t);
            transactionLogList.add(t);
        }, 100, "제민", "jemin");
        System.out.println(transactionLogList.size());
        Assert.assertTrue(183 < transactionLogList.size() &&
                transactionLogList.size() < 225);
    }
}