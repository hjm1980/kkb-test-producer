package kr.jm.test.kkb.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.jm.test.kkb.transaction.log.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
public class TransactionLogGenerator {

    private ObjectMapper objectMapper;
    private AtomicInteger userNumberSupplier;
    private AtomicInteger accountNumberSupplier;
    private Map<Integer, Set<Integer>> userNumberAccountsMap;
    private String[] bankNames =
            {"카카오", "국민", "신한", "하나", "우리", "외환", "농협", "SC"};
    private String[] userNames =
            {"라이언", "jm", "영희", "바둑이", "순희", "유제석", "감호동"};


    public TransactionLogGenerator(int initialUserNumber,
            int initialAccountNumber) {
        this.objectMapper = new ObjectMapper();
        this.userNumberSupplier = new AtomicInteger(initialUserNumber);
        this.accountNumberSupplier = new AtomicInteger(initialAccountNumber);
        this.userNumberAccountsMap = new ConcurrentHashMap<>();
    }

    public TransactionLogInterface[] generateNewUserAndAccount(
            String... userNames) {
        return Arrays.stream(userNames).parallel().map(this::generateNewUser)
                .map(newUser -> new TransactionLogInterface[]{newUser,
                        generateOpeningAccount(newUser.getUserNumber())})
                .flatMap(Arrays::stream)
                .toArray(TransactionLogInterface[]::new);
    }

    public TransactionLogInterface[] generateAbout100TransactionLogEach(
            String... userNames) {
        return Stream
                .concat(Arrays.stream(generateNewUserAndAccount(userNames)),
                        this.userNumberAccountsMap.keySet().stream()
                                .map(this::generateAbout100TransactionLog)
                                .flatMap(Arrays::stream))
                .toArray(TransactionLogInterface[]::new);
    }

    public void generateAbout100TransactionLogEach(
            Consumer<TransactionLogInterface> transactionLogConsumer,
            long delayMillis, String... userNames) {
        Arrays.stream(generateNewUserAndAccount(userNames))
                .peek(transactionLogConsumer)
                .mapToInt(TransactionLogInterface::getUserNumber).distinct()
                .parallel().forEach(
                userNumber -> generateAbout100TransactionLog(
                        transactionLogConsumer, delayMillis, userNumber));
    }

    public TransactionLogInterface[] generateAbout100TransactionLog(
            int userNumber) {
        if (!this.userNumberAccountsMap.containsKey(userNumber))
            return logNotExistAndReturnNull("generateAbout100TransactionLog",
                    "userNumber", userNumber);
        return IntStream.range(0, new Random().nextInt(110 - 90) + 90).mapToObj
                (i -> generateRandomTransactionLog(userNumber))
                .toArray(TransactionLogInterface[]::new);
    }

    public void generateAbout100TransactionLog(
            Consumer<TransactionLogInterface> transactionLogConsumer,
            long delayMillis, int userNumber) {
        if (!this.userNumberAccountsMap.containsKey(userNumber))
            logNotExistAndReturnNull("generateAbout100TransactionLog",
                    "userNumber", userNumber);
        IntStream.range(0, new Random().nextInt(110 - 90) + 90).mapToObj
                (i -> generateRandomTransactionLog(userNumber))
                .peek(t -> sleep(delayMillis)).forEach(transactionLogConsumer);
    }

    private void sleep(long delayMillis) {
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public TransactionLogInterface generateRandomTransactionLog(
            int userNumber) {
        if (!this.userNumberAccountsMap.containsKey(userNumber))
            return logNotExistAndReturnNull("generateRandomTransactionLog",
                    "userNumber",
                    userNumber);
        switch ((int) (Math.random() * 10)) {
            case 0:
                return generateOpeningAccount(userNumber);
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
                return generateDeposit(userNumber,
                        getRandomAccountNumber(userNumber),
                        getRandomAmount());
            case 6:
            case 7:
                return generateWithdraw(userNumber,
                        getRandomAccountNumber(userNumber),
                        getRandomAmount());
            case 8:
            case 9:
                return generateTransfer(userNumber,
                        getRandomAccountNumber(userNumber),
                        getRandomAmount(), getRandomBankName(),
                        getRandomAccountNumber(), getRandomUserName());
            default:
                return null;
        }
    }

    public String getRandomUserName() {
        return userNames[new Random().nextInt(userNames.length)];
    }

    public int getRandomAccountNumber() {
        return (int) (Math.random() * 1000000000);
    }

    public String getRandomBankName() {
        return bankNames[new Random().nextInt(bankNames.length)];
    }

    public long getRandomAmount() {
        return (new Random().nextInt(1000 - 100) + 100) * 100;
    }

    public int getRandomAccountNumber(int userNumber) {
        return getRandomAccountNumber(
                this.userNumberAccountsMap.get(userNumber));
    }

    public int getRandomAccountNumber(Set<Integer> accountNumberSet) {
        return accountNumberSet.stream()
                .skip(new Random().nextInt(accountNumberSet.size())).findFirst()
                .orElseThrow(() -> new RuntimeException("Wrong Size !!!"));
    }

    private <T extends TransactionLogInterface> T logAndReturn(
            T transactionLog) {
        log.info("generateLog - {}", transactionLog);
        return transactionLog;
    }


    public NewUser generateNewUser(String userName) {
        NewUser newUser = new NewUser(this.userNumberSupplier.getAndIncrement(),
                System.currentTimeMillis(), userName);
        this.userNumberAccountsMap.computeIfAbsent(newUser.getUserNumber(),
                i -> new ConcurrentSkipListSet<>());
        return logAndReturn(newUser);
    }

    public OpeningAccount generateOpeningAccount(int userNumber) {
        if (!this.userNumberAccountsMap.containsKey(userNumber))
            return logNotExistAndReturnNull("generateOpeningAccount",
                    "userNumber",
                    userNumber);
        OpeningAccount openingAccount =
                new OpeningAccount(userNumber, System.currentTimeMillis(),
                        this.accountNumberSupplier.getAndIncrement());
        this.userNumberAccountsMap.get(userNumber).add(openingAccount
                .getAccountNumber());
        return logAndReturn(openingAccount);
    }

    private <T> T logNotExistAndReturnNull(
            String methodName, String numberName, int notExistNumber) {
        log.error("{} Failure Occur !!! - No {} = {}", methodName,
                numberName, notExistNumber);
        return null;
    }

    public Deposit generateDeposit(int userNumber, int depositAccountNumber,
            long depositAmount) {
        if (!this.userNumberAccountsMap.containsKey(userNumber))
            return logNotExistAndReturnNull("generateDeposit", "userNumber",
                    userNumber);
        if (!this.userNumberAccountsMap.get(userNumber)
                .contains(depositAccountNumber))
            return logNotExistAndReturnNull("generateDeposit",
                    "depositAccountNumber",
                    depositAccountNumber);
        return logAndReturn(new Deposit(userNumber, System.currentTimeMillis(),
                depositAccountNumber, depositAmount));
    }

    public Withdraw generateWithdraw(int userNumber, int withdrawAccountNumber,
            long withdrawAmount) {
        if (!this.userNumberAccountsMap.containsKey(userNumber))
            return logNotExistAndReturnNull("generateWithdraw", "userNumber",
                    userNumber);
        if (!this.userNumberAccountsMap.get(userNumber)
                .contains(withdrawAccountNumber))
            return logNotExistAndReturnNull("generateWithdraw",
                    "withdrawAccountNumber",
                    withdrawAccountNumber);
        return logAndReturn(new Withdraw(userNumber, System.currentTimeMillis(),
                withdrawAccountNumber, withdrawAmount));
    }

    public Transfer generateTransfer(int userNumber, int transferAccountNumber,
            long transferAmount, String toBank, int toAccountNumber,
            String toAccountUser) {
        if (!this.userNumberAccountsMap.containsKey(userNumber))
            return logNotExistAndReturnNull("generateTransfer", "userNumber",
                    userNumber);
        if (!this.userNumberAccountsMap.get(userNumber)
                .contains(transferAccountNumber))
            return logNotExistAndReturnNull("generateTransfer",
                    "transferAccountNumber",
                    transferAccountNumber);
        return logAndReturn(new Transfer(userNumber, System.currentTimeMillis(),
                transferAccountNumber, transferAmount, toBank, toAccountNumber,
                toAccountUser));
    }

    public String toJsonStringLog(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("toJsonStringLog({})", object, e);
            return null;
        }
    }

    public TransactionLogInterface deserializeLogWithJsonObject(
            Map<String, Object> jsonObject) {
        return objectMapper.convertValue(jsonObject,
                extractTransactionLogClass(
                        jsonObject.get("transactionType").toString()));
    }

    private Class<? extends TransactionLogInterface>
    extractTransactionLogClass(
            String transactionType) {
        switch (TransactionType
                .valueOf(transactionType)) {
            case NEW_USER:
                return NewUser.class;
            case OPENING_ACCOUNT:
                return OpeningAccount.class;
            case DEPOSIT:
                return Deposit.class;
            case WITHDRAW:
                return Withdraw.class;
            case TRANSFER:
                return Transfer.class;
            default:
                throw new RuntimeException(
                        "Wrong Transaction Type Occur !!! - " +
                                transactionType);
        }
    }

    public TransactionLogInterface deserializeLogWithJsonString(
            String jsonString) {
        try {
            return deserializeLogWithJsonObject(objectMapper
                    .readValue(jsonString, new TypeReference<Map<String,
                            Object>>() {}));
        } catch (IOException e) {
            log.error("deserializeLogWithJsonString({})", jsonString, e);
            return null;
        }
    }
}
