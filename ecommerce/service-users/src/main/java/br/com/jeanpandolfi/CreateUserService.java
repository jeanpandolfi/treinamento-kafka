package br.com.jeanpandolfi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.HashMap;
import java.util.UUID;

public class CreateUserService {

    private final Connection conection;

    public CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.conection = DriverManager.getConnection(url);
        String sql = "CREATE TABLE IF NOT EXISTS Users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200)" +
                ")";
        conection.createStatement().execute(sql);
    }

    public static void main(String[] args) throws SQLException {
        var createUserService = new CreateUserService();
        /**O try seria para tentar receber e se caso ocorra alguma exeption ele fecha o porta de conexão*/
        try(var kafkaService = new KafkaService<Order>(
                CreateUserService.class.getSimpleName(), // Consumer group
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                new HashMap<>())){
            kafkaService.run();
        }

    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("-----------------------------");
        System.out.println("Processing new order, checking for new User");
        System.out.println("Key: "+record.key());
        System.out.println("Value: "+record.value());
        System.out.println("Partition: "+record.partition());
        System.out.println("Offset: "+record.offset());

        var order = record.value();
        if(isNewUser(order.getEmail())){
            insertNewUser( order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        String sql = "INSERT INTO Users(uuid, email) values (?, ?)";
        PreparedStatement insert = conection.prepareStatement(sql);
        insert.setString(1, UUID.randomUUID().toString());
        insert.setString(2, email);
        insert.execute();
        System.out.println("Usuário "+ "uuid" + " e " + email + " adicionado");
    }

    private boolean isNewUser(String email) throws SQLException {
        String sql = "SELECT uuid FROM Users WHERE email = ? limit 1";
        PreparedStatement select = conection.prepareStatement(sql);
        select.setString(1, email);
        ResultSet results = select.executeQuery();
        return !results.next();
    }

}
