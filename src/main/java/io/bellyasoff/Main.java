package io.bellyasoff;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author IlyaB
 */
public class Main {
    public static void main(String[] args) throws Exception {

        String jdbcUrl = "jdbc:postgresql://localhost:8080/catalog";
        String username = "postgres";
        String password = "postgres";


        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {

            Service service = new Service(
                    "https://expro.ru/bitrix/catalog_export/export_Sai.xml",
                    connection
            );

            System.out.println("Таблицы:");
            service.getTableNames().forEach(System.out::println);

            for (String table : service.getTableNames()) {
                System.out.println("\nDDL для " + table + ":");
                System.out.println(service.getTableDDL(table));
            }

            service.update();
            System.out.println("\nОбновление завершено");
        }
    }
}
