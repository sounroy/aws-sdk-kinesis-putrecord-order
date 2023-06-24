package com.thecodinginterface.kinesis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class OrderGenerator {
    static final Product[] products = {
            new Product("A0001", "Hair Comb", 2.99),
            new Product("A0002", "Toothbrush", 5.99),
            new Product("A0003", "Dental Floss", 0.99),
            new Product("A0004", "Hand Soap", 1.99)
    };
    static final String[] sellerIDs = { "abc", "xyz", "jkq", "wrp" };
    static final String[] customerIDs = {
            "C1000", "C1001", "C1002", "C1003", "C1004", "C1005", "C1006", "C1007", "C1008", "C1009",
            "C1010", "C1011", "C1012", "C1013", "C1014", "C1015", "C1016", "C1017", "C1018", "C1019",
    };

    public static Order makeOrder() {
        var orderID = UUID.randomUUID().toString();
        var tlr = ThreadLocalRandom.current();
        var sellerID = sellerIDs[tlr.nextInt(sellerIDs.length)];
        var customerID = customerIDs[tlr.nextInt(customerIDs.length)];
        List<Product> availableProducts = new ArrayList<>(Arrays.asList(products));

        List<OrderItem> orderItems = new ArrayList<>();
        int nProducts = tlr.nextInt(1, products.length);
        for (int i = 0; i < nProducts; i++) {
            Product p = availableProducts.remove(tlr.nextInt(availableProducts.size()));
            var qty = tlr.nextInt(1,10);
            orderItems.add(new OrderItem(p.getName(), p.getCode(), qty, p.getPrice()));
        }

        return new Order(orderItems, customerID, orderID, sellerID);
    }
}
