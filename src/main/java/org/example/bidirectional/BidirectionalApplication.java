package org.example.bidirectional;

import org.example.bidirectional.service.ClickHouseService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BidirectionalApplication {

    public static void main(String[] args) {
        var ctx = SpringApplication.run(BidirectionalApplication.class, args);
        ClickHouseService clickHouseService = ctx.getBean(ClickHouseService.class);

        try {
            for (String s : clickHouseService.fetchColumns("uk_price_paid"))
                System.out.println(s);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
