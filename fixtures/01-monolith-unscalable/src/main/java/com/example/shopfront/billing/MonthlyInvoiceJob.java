package com.example.shopfront.billing;

import com.example.shopfront.order.CustomerOrder;
import com.example.shopfront.order.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class MonthlyInvoiceJob {

    private static final Logger log = LoggerFactory.getLogger(MonthlyInvoiceJob.class);

    private final OrderRepository orders;
    private final JavaMailSender mailSender;
    private final String from;

    public MonthlyInvoiceJob(OrderRepository orders, JavaMailSender mailSender,
                             @Value("${shopfront.invoice-from}") String from) {
        this.orders = orders;
        this.mailSender = mailSender;
        this.from = from;
    }

    @Scheduled(cron = "0 0 2 1 * *")
    @Transactional
    public void sendInvoices() {
        List<CustomerOrder> pending = orders.findByInvoicedFalse();
        log.info("Sending {} invoices", pending.size());
        for (CustomerOrder order : pending) {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(from);
            message.setTo(order.getCustomerEmail());
            message.setSubject("Invoice for order #" + order.getId());
            message.setText("Amount due: " + order.getTotal());
            mailSender.send(message);
            order.markInvoiced();
        }
    }
}
