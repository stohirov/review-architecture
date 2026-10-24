package com.example.platform.inventory.messaging;

import com.example.platform.inventory.domain.Reservation;
import com.example.platform.inventory.domain.ReservationRepository;
import com.example.platform.inventory.domain.StockRepository;
import com.example.platform.inventory.outbox.Outbox;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

import static com.example.platform.inventory.messaging.Messages.INVENTORY_EVENTS;

@Service
public class StockService {

    private final StockRepository stock;
    private final ReservationRepository reservations;
    private final Outbox outbox;

    public StockService(StockRepository stock, ReservationRepository reservations, Outbox outbox) {
        this.stock = stock;
        this.reservations = reservations;
        this.outbox = outbox;
    }

    /** All-or-nothing: reserves every line, or undoes the partial reservation and rejects. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void reserve(Messages.ReserveStock command) {
        List<Messages.Line> reserved = new ArrayList<>();
        for (Messages.Line line : command.lines()) {
            if (stock.tryReserve(line.sku(), line.quantity()) == 0) {
                reserved.forEach(r -> stock.release(r.sku(), r.quantity()));
                outbox.publish(INVENTORY_EVENTS, command.orderId().toString(), new Messages.StockRejected(command.orderId(), line.sku()));
                return;
            }
            reserved.add(line);
        }
        reserved.forEach(r -> reservations.save(new Reservation(command.orderId(), r.sku(), r.quantity())));
        outbox.publish(INVENTORY_EVENTS, command.orderId().toString(), new Messages.StockReserved(command.orderId()));
    }

    /** Compensation for a failed payment. Safe to repeat: released reservations are skipped. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void release(Messages.ReleaseStock command) {
        for (Reservation reservation : reservations.findByOrderId(command.orderId())) {
            if (reservation.release()) {
                stock.release(reservation.getSku(), reservation.getQuantity());
            }
        }
    }
}
