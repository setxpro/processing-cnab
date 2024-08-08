package com.github.setxpro.processing_cnab;

import java.math.BigDecimal;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.Date;

public record Transaction(
        Long id,
        Integer tipo,
        Date data,
        BigDecimal valor,
        Long cpf,
        String cartao,
        Time hora,
        String donoDaLoja,
        String nomeDaLoja
) {

    public Transaction withValor(BigDecimal valor) {
        return new Transaction(
                this.id,
                this.tipo,
                this.data,
                valor,
                this.cpf,
                this.cartao,
                this.hora,
                this.donoDaLoja,
                this.nomeDaLoja
        );
    }

    public Transaction withData(String data) throws ParseException {
        var dateFormat = new SimpleDateFormat("yyyyMMdd");
        var date = dateFormat.parse(data);

        return new Transaction(
                this.id,
                this.tipo,
                new Date(date.getTime()),
                this.valor,
                this.cpf,
                this.cartao,
                this.hora,
                this.donoDaLoja,
                this.nomeDaLoja
        );
    }

    public Transaction withHora(String hora) throws ParseException {
        var dateFormat = new SimpleDateFormat("HHmmss");
        var date = dateFormat.parse(hora);

        return new Transaction(
                this.id,
                this.tipo,
                this.data,
                this.valor,
                this.cpf,
                this.cartao,
                new Time(date.getTime()),
                this.donoDaLoja,
                this.nomeDaLoja
        );
    }

}