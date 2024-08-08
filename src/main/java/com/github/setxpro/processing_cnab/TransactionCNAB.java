package com.github.setxpro.processing_cnab;

import java.math.BigDecimal;

public record TransactionCNAB(
        Integer tipo,
        String data,
        BigDecimal valor,
        Long cpf,
        String cartao,
        String hora,
        String donoDaLoja,
        String nomeDaLoja
) {
}
