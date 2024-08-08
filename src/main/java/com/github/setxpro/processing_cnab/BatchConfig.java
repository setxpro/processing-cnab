package com.github.setxpro.processing_cnab;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;

@Configuration
public class BatchConfig {
    private PlatformTransactionManager transactionManager;
    private JobRepository jobRepository;

    // BASIC CONFIG FROM SPRING BATCH
    public BatchConfig(PlatformTransactionManager transactionManager, JobRepository jobRepository) {
        this.transactionManager = transactionManager;
        this.jobRepository = jobRepository;
    }

    // JOBS
    @Bean
    Job job(Step step) {
        return new JobBuilder("job", this.jobRepository)
                .start(step) // inicia o step
                .incrementer(new RunIdIncrementer()) // config para ele rodar mais de uma vez
                .build();
    }

    // CREATE STEP
    @Bean
    Step step(
            ItemReader<TransactionCNAB> reader,
            ItemProcessor<TransactionCNAB, Transaction> processor,
            ItemWriter<Transaction> writer
    ) {
        return new StepBuilder("job", this.jobRepository)
                .<TransactionCNAB, Transaction>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    // CREATE WRITER
    @Bean
    FlatFileItemReader<TransactionCNAB> reader() {
        return new FlatFileItemReaderBuilder<TransactionCNAB>()
                .name("reader")
                .resource(new FileSystemResource("files/CNAB.txt"))
                .fixedLength()
                .columns(
                        new Range(1, 1), new Range(2, 9),
                        new Range(10, 19), new Range(20, 30),
                        new Range(31, 42), new Range(43, 48),
                        new Range(49, 62), new Range(63, 80)
                )
                .names(
                        "tipo", "data", "valor", "cpf", "cartao", "hora", "donoDaLoja", "nomeDaLoja"
                )
                .targetType(TransactionCNAB.class)
                .build();
    }

    //  PROCESSOR
    @Bean
    ItemProcessor<TransactionCNAB, Transaction> processor() {

        // Wither Pattern
        return item -> {
          var transaction = new Transaction(
              null,
              item.tipo(),
              null,
              null,
              item.cpf(),
              item.cartao(),
              null,
              item.donoDaLoja().trim(),
              item.nomeDaLoja().trim()

          )
          .withValor(item.valor().divide(BigDecimal.valueOf(100)))
          .withData(item.data())
          .withHora(item.hora());

          return transaction;
        };
    }

    // WRITER
    @Bean
    JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .sql(
                        """
                                INSERT INTO transaction (
                                	tipo, data, valor, cpf, cartao, hora, dono_loja, nome_loja
                                	) VALUES (
                                	:tipo, :data, :valor, :cpf, :cartao, :hora, :donoDaLoja, :nomeDaLoja
                                	)
                        """
                ).beanMapped()
                .build();
    }
}
