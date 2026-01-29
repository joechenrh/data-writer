CREATE TABLE `sbtest6` (
    `id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `k` bigint NOT NULL DEFAULT '0',
    `c` char(120) NOT NULL DEFAULT '' COMMENT 'max_length=120, min_length=120',
    `pad` char(60) NOT NULL DEFAULT '' COMMENT 'max_length=60, min_length=60',
    `int_0` int NOT NULL DEFAULT '0' COMMENT 'stddev=10000, mean=0',
    `int_1` int NOT NULL DEFAULT '0' COMMENT 'stddev=1000000, mean=0',
    `int_2` int NOT NULL DEFAULT '0' COMMENT 'stddev=700000000, mean=0',
    `bigint_0` bigint DEFAULT NULL COMMENT 'order=total_order',
    `bigint_1` bigint DEFAULT NULL COMMENT 'order=partial_order',
    `bigint_2` bigint DEFAULT NULL COMMENT 'order=random_order',
    `varchar_0` varchar(768) DEFAULT NULL COMMENT 'max_length=768, min_length=768',
    `text_0` text DEFAULT NULL COMMENT 'max_length=61440'
);
