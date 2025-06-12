CREATE TABLE `sbtest6` (
    `id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `k` bigint NOT NULL DEFAULT '0',
    `c` char(120) NOT NULL DEFAULT '',
    `pad` char(60) NOT NULL DEFAULT '',
    `int_0` int NOT NULL DEFAULT '0', -- stddev=10000, mean=0
    `int_1` int NOT NULL DEFAULT '0', -- stddev=1000000, mean=0
    `int_2` int NOT NULL DEFAULT '0', -- stddev=700000000, mean=0
    `bigint_0` bigint DEFAULT NULL, -- order=total_order
    `bigint_1` bigint DEFAULT NULL, -- order=partial_order
    `bigint_2` bigint DEFAULT NULL, -- order=random_order
    `varchar_0` varchar(768) DEFAULT NULL,
    `text_0` text DEFAULT NULL -- max_length=61440
);