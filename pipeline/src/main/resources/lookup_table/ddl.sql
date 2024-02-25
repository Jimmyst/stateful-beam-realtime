CREATE TABLE `ltv_lookup_table` (
  `uid` varchar(100) NOT NULL,
  `total` double DEFAULT NULL,
  `cnt` int(11) DEFAULT NULL,
  UNIQUE KEY `ltv_lookup_table_UN` (`uid`),
  KEY `ltv_lookup_table_uid_IDX` (`uid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;